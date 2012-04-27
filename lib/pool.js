// Copyright (c) 2012, Mark Cavage. All rights reserved.

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assertions = require('./assert');



///--- Globals

var assertFunction = assertions.assertFunction;
var assertNumber = assertions.assertNumber;
var assertObject = assertions.assertObject;
var assertString = assertions.assertString;

var DEFAULT_EVENTS = ['close', 'end', 'error', 'timeout'];
var MAX_INT = Math.pow(2, 32) - 1;

var SEQUENCE = 0;



///--- Internal Functions

function nextSequence() {
        if (++SEQUENCE >= MAX_INT)
                SEQUENCE = 1;

        return (SEQUENCE);
}


function enqueue(list, obj) {
        if (list.indexOf(obj) !== -1)
                return (false);

        return (list.unshift(obj));
}


function dequeue(list) {
        return (list.pop());
}


function createPoolObject(client) {
        var now = Date.now();

        var obj = {
                id: nextSequence(),
                alive: true,
                client: client,
                atime: now,
                ctime: now
        };

        return (obj);
}


function getPoolObject(list, client, ignoreHealth) {
        var obj = null;

        for (var i = 0; i < list.length; i++) {
                if ((list[i].client === client) &&
                    (list[i].alive || ignoreHealth)) {
                        obj = list[i];
                        break;
                }
        }

        if (obj)
                obj.atime = Date.now();

        return (obj);
}


function removePoolObject(list, obj) {
        var i;

        for (i = 0; i < list.length; i++) {
                if (list[i] === obj)
                        break;
        }

        if (i !== list.length) {
                list.splice(i, 1);
                obj.alive = false;
                obj.dtime = Date.now();
        }

        return (obj);
}




///--- API

function Pool(options) {
        assertObject('options', options);
        assertObject('options.log', options.log);
        assertFunction('options.check', options.check);
        assertNumber('options.checkInterval', options.checkInterval);
        assertFunction('options.create', options.create);
        assertString('options.name', options.name);
        assertNumber('options.max', options.max);
        assertNumber('options.maxIdleTime', options.maxIdleTime);

        EventEmitter.call(this, options);

        this.available = [];
        this.create = options.create;
        this.check = options.check;
        this.checkInterval = options.checkInterval;
        this.destroy = options.destroy || false;
        this.events = (options.events || DEFAULT_EVENTS).slice();
        this.log = options.log.child({pool: options.name}, true);
        this.max = Math.max(options.max, 1);
        this.maxIdleTime = options.maxIdleTime;
        this.name = options.name;
        this.queue = [];
        this.objects = [];
        this.stop = false;
        this.timer = false;

        this._scheduleReaper();
}
util.inherits(Pool, EventEmitter);
module.exports = Pool;


Pool.prototype.acquire = function acquire(callback) {
        assertFunction('callback', callback);

        var log = this.log;
        var obj;
        var self = this;

        if (log.trace())
                log.trace({state: self._state()}, 'acquire entered');

        if (this.stop) {
                callback(new Error('pool is shutting down'));
                return (false);
        }

        if ((obj = dequeue(this.available))) {
                obj.atime = Date.now();
                callback(null, obj.client);
                return (true);
        }

        if (this.objects.length >= this.max) {
                enqueue(this.queue, callback);
                return (false);
        }

        this.create(function createCallback(err, client) {
                if (err)
                        return (callback(err));

                obj = createPoolObject(client);
                self._watch(client);
                enqueue(self.objects, obj);

                process.nextTick(function () {
                        self.emit('create', client);
                });
                return (callback(null, client));
        });
        return (true);
};


Pool.prototype.release = function release(client) {
        assertObject('client', client);

        var log = this.log;
        var obj;
        var self = this;
        var waiter;

        if (!(obj = getPoolObject(this.objects, client)))
                return (false);

        if (log.trace())
                log.trace({state: self._state()}, 'release entered');

        enqueue(this.available, obj);
        if ((waiter = dequeue(this.queue))) {
                process.nextTick(function runNextWaiter() {
                        self.acquire(waiter);
                });
        } else if (this.available.length === this.objects.length) {
                process.nextTick(function () {
                        self.emit('drain');
                });
        }

        return (true);
};


Pool.prototype.shutdown = function shutdown(callback) {
        var log = this.log;
        var self = this;

        function end() {
                self.objects.forEach(function (o) {
                        self._kill(o);
                });
                self.emit('end');
                return (typeof (callback) === 'function' ? callback() : false);
        }

        if (log.trace())
                log.trace({state: self._state()}, 'shutdown entered');

        this.stop = true;
        if (this.timer)
                clearTimeout(this.timer);

        if (this.available.length === this.objects.length) {
                process.nextTick(function () {
                        return (end());
                });
        } else {
                this.on('drain', function () {
                        return (end());
                });
        }
};


///--- Private methods

Pool.prototype._reap = function _reap() {
        var first;
        var log = this.log;
        var self = this;

        function walk() {
                var age;
                var obj;

                if (!(obj = dequeue(self.available)) || obj === first) {
                        if (obj)
                                enqueue(self.available, obj);

                        self._scheduleReaper();
                        return (false);
                }
                first = first || obj;

                if (log.trace()) {
                        log.trace({state: self._state()},
                                  '_reap:: inspecting %s', util.inspect(obj));
                }

                age = Date.now() - obj.atime;
                if (age < self.maxIdleTime) {
                        enqueue(self.available, obj);
                        return (walk());
                }

                self.check(obj.client, function (err) {
                        if (err) {
                                self._kill(obj);
                        } else {
                                enqueue(self.available, obj);
                        }


                        return (walk());
                });

                return (true);
        }

        if (log.trace())
                log.trace({state: self._state()}, 'Checking available objects');

        return (walk());
};


Pool.prototype._kill = function _kill(obj) {
        var log = this.log;
        var self = this;

        if (log.trace())
                log.trace({state: self._state()}, 'killing object: %d', obj.id);

        removePoolObject(this.available, obj);
        removePoolObject(this.objects, obj);

        // Deregister
        if (obj.client instanceof EventEmitter) {
                self.events.forEach(function (e) {
                        (obj.client.listeners(e) || []).forEach(function (l) {
                                if (l.name === '__poolingWatch')
                                        obj.client.removeListener(e, l);
                        });
                });
        }

        if (typeof (this.destroy) === 'function')
                this.destroy(obj.client);

        process.nextTick(function () {
                self.emit('death', obj.client);
        });

};


Pool.prototype._scheduleReaper = function _scheduleReaper() {
        var self = this;

        if (this.stop)
                return (false);

        this.timer = setTimeout(function reaper() {
                return (self._reap());
        }, this.checkInterval);

        return (true);
};


Pool.prototype._state = function _state() {
        var self = this;

        return {
                available: self.available.length,
                objects: self.objects.length,
                queue: self.queue.length,
                interval: self.interval,
                max: self.max,
                min: self.min,
                stop: self.stop,
                timeout: self.timeout
        };
};


Pool.prototype._watch = function _watch(client) {
        var log = this.log;
        var obj;
        var self = this;

        if (!(client instanceof EventEmitter))
                return (false);

        this.events.forEach(function (e) {
                client.once(e, function __poolingWatch() {
                        if (log.trace()) {
                                var a = Array.prototype.slice.call(arguments);
                                log.trace({
                                        args: a.join(', '),
                                        event: e,
                                        state: self._state()
                                }, 'client event triggering closure');
                        }

                        // Now purge from ourself
                        if ((obj = getPoolObject(self.objects, client, true)))
                                self._kill(obj);
                });
        });

        return (true);
};
