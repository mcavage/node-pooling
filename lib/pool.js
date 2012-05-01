// Copyright (c) 2012, Mark Cavage. All rights reserved.

var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var vasync = require('vasync');

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
        this.resources = [];
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

        if (this.resources.length >= this.max) {
                enqueue(this.queue, callback);
                return (false);
        }

        this.create(function createCallback(err, client) {
                if (err)
                        return (callback(err));

                // Handle shutdown being called while a
                // new client was being made
                if (self.stop)
                        return (self.destroy(obj.client));

                obj = createPoolObject(client);
                self._watch(client);
                enqueue(self.resources, obj);

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

        if (log.trace()) {
                log.trace({
                        state: self._state(),
                        client: util.inspect(client)
                }, 'release entered');
        }

        if (!(obj = getPoolObject(this.resources, client)))
                return (false);

        enqueue(this.available, obj);
        if ((waiter = dequeue(this.queue))) {
                process.nextTick(function runNextWaiter() {
                        self.acquire(waiter);
                });
        } else if (this.available.length === this.resources.length) {
                self.emit('drain');
        }

        return (true);
};


Pool.prototype.shutdown = function shutdown(callback) {
        var log = this.log;
        var self = this;

        function end() {
                self.resources.forEach(function (o) {
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

        if (this.available.length === this.resources.length) {
                process.nextTick(function () {
                        return (end());
                });
        } else {
                this.on('drain', end);
        }
};


///--- Private methods

Pool.prototype._reap = function _reap() {
        var toCheck = [];
        var log = this.log;
        var self = this;

        if (this.stop)
                return (false);

        if (log.trace())
                log.trace({state: self._state()}, 'Checking available objects');

        // Kind of ghetto - we temporarily purge the available list
        // and then run health checks over those that need it.
        this.available = this.available.filter(function (obj) {
                var skip = ((Date.now() - obj.atime) < self.maxIdleTime);
                if (!skip)
                        toCheck.push(obj);

                return (skip);
        });

        if (toCheck.length === 0) {
                if (log.trace()) {
                        log.trace({state: self._state()},
                                  '_reap:: nothing to reap');
                }
                return (self._scheduleReaper());
        }

        if (log.trace()) {
                log.trace({
                        state: self._state(),
                        toCheck: toCheck.map(function (obj) {
                                return (util.inspect(obj));
                        })
                }, '_reap:: checking idle objects');
        }

        // Run over all the "locked" items in ||
        var opts = {
                func: function iterator(obj, cb) {
                        return (self.check(obj.client, function (err) {
                                return (cb(err, obj));
                        }));
                },
                inputs: toCheck
        };
        vasync.forEachParallel(opts, function (err, results) {
                if (log.trace()) {
                        log.trace({
                                state: self._state(),
                                results: util.inspect(results, null, 2)
                        }, '_reap:: check done');
                }
                results.operations.forEach(function (op) {
                        if (op.status === 'ok') {
                                self.available.push(op.result);
                        } else {
                                self._kill(op.result);
                        }
                });

                self._scheduleReaper();
        });

        return (true);
};


Pool.prototype._kill = function _kill(obj) {
        var log = this.log;
        var self = this;

        if (log.trace())
                log.trace({state: self._state()}, 'killing object: %d', obj.id);

        removePoolObject(this.available, obj);
        removePoolObject(this.resources, obj);

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
        var log = this.log;
        var self = this;

        if (this.stop)
                return (false);

        this.timer = setTimeout(function reaper() {
                return (self._reap());
        }, this.checkInterval);

        if (log.trace())
                log.trace({state: self._state()}, 'reaper scheduled');

        return (true);
};


Pool.prototype._state = function _state() {
        var self = this;

        return {
                available: self.available.length,
                resources: self.resources.length,
                queue: self.queue.length,
                interval: self.interval,
                max: self.max,
                min: self.min,
                stop: self.stop,
                timer: self.timer ? util.inspect(self.timer) : null,
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
                        if ((obj = getPoolObject(self.resources, client, true)))
                                self._kill(obj);
                });
        });

        return (true);
};
