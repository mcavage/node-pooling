// Copyright (c) 2012, Mark Cavage. All rights reserved.

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var vasync = require('vasync');



///--- Globals

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
        assert.object(options, 'options');
        assert.optionalFunc(options.assert, 'options.assert');
        assert.func(options.check, 'options.check');
        assert.number(options.checkInterval, 'options.checkInterval');
        assert.func(options.create, 'options.create');
        assert.object(options.log, 'options.log');
        assert.number(options.max, 'options.max');
        assert.number(options.maxIdleTime, 'options.maxIdleTime');
        assert.string(options.name, 'options.name');

        EventEmitter.call(this, options);

        this.available = [];
        this.assert = options.assert || function () { return (true); };
        this.create = options.create;
        this.check = options.check;
        this.checkInterval = options.checkInterval;
        this.destroy = options.destroy || false;
        this.events = (options.events || DEFAULT_EVENTS).slice();
        this.log = options.log.child({pool: options.name}, true);
        this.max = Math.max(options.max, 1);
        this.maxIdleTime = options.maxIdleTime;
        this.name = options.name;
        this.pendingResources = 0;
        this.queue = [];
        this.resources = [];
        this.stopped = false;
        this.stopping = false;
        this.timer = false;

        this._scheduleReaper();
}
util.inherits(Pool, EventEmitter);
module.exports = Pool;


Pool.prototype.acquire = function acquire(callback) {
        assert.func(callback, 'callback');

        var log = this.log;
        var obj;
        var self = this;

        if (log.trace())
                log.trace({state: self._state()}, 'acquire entered');

        if (this.stopping)
                return (callback(new Error('pool is shutting down')));

        if (this.stopped)
                return (callback(new Error('pool has been closed')));

        if ((obj = dequeue(this.available))) {
                obj.atime = Date.now();
                if (this._ensure(obj)) {
                        callback(null, obj.client);
                        return (true);
                } else {
                        return (acquire(callback));
                }
        }

        if (this.resources.length + this.pendingResources >= this.max) {
                enqueue(this.queue, callback);
                return (false);
        }

        this.pendingResources++;
        this.create(function createCallback(err, client) {
                self.pendingResources = Math.max(self.pendingResources - 1, 0);
                if (err)
                        return (callback(err));

                // Handle shutdown being called while a
                // new client was being made
                if (self.stopped || self.stopping) {
                        self.destroy(client);
                        return (callback(new Error('pool is shutting down')));
                }

                obj = createPoolObject(client);
                self._watch(client);
                enqueue(self.resources, obj);

                return (callback(null, client));
        });
        return (true);
};


Pool.prototype.remove = function remove(client) {
        assert.object(client, 'client');

        var log = this.log;
        var obj;
        var self = this;

        if (log.trace()) {
                log.trace({
                        state: self._state(),
                        client: util.inspect(client)
                }, 'destroy entered');
        }

        if (!(obj = getPoolObject(this.resources, client)))
                return (false);

        this._kill(obj);
        process.nextTick(function emitDrain() {
                if (self.available.length === self.resources.length)
                        self.emit('drain');
        });
        return (true);
};


Pool.prototype.release = function release(client) {
        assert.object(client, 'client');

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

        if (!this._ensure(obj))
                return (true);


        function emitDrain() {
                if (self.available.length === self.resources.length)
                        self.emit('drain');
        }

        if (this.stopping) {
                this._kill(obj);
                process.nextTick(emitDrain);
        } else if (this.stopped) {
                // Done
                this._kill(obj);
        } else {
                enqueue(this.available, obj);
                if ((waiter = dequeue(this.queue))) {
                        process.nextTick(function runNextWaiter() {
                                self.acquire(waiter);
                        });
                } else {
                        process.nextTick(emitDrain);
                }
        }

        return (true);
};


Pool.prototype.shutdown = function shutdown(callback) {
        var log = this.log;
        var self = this;

        function end() {
                while (self.resources.length !== 0) {
                        self._kill(self.resources[0]);
                }

                if (!self.stopped) {
                        log.trace('emitting end');
                        self.emit('end');
                }

                self.stopped = true;
                self.stopping = false;
                return (typeof (callback) === 'function' ? callback() : false);
        }

        if (log.trace())
                log.trace({state: self._state()}, 'shutdown entered');

        if (this.stopped)
                return (end());

        this.stopping = true;
        if (this.timer)
                clearTimeout(this.timer);

        if (this.available.length === this.resources.length) {
                process.nextTick(function () {
                        return (end());
                });
        } else {
                this.once('drain', end);
        }

        return (undefined);
};


///--- Private methods

Pool.prototype._ensure = function _ensure(obj) {
        var ok = false;
        var log = this.log;

        try {
                ok = this.assert(obj.client);
                if (ok === undefined)
                        ok = true;
        } catch (e) {
                if (log.trace()) {
                        log.trace({
                                err: e,
                                client: util.inspect(obj.client)
                        }, '_ensure: assert failed');
                }
                ok = false;
        }

        if (!ok) {
                this._kill(obj);
                return (false);
        }

        return (true);
};


Pool.prototype._reap = function _reap() {
        var toCheck = [];
        var self = this;

        if (this.stopped || this.stopping)
                return (false);

        // Kind of ghetto - we temporarily purge the available list
        // and then run health checks over those that need it.
        this.available = this.available.filter(function (obj) {
                var skip = ((Date.now() - obj.atime) < self.maxIdleTime);
                if (!skip)
                        toCheck.push(obj);

                return (skip);
        });

        if (toCheck.length === 0) {
                return (self._scheduleReaper());
        }

        // Run over all the "locked" items in ||
        var opts = {
                func: function iterator(obj, cb) {
                        var done = false;
                        function _cb(err) {
                                if (!done) {
                                        done = true;
                                        cb(err, obj);
                                }
                        }

                        if (!self._ensure(obj))
                                return (_cb(new Error('dead client')));

                        try {
                                self.check(obj.client, _cb);
                        } catch (e) {
                                _cb(e);
                        }

                        return (undefined);
                },
                inputs: toCheck
        };
        vasync.forEachParallel(opts, function (err, results) {
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
        var self = this;

        if (this.stopped || this.stopping)
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
                resources: self.resources.length,
                queue: self.queue.length,
                interval: self.interval,
                max: self.max,
                min: self.min,
                stopped: self.stopped,
                stopping: self.stopping,
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
