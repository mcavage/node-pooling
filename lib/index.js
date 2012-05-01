// Copyright (c) 2012, Mark Cavage. All rights reserved.

var bunyan = require('bunyan');

var assertions = require('./assert');
var Pool = require('./pool');



///--- Globals

var assertObject = assertions.assertObject;

var LOG = bunyan.createLogger({
        name: 'pooling',
        stream: process.stderr,
        level: 'warn',
        serializers: {
                err: bunyan.stdSerializers.err
        }
});



///--- Internal Functions

function defaultCheck(callback) {
        return (callback(new Error('idle timeout reached')));
}



///--- Exports

module.exports = {

        createPool: function createPool(options) {
                assertObject('options', options);

                var pool = new Pool({
                        check: options.check || defaultCheck,
                        checkInterval: options.checkInterval || 30000, // 30s
                        create: options.create,
                        destroy: options.destroy,
                        events: options.events,
                        log: options.log || LOG,
                        max: options.max,
                        maxIdleTime: options.maxIdleTime || 3600000, // 1hr
                        name: options.name || 'pooling'
                });

                return (pool);
        }

};
