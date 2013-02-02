// Copyright 2012 Mark Cavage.  All rights reserved.
//
// Just a simple wrapper over nodeunit's exports syntax. Also exposes
// a common logger for all tests.
//

var bunyan = require('bunyan');
var once = require('once');



///--- Exports

module.exports = {

        after: function after(teardown) {
                module.parent.exports.tearDown = teardown;
        },

        before: function before(setup) {
                module.parent.exports.setUp = setup;
        },

        test: function test(name, tester) {
                module.parent.exports[name] = function _(t) {
                        t.end = once(function () {
                                t.done();
                        });
                        return (tester(t));
                };
        },

        log:  bunyan.createLogger({
                level: (process.env.LOG_LEVEL || 'info'),
                name: process.argv[1],
                stream: process.stdout,
                src: true,
                serializers: {
                        err: bunyan.stdSerializers.err
                }
        })

};
