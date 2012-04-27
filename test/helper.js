// Copyright 2012 Mark Cavage.  All rights reserved.
//
// Just a simple wrapper over nodeunit's exports syntax. Also exposes
// a common logger for all tests.
//

var Logger = require('bunyan');



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
                        var _done = false;
                        t.end = function end() {
                                if (!_done) {
                                        _done = true;
                                        t.done();
                                }
                        };
                        return (tester(t));
                };
        },

        log:  new Logger({
                level: (process.env.LOG_LEVEL || 'info'),
                name: process.argv[1],
                stream: process.stdout,
                src: true,
                serializers: {
                        err: Logger.stdSerializers.err
                }
        })

};
