/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017 Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var moray = require('moray');
var restify = require('restify');
var vasync = require('vasync');
var verror = require('verror');

var common = require('../common');

/*
 * Deletes all buckets whose name is present in the "bucketsName" array. When
 * done or when if an error is encountered, calls the function "callback" with
 * an optional parameter, which is the error that was encountered if any.
 */
function cleanupLeftoverBuckets(bucketsName, callback) {
    assert.arrayOfString(bucketsName, 'bucketsName');
    assert.func(callback, 'callback');

    var morayClientOpts = jsprim.deepCopy(common.config.moray);
    morayClientOpts.retry = {
        retries: Infinity,
        minTimeout: 100,
        maxTimeout: 1000
    };

    morayClientOpts.log = bunyan.createLogger({
        name: 'moray-client',
        level: common.config.logLevel,
        serializers: restify.bunyan.serializers
    });

    var morayClient = moray.createClient(morayClientOpts);

    morayClient.on('connect', function onMorayClientConnected() {
        vasync.forEachParallel({
            func: function deleteBucket(bucketName, done) {
                morayClient.delBucket(bucketName, done);
            },
            inputs: bucketsName
        }, function onAllLeftoverBucketsDeleted(deleteErrs) {
            var unexpectedErrs;
            var forwardedMultiErr;

            morayClient.close();

            if (deleteErrs) {
                unexpectedErrs =
                    deleteErrs.ase_errors.filter(filterBucketNotFoundErr);

                if (unexpectedErrs && unexpectedErrs.length > 0) {
                    forwardedMultiErr =
                        new verror.MultiError(unexpectedErrs);
                }
            }

            callback(forwardedMultiErr);
        });
    });

    morayClient.on('error', function onMorayClientConnectionError(err) {
        /*
         * The current semantics of the underlying node-moray client
         * connection means that it can emit 'error' events for errors
         * that the client can actually recover from and that don't
         * prevent it from establishing a connection. See MORAY-309 for
         * more info.
         *
         * Since it's expected that, at least in some testing
         * environments, the moray client will fail to connect a certain
         * number of times, aborting tests in that case would mean that
         * tests would fail most of the time, even though they should
         * pass. Instead, we explicitly ignore errors and retry
         * connecting indefinitely. If the moray client is not able to
         * connect, then the process will hang or time out.
         */
    });

    function filterBucketNotFoundErr(err) {
        assert.object(err, 'err');
        return err.name !== 'BucketNotFoundError';
    }
}

module.exports = {
    cleanupLeftoverBuckets: cleanupLeftoverBuckets
};
