/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * Handler for the /ping endpoint including data about connected backing
 * services.
 */

var assert = require('assert-plus');
var restify = require('restify');
var vasync = require('vasync');

var common = require('../common');

var ONLINE_STATUS = 'online';
var OFFLINE_STATUS = 'offline';

/*
 * GET /ping
 */
function ping(req, res, next) {
    var morayInitialization;
    var morayStatus = OFFLINE_STATUS;
    var wfapiServiceStatus = OFFLINE_STATUS;
    var overallHealthy = true;
    var overallStatus = 'OK';
    var pingErrors = {};
    var response = {};
    var responseCode = 200;

    vasync.parallel({funcs: [
        function getMorayConnectivity(done) {
            req.log.debug('pinging moray...');

            req.app.moray.ping(function onMorayPinged(pingErr) {
                if (pingErr) {
                    req.log.debug({
                        err: pingErr
                    }, 'moray ping error');
                } else {
                    req.log.debug('successfully pinged moray');
                }

                if (!pingErr) {
                    morayStatus = ONLINE_STATUS;
                } else {
                    overallHealthy = false;
                    pingErrors.moray = pingErr;
                }

                done();
            });
        },
        function getMorayInitialization(done) {
            req.log.debug('checking moray initialization status...');

            var morayBucketsInitStatus =
                req.app.morayBucketsInitializer.status();
            var morayBucketsInitError =
                req.app.morayBucketsInitializer.lastInitError();

            assert.optionalObject(morayBucketsInitError,
                'morayBucketsInitError');
            if (morayBucketsInitError) {
                morayBucketsInitError = morayBucketsInitError.toString();
            }

            if (morayBucketsInitError ||
                ((morayBucketsInitStatus !== 'BUCKETS_SETUP_DONE') &&
                (morayBucketsInitStatus !== 'BUCKETS_REINDEX_DONE'))) {
                overallHealthy = false;
            }

            req.log.debug({
                error: morayBucketsInitError,
                status: morayBucketsInitStatus
            }, 'moray initialization check results');

            morayInitialization = {
                status: morayBucketsInitStatus
            };

            if (morayBucketsInitError) {
                morayInitialization.error = morayBucketsInitError;
            }

            done();
        },
        function getWfApiConnectivity(done) {
            req.log.debug({wfapiUrl: req.app.wfapi.url},
                'checking wfapi connectivity...');

            if (req.app.wfapi && req.app.wfapi.connected === true) {
                wfapiServiceStatus = ONLINE_STATUS;
            } else {
                overallHealthy = false;
            }

            req.log.debug({
                status: wfapiServiceStatus
            }, 'wfapi connectivity check results');

            done();
        }
    ]}, function allStatusInfoRetrieved(err) {
        req.log.debug('all status info retrieved');

        var services = {
            moray: morayStatus,
            wfapi: wfapiServiceStatus
        };

        if (overallHealthy === false) {
            responseCode = 503;
            overallStatus = 'some services are not ready';
        }

        response.healthy = overallHealthy;
        response.initialization = {
            moray: morayInitialization
        };
        response.pid = process.pid;
        response.pingErrors = pingErrors;
        response.status = overallStatus;
        response.services = services;

        res.send(responseCode, response);

        return next();
    });
}



/*
 * Mounts job actions as server routes
 */
function mount(server) {
    server.get({ path: '/ping', name: 'Ping' }, ping);
}


// --- Exports

module.exports = {
    mount: mount
};
