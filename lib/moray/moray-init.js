/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * This module is a helper module aimed at making initializing the Moray
 * database layer a bit easier than having to use the several different
 * subsystems involved. Instead, it exports one function, "startMorayInit", that
 * can be called to perform all of the steps required.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var mod_moray = require('moray');
var restify = require('restify');

var Moray = require('../apis/moray');
var MorayBucketsInitializer = require('./moray-buckets-initializer.js');
var DEFAULT_MORAY_BUCKETS_CONFIG = require('./moray-buckets-config.js');

/*
 * Creates and returns an object that represents the appropriate options to pass
 * when calling moray.createClient.
 *
 * @param {Object} morayConfig: an object that represents the desired moray
 * configuration.
 */
function createMorayClientOpts(morayConfig) {
    assert.object(morayConfig, 'morayConfig');

    assert.object(morayConfig, 'morayConfig');

    var morayClientOpts = jsprim.deepCopy(morayConfig);
    var morayClientLogger = bunyan.createLogger({
        name: 'moray-client',
        level: 'info',
        serializers: restify.bunyan.serializers
    });

    morayClientOpts.log = morayClientLogger;

    var DEFAULT_MORAY_CONNECTION_RETRY_MIN_TIMEOUT = 1000;
    var morayConnectionMinTimeout = DEFAULT_MORAY_CONNECTION_RETRY_MIN_TIMEOUT;

    var DEFAULT_MORAY_CONNECTION_RETRY_MAX_TIMEOUT = 16000;
    var morayConnectionMaxTimeout = DEFAULT_MORAY_CONNECTION_RETRY_MAX_TIMEOUT;

    if (morayConfig.retry && morayConfig.retry.minTimeout !== undefined) {
        assert.number(morayConfig.retry.minTimeout,
            'morayConfig.retry.minTimeout');
        morayConnectionMinTimeout = morayConfig.retry.minTimeout;
    }

    if (morayConfig.retry && morayConfig.retry.maxTimeout !== undefined) {
        assert.number(morayConfig.retry.maxTimeout,
            'morayConfig.retry.maxTimeout');
        morayConnectionMaxTimeout = morayConfig.retry.maxTimeout;
    }

    var morayConnectTimeout;
    if (morayConfig.connectTimeout !== undefined) {
        assert.number(morayConfig.connectTimeout,
            'morayConfig.connectTimeout');
        morayConnectTimeout = morayConfig.connectTimeout;
    }

    morayClientOpts.connectTimeout = morayConnectTimeout;

    /*
     * Because there is no way to differentiate recoverable from unrecoverable
     * errors with the node-moray module currently used by VMAPI, we setup a
     * no-op error handler for moray clients created by the "setupMorayStorage"
     * function below. It means that, if we were to setup a finite number of
     * retries, we wouldn't be able to know when this number has been reached,
     * so instead we create the options object so that moray clients try to
     * connect indefinitely. This is the expected behavior for the VMAPI server,
     * but also for all tools and tests in VMAPI.
     */
    morayClientOpts.retry = {
        retries: Infinity,
        minTimeout: morayConnectionMinTimeout,
        maxTimeout: morayConnectionMaxTimeout
    };

    return morayClientOpts;
}

/*
 * Starts the initialization of the moray storage layer and calls "callback"
 * when the process started.
 *
 * Parameters:
 *
 * - "options":
 *
 * - "options.morayConfig": an object that represents the settings to use to
 *   connect to a moray server.
 *
 * - "options.maxBucketsSetupAttempts": the maximum number of attempts to be
 *   used by the MorayBucketsInitializer instance that is driving the moray
 *   buckets setup process. If undefined, the MorayBucketsInitializer will retry
 *   indefinitely.
 *
 * - "options.parentLog": a bunyan logger object to use as a parent logger for
 *   any logger created by the moray initialization process.
 *
 * - "callback": a function called when the process has started. It is called
 *   with one parameter: an object with the following properties:
 *
 *    - "morayBucketsInitializer": the instance of MorayBucketsInitializer used
 *      to setup moray buckets. Event listeners for the 'ready' and 'error'
 *      events can be setup on this instance to run code when the moray buckets
 *      have been setup, or when an unrecoverable error (including reaching the
 *      maximum number of retires) has occured.
 *
 *    - "moray": the instance of Moray used to perform any operations at the
 *      storage layer.
 *
 *    - "morayClient": the instance of node-moray used to connect to the moray
 *      server.
 *
 * "callback" is _not_ passed an error object. Errors related to the moray
 * client are ignored (see comment below), and errors related to the moray
 * buckets initialization process are emitted on the MorayBucketsInitializer
 * instance passed to the callback's first parameter.
 *
 * Here's how the initialization process is broken down:
 *
 * 1. Creating a node-moray client instance and using it to connect to a moray
 * server according to the settings found in "morayConfig".
 *
 * 2. Creating a Moray instance associated with that client.
 *
 * 3. Creating a MorayBucketsInitializer instance associated to that Moray
 * instance and starting setting up moray buckets.
 */
function startMorayInit(options, callback) {
    if (typeof (options) === 'function') {
        callback = options;
        options = undefined;
    }

    assert.optionalObject(options, 'options');
    assert.func(callback, 'callback');

    options = options || {};

    assert.object(options.morayConfig, 'options.morayConfig');
    assert.optionalObject(options.log, 'options.log');
    assert.optionalNumber(options.maxBucketsReindexAttempts,
        'options.maxBucketsReindexAttempts');
    assert.optionalNumber(options.maxBucketsSetupAttempts,
        'options.maxBucketsSetupAttempts');
    assert.optionalObject(options.morayBucketsConfig,
        'options.morayBucketsConfig');
    assert.object(options.changefeedPublisher, 'options.changefeedPublisher');

    var changefeedPublisher = options.changefeedPublisher;
    var maxBucketsReindexAttempts = options.maxBucketsReindexAttempts;
    var maxBucketsSetupAttempts = options.maxBucketsSetupAttempts;
    var morayBucketsConfig = options.morayBucketsConfig ||
        DEFAULT_MORAY_BUCKETS_CONFIG;
    var morayBucketsInitializerLog;
    var morayClient;
    var morayClientOpts;
    var morayConfig = options.morayConfig;
    var moray;
    var morayStorageLog;
    var log = options.log;

    morayClientOpts = createMorayClientOpts(morayConfig);
    morayClient = mod_moray.createClient(morayClientOpts);

    if (log === undefined) {
        log = bunyan.createLogger({
            name: 'moray-init',
            level: 'info',
            serializers: restify.bunyan.serializers
        });
    }

    morayStorageLog = log.child({
        component: 'moray-storage'
    }, true);

    morayBucketsInitializerLog = log.child({
        component: 'moray-buckets-initializer'
    }, true);

    moray = new Moray(morayClient, {
        log: morayStorageLog,
        changefeedPublisher: changefeedPublisher
    });

    var morayBucketsInitializer = new MorayBucketsInitializer({
        maxBucketsSetupAttempts: maxBucketsSetupAttempts,
        maxBucketsReindexAttempts: maxBucketsReindexAttempts,
        log: morayBucketsInitializerLog
    });

    morayClient.on('connect', function onMorayClientConnected() {

        morayBucketsInitializer.start(moray, morayBucketsConfig);
        callback({
            morayBucketsInitializer: morayBucketsInitializer,
            moray: moray,
            morayClient: morayClient
        });
    });

    morayClient.on('error', function onMorayClientError(morayClientErr) {
        /*
         * The current behavior of the underlying node-moray client means that
         * it can emit 'error' events for errors that the client can actually
         * recover from and that don't prevent it from establishing a
         * connection. See MORAY-309 for more info.
         *
         * Since it's expected that, at least in some environments, the moray
         * client will fail to connect a certain number of times, and we don't
         * want the process to abort in that case, we setup an intentionally
         * no-op 'error' event listener here, and setup the moray client to
         * retry connecting indefinitely in "createMorayClientOpts". If the
         * moray client is not able to connect, then the process will hang or
         * time out.
         */
    });
}

exports.startMorayInit = startMorayInit;