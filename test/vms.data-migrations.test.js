/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var once = require('once');
var path = require('path');
var restify = require('restify');
var util = require('util');
var vasync = require('vasync');
var VMAPI = require('sdc-clients').VMAPI;

var changefeedUtils = require('../lib/changefeed');
var common = require('./common');
var dataMigrations = require('../lib/data-migrations');
var morayInit = require('../lib/moray/moray-init');
var testMoray = require('./lib/moray.js');
var VmapiApp = require('../lib/vmapi');

var DataMigrationsController = dataMigrations.DataMigrationsController;

var MOCKED_WFAPI_CLIENT = {
    connected: true,
    connect: function mockedWfapiConnect(callback) {
        callback();
    }
};

var VMS_BUCKET_NAME = 'test_vmapi_vms_data_migrations';
var SERVER_VMS_BUCKET_NAME = 'test_vmapi_server_vms_data_migrations';
var ROLE_TAGS_BUCKET_NAME = 'test_vmapi_vm_role_tags_data_migrations';

var VMS_BUCKET_CONFIG = {
    name: VMS_BUCKET_NAME,
    schema: {
        index: {
            foo: { type: 'string' },
            bar: { type: 'string' },
            data_version: { type: 'number' }
        }
    }
};

var SERVER_VMS_MORAY_BUCKET_CONFIG = {
    name: SERVER_VMS_BUCKET_NAME,
    schema: {}
};

var ROLE_TAGS_MORAY_BUCKET_CONFIG = {
    name: ROLE_TAGS_BUCKET_NAME,
    schema: {
    }
};

var TEST_BUCKETS_CONFIG = {
    VMS: VMS_BUCKET_CONFIG,
    SERVER_VMS: SERVER_VMS_MORAY_BUCKET_CONFIG,
    VM_ROLE_TAGS: ROLE_TAGS_MORAY_BUCKET_CONFIG
};

var NUM_TEST_OBJECTS = 200;

function findAllObjects(morayClient, bucketName, filter, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.func(callback, 'callback');

    var callbackOnce = once(callback);
    var allRecords = [];

    var findAllObjectsReq = morayClient.findObjects(bucketName, filter);

    findAllObjectsReq.once('error', function onError(findErr) {
        cleanup();
        callbackOnce(findErr);
    });

    findAllObjectsReq.on('record', function onRecord(record) {
        allRecords.push(record);
    });

    findAllObjectsReq.once('end', function onGotAllRecords() {
        cleanup();
        callbackOnce(null, allRecords);
    });

    function cleanup() {
        findAllObjectsReq.removeAllListeners('error');
        findAllObjectsReq.removeAllListeners('record');
        findAllObjectsReq.removeAllListeners('end');
    }
}

function writeObjects(morayClient, bucketName, valueTemplate, nbObjects,
    callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.object(valueTemplate, 'valueTemplate');
    assert.number(nbObjects, 'nbObjects');
    assert.func(callback, 'callback');

    var i;

    var objectKeys = [];
    for (i = 0; i < nbObjects; ++i) {
        objectKeys.push(libuuid.create());
    }

    vasync.forEachParallel({
        func: function writeObject(objectUuid, done) {
            var newObjectValue = jsprim.deepCopy(valueTemplate);
            newObjectValue.uuid = objectUuid;
            /*
             * noBucketCache: true is needed so that when putting objects in
             * moray after a bucket has been deleted and recreated, it doesn't
             * use an old bucket schema and determine that it needs to update an
             * _rver column that doesn't exist anymore.
             */
            morayClient.putObject(bucketName, objectUuid, newObjectValue,
                {noBucketCache: true}, done);
        },
        inputs: objectKeys
    }, callback);
}

exports.moray_data_migrations = function (t) {
    var context = {};

    vasync.pipeline({arg: context, funcs: [
        function cleanup(ctx, next) {
            testMoray.cleanupLeftoverBuckets([
                VMS_BUCKET_NAME,
                SERVER_VMS_BUCKET_NAME,
                ROLE_TAGS_BUCKET_NAME
            ],
            function onCleanupLeftoverBuckets(cleanupErr) {
                t.ok(!cleanupErr,
                    'cleaning up leftover buckets should be successful');
                next(cleanupErr);
            });
        },
        function setupMorayBuckets(ctx, next) {
            var morayBucketsInitializer;
            var morayClient;
            var moraySetup = morayInit.startMorayInit({
                morayConfig: common.config.moray,
                morayBucketsConfig: TEST_BUCKETS_CONFIG,
                changefeedPublisher: changefeedUtils.createNoopCfPublisher()
            });
            var nextOnce = once(next);

            ctx.moray = moraySetup.moray;
            ctx.morayBucketsInitializer = morayBucketsInitializer =
                moraySetup.morayBucketsInitializer;
            ctx.morayClient = morayClient = moraySetup.morayClient;

            function cleanUp() {
                morayBucketsInitializer.removeAllListeners('error');
                morayBucketsInitializer.removeAllListeners('done');
            }

            morayBucketsInitializer.on('done', function onMorayBucketsInit() {
                t.ok(true,
                    'original moray buckets setup should be ' +
                        'successful');

                cleanUp();
                nextOnce();
            });

            morayBucketsInitializer.on('error',
                function onMorayBucketsInitError(morayBucketsInitErr) {
                    t.ok(!morayBucketsInitErr,
                        'original moray buckets initialization should ' +
                            'not error');

                    cleanUp();
                    nextOnce(morayBucketsInitErr);
                });
        },
        function writeTestObjects(ctx, next) {
            assert.object(ctx.morayClient, 'ctx.morayClient');

            writeObjects(ctx.morayClient, VMS_BUCKET_NAME, {
                foo: 'foo'
            }, NUM_TEST_OBJECTS, function onTestObjectsWritten(writeErr) {
                t.ok(!writeErr, 'writing test objects should not error, got: ' +
                    util.inspect(writeErr));
                next(writeErr);
            });
        },
        function loadDataMigrations(ctx, next) {
            var dataMigrationsLoaderLogger = bunyan.createLogger({
                name: 'data-migrations-loader',
                level: 'info',
                serializers: restify.bunyan.serializers
            });

            dataMigrations.loadMigrations(path.resolve(__dirname, 'fixtures',
                'data-migrations'), {log: dataMigrationsLoaderLogger},
                function onMigrationsLoaded(loadMigrationsErr, migrations) {
                    ctx.migrations = migrations;
                    next(loadMigrationsErr);
                });
        },
        function performMigrations(ctx, next) {
            assert.arrayOfObject(ctx.migrations, 'ctx.migrations');
            assert.object(ctx.moray, 'ctx.moray');

            var dataMigrationCtrl =
                new DataMigrationsController(ctx.migrations, {
                    log: bunyan.createLogger({
                        name: 'data-migratons-controller',
                        level: 'info',
                        serializers: restify.bunyan.serializers
                    }),
                    moray: ctx.moray
                });

            dataMigrationCtrl.start();

            dataMigrationCtrl.once('done', function onDataMigrationsDone() {
                t.ok(true,
                    'data migration should eventually complete successfully');
                next();
            });

            dataMigrationCtrl.once('error',
                function onDataMigrationsError(dataMigrationErr) {
                    t.ok(false, 'data migrations should not error, got: ',
                        util.inspect(dataMigrationErr));
                    next(dataMigrationErr);
                });
        },
        function readTestObjects(ctx, next) {
            assert.object(ctx.morayClient, 'ctx.morayClient');

            findAllObjects(ctx.morayClient, VMS_BUCKET_NAME, '(foo=*)',
                function onFindAllObjects(findErr, objects) {
                    var nonMigratedObjects;

                    t.ok(!findErr,
                        'reading all objects back should not error, got: ' +
                            util.inspect(findErr));
                    t.ok(objects,
                        'reading all objects should not return empty response');

                    if (objects) {
                        nonMigratedObjects =
                            objects.filter(function checkObjects(object) {
                                return object.value.bar !== 'foo';
                            });
                        t.equal(nonMigratedObjects.length, 0,
                            'data migrations should have migriated all ' +
                                'objects, got the following non-migrated ' +
                                'objects: ' + nonMigratedObjects.join(', '));
                    }

                    next(findErr);
                });
        }
    ]}, function allMigrationsDone(allMigrationsErr) {
        t.equal(allMigrationsErr, undefined,
                'data migrations test should not error');
        context.morayClient.close();
        t.done();
    });
};
