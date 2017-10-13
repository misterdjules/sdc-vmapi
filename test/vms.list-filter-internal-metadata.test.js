/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var async = require('async');
var bunyan = require('bunyan');
var Logger = require('bunyan');
var restify = require('restify');
var util = require('util');

var changefeedUtils = require('../lib/changefeed');
var common = require('./common');
var morayInit = require('../lib/moray/moray-init');
var validation = require('../lib/common/validation');
var vmTest = require('./lib/vm');

var longMetadataValue = Buffer.alloc(101, 'a');
longMetadataValue.write('b', 100, 1);

var client;
var moray;
var morayClient;

var testLogger = bunyan.createLogger({
    name: 'test-internal-metadata',
    level: 'debug',
    serializers: restify.bunyan.serializers
});

exports.setUp = function (callback) {
    common.setUp(function (err, _client) {
        assert.ifError(err);
        assert.ok(_client, 'restify client');
        client = _client;
        callback();
    });
};

exports.init_storage_layer = function (t) {
    var morayBucketsInitializer;

    var moraySetup = morayInit.startMorayInit({
        morayConfig: common.config.moray,
        maxBucketsReindexAttempts: 1,
        maxBucketsSetupAttempts: 1,
        changefeedPublisher: changefeedUtils.createNoopCfPublisher()
    });

    morayBucketsInitializer = moraySetup.morayBucketsInitializer;
    morayClient = moraySetup.morayClient;
    moray = moraySetup.moray;

    morayBucketsInitializer.on('done', function onMorayStorageReady() {
        t.done();
    });
};

exports.cleanup_leftover_test_vms = function (t) {
    vmTest.deleteTestVMs(moray, {}, function onTestVmsDeleted(delTestVmsErr) {
        t.ifError(delTestVmsErr, 'Deleting test VMs should not error');
        t.done();
    });
};

exports.list_invalid_empty_metadata_key = function (t) {
    var expectedError = {
        code: 'ValidationFailed',
        message: 'Invalid Parameters',
        errors: [ {
            field: 'internal_metadata',
            code: 'Invalid',
            message: 'Invalid internal_metadata key: ""'
        } ]
    };
    var listVmsQuery = '/vms?internal_metadata.=foo';

    client.get(listVmsQuery, function onListVms(err, req, res, body) {
        t.ok(err,
            'listing VMs using invalid internal_metadata key should error');
        if (err) {
            t.deepEqual(body, expectedError, 'Error should be equal to ' +
                expectedError + ', got: ' + err);
        }

        t.done();
    });
};

exports.list_invalid_metadata_key = function (t) {
    var expectedError = {
        code: 'ValidationFailed',
        message: 'Invalid Parameters',
        errors: [ {
            field: 'internal_metadata',
            code: 'Invalid',
            message: 'Invalid internal_metadata key: "bar"'
        } ]
    };
    var listVmsQuery = '/vms?internal_metadata.bar=foo';

    client.get(listVmsQuery, function onListVms(err, req, res, body) {
        t.ok(err,
            'listing VMs using invalid internal_metadata key should error');
        if (err) {
            t.deepEqual(body, expectedError, 'Error should be equal to ' +
                expectedError + ', got: ' + err);
        }

        t.done();
    });
};


exports.list_invalid_metadata_value = function (t) {
    var expectedError = {
        code: 'ValidationFailed',
        message: 'Invalid Parameters',
        errors: [ {
            field: 'internal_metadata',
            code: 'Invalid',
            message: 'Invalid internal_metadata value: ""'
        } ]
    };
    var listVmsQuery = '/vms?internal_metadata.foo:bar=';

    client.get(listVmsQuery, function onListVms(err, req, res, body) {
        t.ok(err,
            'listing VMs using invalid internal_metadata key should error');
        if (err) {
            t.deepEqual(body, expectedError, 'Error should be equal to ' +
                expectedError + ', got: ' + err);
        }

        t.done();
    });
};

exports.create_test_vm_records = function (t) {
    vmTest.createTestVMs(1, moray,
        {concurrency: 1, log: testLogger},
        {internal_metadata: {'some:key': 'foo'}},
            function fakeVmsCreated(err, vmUuids) {
                t.ifError(err, 'Creating test VM should not error');
                t.done();
            });
};

exports.list_valid_internal_metadata = function (t) {
    var listVmsQuery = '/vms?internal_metadata.some:key=foo';

    client.get(listVmsQuery, function onListVms(err, req, res, body) {
        t.ifError(err,
            'Listing VMs using valid internal_metadata filter should not ' +
                'error');
        t.ok(body, 'response should not be empty');
        if (body) {
            t.equal(body.length, 1, 'Response should include just one VM');
        }

        t.done();
    });
};

exports.list_valid_internal_metadata_with_predicate = function (t) {
    var listVmsQuery;
    var predicate = JSON.stringify({
        eq: ['internal_metadata.some:key', 'foo']
    });

    listVmsQuery = '/vms?predicate=' + predicate;

    client.get(listVmsQuery, function onListVms(err, req, res, body) {
        t.ifError(err,
            'Listing VMs using valid internal_metadata predicate should not ' +
                'error');
        t.ok(body, 'response should not be empty');
        if (body) {
            t.equal(body.length, 1, 'Response should include just one VM');
        }

        t.done();
    });
};

exports.create_test_vm_records_with_long_metadata_value = function (t) {
    vmTest.createTestVMs(1, moray,
        {concurrency: 1, log: testLogger},
        {internal_metadata: {'some:key': longMetadataValue.toString()}},
            function fakeVmsCreated(err, vmUuids) {
                t.ifError(err, 'Creating test VM should not error');
                t.done();
            });
};

exports.list_long_internal_metadata = function (t) {
    var listVmsQuery;
    var queryMetadataValue = longMetadataValue.toString();

    listVmsQuery = '/vms?internal_metadata.some:key=' + queryMetadataValue;

    client.get(listVmsQuery, function onListVms(err, req, res, body) {
        t.ifError(err,
            'Listing VMs using valid internal_metadata filter should not ' +
                'error');
        t.ok(body, 'response should not be empty');
        if (body) {
            t.equal(body.length, 0, 'Response should not include any VM');
        }

        t.done();
    });
};

exports.list_long_internal_metadata_with_shorter_value = function (t) {
    var clampedMetadataValue = longMetadataValue.slice(0, 100).toString();
    var listVmsQuery = '/vms?internal_metadata.some:key=' +
        clampedMetadataValue;

    client.get(listVmsQuery, function onListVms(err, req, res, body) {
        t.ifError(err,
            'Listing VMs using valid internal_metadata filter should not ' +
                'error');
        t.ok(body, 'response should not be empty');
        if (body) {
            t.equal(body.length, 1, 'Response should include just one VM');
        }

        t.done();
    });
};

exports.cleanup_test_vms = function (t) {
    vmTest.deleteTestVMs(moray, {}, function onTestVmsDeleted(delTestVmsErr) {
        t.ifError(delTestVmsErr, 'Deleting test VMs should not error');
        t.done();
    });
};

exports.close_clients = function (t) {
    morayClient.close();
    client.close();
    t.done();
};