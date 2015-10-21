/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

// The goal of this test is to make sure that, when creating a VM with a
// specific alias, then deleting it and creating a new VM again with the
// same alias, the second creation doesn't fail e.g due to a duplicate alias
// error.

var libuuid = require('libuuid');
var assert = require('assert-plus');
var vasync = require('vasync');

var common = require('./common');
var moray = require('../lib/apis/moray');
var testCommon = require('./common');
var vmTest = require('./lib/vm');
var workflow = require('./lib/workflow');

var client;

var VMS_LIST_ENDPOINT = '/vms';
var NON_EXISTING_CN_UUID = libuuid.create();
var TEST_VMS_ALIAS = vmTest.TEST_VMS_ALIAS + '-create-delete-same-alias-test';
var IMAGE = 'fd2cc906-8938-11e3-beab-4359c665ac99';
var CUSTOMER = testCommon.config.ufdsAdminUuid;
var NETWORKS = null;
var SERVER = null;

var leftoverTestVms = [];
var leftoverTestVmsDestroyJobUuids = [];
var firstTestVmLocation;
var secondTestVmLocation;

function createTestVmPayload(opts) {
    assert.object(opts, 'opts');
    assert.string(opts.ownerUuid, 'opts.ownerUuid');
    assert.string(opts.creatorUuid, 'opts.creatorUuid');
    assert.string(opts.imageUuid, 'opts.imageUuid');
    assert.object(opts.server, 'opts.server');
    assert.arrayOfObject(opts.networks, 'opts.networks');
    assert.string(opts.alias, 'opts.alias');

    return {
        owner_uuid: opts.ownerUuid,
        creator_uuid: opts.creatorUuid,
        image_uuid: opts.imageUuid,
        server_uuid: opts.server.uuid,
        networks: [ { uuid: opts.networks[0].uuid } ],
        brand: 'joyent-minimal',
        billing_id: '00000000-0000-0000-0000-000000000000',
        ram: 64,
        quota: 10,
        alias: opts.alias
    };
}

function createTestVm(opts, callback) {
    assert.object(opts, 'opts');
    assert.func(callback, 'callback');

    var createReqOpts = {path: VMS_LIST_ENDPOINT};
    var VM_PAYLOAD = createTestVmPayload(opts);

    var vmLocation;
    var jobLocation;

    vasync.pipeline({
        funcs: [
            function createVm(args, next) {
                client.post(createReqOpts, VM_PAYLOAD,
                    function (err, req, res, body) {
                        jobLocation = '/jobs/' + body.job_uuid;
                        vmLocation = '/vms/' + body.vm_uuid;
                        return next(err);
                    });
            },
            function waitForJobCompletion(args, next) {
                workflow.waitForValue(client, jobLocation, 'execution',
                    'succeeded', function onJobCompleted(err) {
                        return next(err);
                    });
            }
        ]
    }, function vmCreationDone(err, results) {
        return callback(err, vmLocation);
    });
}

exports.setUp = function (callback) {
    common.setUp(function (err, _client) {
        assert.ifError(err);
        assert.ok(_client, 'restify client');
        client = _client;
        callback();
    });
};

exports.find_headnode = function (t) {
    client.cnapi.get('/servers?headnode=true',
        function (err, req, res, servers) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.ok(servers);
            t.ok(Array.isArray(servers));
            for (var i = 0; i < servers.length; i++) {
                if (servers[i].headnode === true) {
                    SERVER = servers[i];
                    break;
                }
            }
            t.ok(SERVER);
            t.done();
        });
};

exports.napi_networks_ok = function (t) {
    client.napi.get('/networks?provisionable_by=' + CUSTOMER,
        function (err, req, res, networks) {
            t.ifError(err);
            t.equal(res.statusCode, 200);
            t.ok(networks);
            t.ok(Array.isArray(networks));
            t.ok(networks.length > 1);
            NETWORKS = networks;
            t.done();
        });
};

/*
 * Fist, delete any leftover VMs from a previous tests run that may not have
 * been cleaned up properly.
 */
exports.get_leftover_test_vms = function (t) {
    vasync.pipeline({
        funcs: [
            function getDestroyingLeftoverVms(args, callback) {
                client.get(VMS_LIST_ENDPOINT + '?alias=' +
                    TEST_VMS_ALIAS + '&transitive_state=destroying',
                    function (err, req, res, body) {
                        var expectedStatusCode = 200;
                        t.equal(res.statusCode, expectedStatusCode,
                            'Listing all destroying VMs should respond with ' +
                            'HTTP status code ' + expectedStatusCode);
                        t.ok(Array.isArray(body),
                            'response should represent an array');

                        leftoverTestVms = leftoverTestVms.concat(body);
                        return callback(err);
                    });
            },
            function getActiveLeftoverVms(args, callback) {
                client.get(VMS_LIST_ENDPOINT + '?alias=' +
                    TEST_VMS_ALIAS + '&state=active',
                    function (err, req, res, body) {
                        var expectedStatusCode = 200;
                        t.equal(res.statusCode, expectedStatusCode,
                            'Listing all destroying VMs should respond with ' +
                            'HTTP status code ' + expectedStatusCode);
                        t.ok(Array.isArray(body),
                            'response should represent an array');

                        leftoverTestVms = leftoverTestVms.concat(body);
                        return callback(err);
                    });
            }
        ]
    }, function allDone(err) {
        t.ifError(err);
        t.done();
    });
};

exports.remove_leftover_test_vms = function (t) {
    function removeLeftoverVm(testVm, callback) {
        var leftoverVmLocation = '/vms/' + testVm.uuid;
        client.del(leftoverVmLocation,
            function (err, req, res, body) {
                leftoverTestVmsDestroyJobUuids.push(body.job_uuid);
                return callback(err);
            });
    }

    vasync.forEachPipeline({
        inputs: leftoverTestVms,
        func: removeLeftoverVm
    }, function allVmsDestroyed(err) {
        t.ifError(err,
            'Queueing deletion for all leftover VMs should be successful');
        t.done();
    });
};

exports.wait_for_leftover_vms_to_actually_be_destroyed = function (t) {
    vasync.forEachParallel({
        inputs: leftoverTestVmsDestroyJobUuids,
        func: function (jobUuid, next) {
            var destroyJobLocation = '/jobs/' + jobUuid;
            workflow.waitForValue(client, destroyJobLocation, 'execution',
                'succeeded', next);
        }
    }, function allDestroyJobsDone(err) {
        t.ifError(err, 'All leftover VMs should be deleted successfully');
        t.done();
    });
};

/*
 * Create a new VM with a specific alias, so that we can create
 * another VM with the same specific alias and check that we don't get
 * a "duplicate alias" error.
 */
exports.create_first_vm = function (t) {
    createTestVm({
        ownerUuid: CUSTOMER,
        creatorUuid: CUSTOMER,
        imageUuid: IMAGE,
        server: SERVER,
        networks: [ { uuid: NETWORKS[0].uuid } ],
        alias: TEST_VMS_ALIAS,
        // set archive on delete so that actual deletion takes some more time
        // and has higher chances to expose the problem of sending a response
        // to a synchronous deletion before the VM is considered inactive.
        archive_on_delete: true
    }, function firstVmCreated(err, vmLocation) {
        firstTestVmLocation = vmLocation;
        t.ifError(err, 'First VM should be created successfully');
        t.done();
    });
};

exports.destroy_first_vm = function (t) {
    client.del(firstTestVmLocation + '?sync=true',
        function onFirstVmDelete(err, req, res, body) {
            var expectedStatusCode = 202;

            t.ifError(err, 'First VM should be deleted successfully');
            t.equal(res.statusCode, expectedStatusCode,
                'Status code should be ' + expectedStatusCode);
            t.done();
        });
};

/*
 * Now that the first VM was deleted, create another VM with the same alias,
 * and make sure that it was created successfully.
 */
exports.create_second_vm_with_same_alias = function (t) {
    createTestVm({
        ownerUuid: CUSTOMER,
        creatorUuid: CUSTOMER,
        imageUuid: IMAGE,
        server: SERVER,
        networks: [ { uuid: NETWORKS[0].uuid } ],
        alias: TEST_VMS_ALIAS,
        // set archive on delete so that actual deletion takes some more time
        // and has higher chances to expose the problem of sending a response
        // to a synchronous deletion before the VM is considered inactive.
        archive_on_delete: true
    }, function secondVmCreated(err, vmLocation) {
        secondTestVmLocation = vmLocation;
        t.ifError(err, 'Second VM should be created successfully');
        t.done();
    });
};

exports.destroy_second_vm = function (t) {
    client.del(secondTestVmLocation + '?sync=true',
        function onSecondVmDeleted(err, req, res, body) {
            var expectedStatusCode = 202;

            t.ifError(err, 'Second VM should be deleted successfully');
            t.equal(res.statusCode, expectedStatusCode,
                'Status code should be ' + expectedStatusCode);
            t.done();
        });
};
