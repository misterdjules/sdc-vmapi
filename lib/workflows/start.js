/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * A brief overview of this source file: what is its purpose.
 */

var common = require('./job-common');
var nfsVolumes = require('./nfs-volumes');

var VERSION = '7.0.6';


/*
 * Sets up a CNAPI VM action request. Take a look at common.zoneAction. Here you
 * can set parameters such as:
 * - request endpoint (usually the VM endpoint)
 * - jobid (so CNAPI can post status updates back to the job info object)
 * - requestMethod
 * - expects (if you want to check a specific running status of the machine)
 */
function setupStartRequest(job, cb) {
    job.endpoint = '/servers/' +
                   job.params['server_uuid'] + '/vms/' +
                   job.params['vm_uuid'] + '/start';
    job.params.jobid = job.uuid;
    job.requestMethod = 'post';
    job.expects = 'running';
    job.action = 'start';
    job.server_uuid = job.params['server_uuid'];

    // Clear any payload that may have been set by another request.
    if (job.params && job.params.payload) {
        delete job.params.payload;
    }

    return cb(null, 'Start request has been setup!');
}

var workflow = module.exports = {
    name: 'start-' + VERSION,
    version: VERSION,
    chain: [ {
        name: 'common.validate_params',
        timeout: 10,
        retry: 1,
        body: common.validateForZoneAction,
        modules: {}
    }, {
        name: 'cnapi.acquire_vm_ticket',
        timeout: 10,
        retry: 1,
        body: common.acquireVMTicket,
        modules: { sdcClients: 'sdc-clients' }
    }, {
        name: 'cnapi.wait_on_vm_ticket',
        timeout: 120,
        retry: 1,
        body: common.waitOnVMTicket,
        modules: { sdcClients: 'sdc-clients' }
    }, {
        name: 'common.ensure_vm_state',
        timeout: 10,
        retry: 1,
        body: common.ensureVmState,
        modules: { sdcClients: 'sdc-clients' }
    }
    ].concat(nfsVolumes.provisionChain).concat([
    {
        name: 'setup_start_request',
        timeout: 10,
        retry: 1,
        body: setupStartRequest,
        modules: {}
    }, {
        name: 'cnapi.start_vm',
        timeout: 10,
        retry: 1,
        body: common.zoneAction,
        modules: { restify: 'restify' }
    }, {
        name: 'cnapi.wait_task',
        timeout: 120,
        retry: 1,
        body: common.waitTask,
        modules: { sdcClients: 'sdc-clients' }
    }, {
        name: 'vmapi.put_vm',
        timeout: 60,
        retry: 1,
        body: common.putVm,
        modules: { sdcClients: 'sdc-clients' }
    }, {
        name: 'cnapi.release_vm_ticket',
        timeout: 60,
        retry: 1,
        body: common.releaseVMTicket,
        modules: { sdcClients: 'sdc-clients' }
    } ]),
    timeout: 410,
    onerror: [ {
        name: 'On error',
        modules: { sdcClients: 'sdc-clients' },
        body: function (job, cb) {
            var cnapi = new sdcClients.CNAPI({
                url: cnapiUrl,
                headers: { 'x-request-id': job.params['x-request-id'] }
            });
            cnapi.waitlistTicketRelease(job.ticket.uuid, function (err) {
                cb('Error executing job');
                return;
            });

        }
    } ],
    oncancel: [ {
        name: 'On cancel',
        modules: { sdcClients: 'sdc-clients' },
        body: function (job, cb) {
            var cnapi = new sdcClients.CNAPI({
                url: cnapiUrl,
                headers: { 'x-request-id': job.params['x-request-id'] }
            });
            cnapi.waitlistTicketRelease(job.ticket.uuid, function (err) {
                cb();
                return;
            });
        }
    } ]
};
