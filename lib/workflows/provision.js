/*
 * Copyright (c) 2012, Joyent, Inc. All rights reserved.
 *
 * A brief overview of this source file: what is its purpose.
 */


// This is not really needed, but javascriptlint will complain otherwise:
var sdcClients = require('sdc-clients');
var uuid = require('node-uuid');
var restify = require('restify');
var common = require('./job-common');


function validateParams(job, cb) {
    if (!job.params.dapiUrl)
        return cb('No DAPI URL provided');

    if (!job.params.napiUrl || !job.params.napiUsername ||
        !job.params.napiPassword)
        return cb('No NAPI parameters provided');

    if (!job.params.cnapiUrl)
        return cb('No CNAPI URL provided');

    if (!job.params.owner_uuid)
        return cb('\'owner_uuid\' is required');

    if (!job.params.image_uuid)
        return cb('\'image_uuid\' is required');

    if (!job.params.brand)
        return cb('VM \'brand\' is required');

    if (!job.params.ram)
        return cb('VM \'ram\' is required');

    if (job.params.muuid) {
        job.params.uuid = job.params.muuid;
        delete job.params.muuid;
    }

    return cb(null, 'All parameters OK!');
}



// function getNetworks(job, cb) {
//     var uuids = job.params.networks;
//
//     if (!uuids)
//         return cb('Networks are required');
//
//     var napi = restify.createJsonClient({ url: job.params.napiUrl });
//     napi.basicAuth(job.params.napiUsername, job.params.napiPassword);
//
//     job.nic_tags = [];
//     var added = 0;
//
//     for (var i = 0; i < uuids.length; i++) {
//         napi.get('/networks/' + uuids[i], function (err, req, res, network) {
//               if (err) {
//                   return cb(err);
//               } else {
//                   // Might be the case that we want 2 nics on the same network
//                   if (job.nic_tags.indexOf(network.name) == -1)
//                       job.nic_tags.push(network.name);
//
//                   added++;
//
//                   if (added == uuids.length)
//                     return cb(null, 'NIC Tags retrieved!');
//               }
//         });
//     }
// }



// function getServerNics(job, cb) {
//     var nic_tags = job.nic_tags;
//
//     if (!nic_tags)
//         return cb('NIC Tags are required');
//
//     var napi = restify.createJsonClient({ url: job.params.napiUrl });
//     napi.basicAuth(job.params.napiUsername, job.params.napiPassword);
//
//     job.server_uuids = [];
//     var added = 0;
//
//     for (var i = 0; i < nic_tags.length; i++) {
//         var query = {
//             path: '/nics',
//             query: { belongs_to_type: 'server', nic_tag: nic_tags[i] }
//         };
//         napi.get(query, function (err, req, res, nics) {
//             if (err) {
//                 return cb(err);
//             } else {
//                 for (var j = 0; j < nics.length) {
//                     var nic = nics[j];
//
//                     // Might be the case that we want 2 nics on the same network
//                     if (job.server_uuids.indexOf(nic.belongs_to_uuid) == -1)
//                       job.server_uuids.push(nic.belongs_to_uuid);
//                 }
//
//                 added++;
//
//                 if (added == nic_tags.length)
//                 return cb(null, 'Server UUIDs retrieved!');
//             }
//         });
//     }
// }



function getServers(job, cb) {
    if (job.params.server_uuid)
        return cb(null, 'Server UUID present, no need to get servers from CNAPI');

    var cnapi = restify.createJsonClient({ url: job.params.cnapiUrl });

    return cnapi.get('/servers', function (err, req, res, servers) {
        if (err) {
            return cb(err);
        } else {
            if (Array.isArray(servers) && servers.length) {
                job.servers = servers;
                return cb(null, 'Got servers!');
            } else {
                return cb(new Error('No servers found on CNAPI'));
            }
        }
    });
}



function getAllocation(job, cb) {
    if (job.params.server_uuid)
        return cb(null, 'Server UUID present, no need to get allocation from DAPI');

    var dapi = restify.createJsonClient({ url: job.params.dapiUrl });

    var serversJSON = { servers: JSON.stringify(job.servers) };

    return dapi.post('/allocation', serversJSON,
      function (err, req, res, server) {
          if (err) {
              return cb(err);
          } else {
              job.params.server_uuid = server.uuid;
              return cb(null, 'Server allocated!');
          }
    });
}



function getNICs(job, cb) {
    var networks = job.params.networks;
    var nics = job.params.nics;

    if (!networks && !nics)
        return cb('Networks or NICs are required');

    var napi = restify.createJsonClient({ url: job.params.napiUrl });
    napi.basicAuth(job.params.napiUsername, job.params.napiPassword);

    job.params.nics = [];
    var params = {
        owner_uuid: job.params.owner_uuid,
        belongs_to_uuid: job.params.uuid,
        belongs_to_type: 'zone'
    };

    var length;
    if (networks)
        length = networks.length;
    else
        length = nics.length;

    for (var i = 0; i < length; i++) {
        var path, theNic;

        if (networks) {
            path = '/networks/' + networks[i] + '/nics';
            theNic = params;
        } else {
            path = '/nics';
            theNic = nics[i];
            theNic.owner_uuid = job.params.owner_uuid;
            theNic.belongs_to_uuid = job.params.uuid;
            theNic.belongs_to_type = 'zone';
        }

        napi.post(path, theNic, function (err, req, res, nic) {
            if (err) {
                return cb(err);
            } else {
                job.params.nics.push(nic);

                if (job.params.nics.length == length)
                    return cb(null, 'NICs allocated!');
            }
        });
    }
}



function provision(job, cb) {
    var cnapi = restify.createJsonClient({ url: job.params.cnapiUrl });
    var endpoint = '/servers/' + job.params.server_uuid + '/vms';
    job.params.jobid = job.uuid;

    return cnapi.post(endpoint, job.params, function (err, req, res, task) {
      if (err) {
          return cb(err);
      } else {
          job.taskId = task.id;
          return cb(null, 'Provision queued!');
      }
    });
}



var workflow = module.exports = {
    name: 'provision-' + uuid(),
    chain: [ {
        name: 'Validate parameters',
        timeout: 30,
        retry: 1,
        body: validateParams
    }, {
        name: 'Get servers',
        timeout: 30,
        retry: 1,
        body: getServers
    }, {
        name: 'Get allocation',
        timeout: 30,
        retry: 1,
        body: getAllocation
    }, {
        name: 'Get NICs',
        timeout: 30,
        retry: 1,
        body: getNICs
    }, {
        name: 'Provision',
        timeout: 30,
        retry: 1,
        body: provision
    }, {
        name: 'Poll',
        timeout: 240,
        retry: 1,
        body: common.pollTask
    }],
    timeout: 240,
    onerror: [ {
        name: 'On error',
        body: function (job, cb) {
            return cb('Error executing job');
        }
    }]
};