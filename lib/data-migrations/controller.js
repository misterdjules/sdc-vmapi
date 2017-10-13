/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var backoff = require('backoff');
var EventEmitter = require('events');
var util = require('util');
var vasync = require('vasync');
var VError = require('verror');

var dataMigrations = require('./migrations');

function DataMigrationsController(migrations, options) {
    assert.arrayOfObject(migrations, 'migrations');
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.moray, 'options.moray');

    EventEmitter.call(this);

    this.latestCompletedMigration = undefined;
    this.log = options.log;
    this.migrations = migrations;
    this.moray = options.moray;

    _validateMigrations(migrations);
}
util.inherits(DataMigrationsController, EventEmitter);

/*
 * Validates that all data migrations that need to be performed are valid. For
 * instance, that their DATA_VERSION numbers are a proper sequence starting at
 * 1, and that they export a function named "migrateRecord".
 */
function _validateMigrations(migrations) {
    assert.arrayOfObject(migrations, 'migrations');

    var idxMigration;
    var expectedDataVersion = 1;

    for (idxMigration = 0; idxMigration < migrations.length; ++idxMigration) {
        assert.equal(migrations[idxMigration].DATA_VERSION, expectedDataVersion,
            'Data version of migration ' + (idxMigration + 1) + ' should be ' +
                expectedDataVersion);
        assert.func(migrations[idxMigration].migrateRecord,
                'MIGRATIONS[' + idxMigration + '].migrateRecord');
        ++expectedDataVersion;
    }
}

function dataMigrationErrorTransient(error) {
    assert.object(error, 'error');

    var idx;
    var nonTransientErrors = [
        /*
         * For now, we consider a bucket not found to be a non-transient error
         * because it's not clear how that error would resolve itself by
         * retrying the data migrations process.
         */
        'BucketNotFoundError',
        'InvalidIndexTypeError',
        'InvalidQueryError',
        'NoDatabasePeersError',
        /*
         * We consider NotIndexedError errors to be non-transient because data
         * migrations happen *after any schema migration, including reindexing
         * of all affected buckets* is considered to be complete. As a result,
         * when data migrations start, the indexes that are present will not
         * change, and so retrying on such an error would lead to the same error
         * occurring.
         */
        'NotIndexedError',
        /*
         * Unless a specific data migration handles a UniqueAttributeError
         * itself, we consider that retrying that migration would have the same
         * result, so we treat it as a non-transient error.
         */
        'UniqueAttributeError'
    ];

    for (idx = 0; idx < nonTransientErrors.length; ++idx) {
        if (VError.hasCauseWithName(error, nonTransientErrors[idx])) {
            return false;
        }
    }

    return true;
}

DataMigrationsController.prototype.start = function start() {
    var dataMigrationsBackoff = backoff.exponential();
    var self = this;

    dataMigrationsBackoff.on('backoff',
        function onDataMigrationBackoff(number, delay) {
            self.log.info('Data migration backed off, will retry in ' + delay +
                ' ms');
        });

    dataMigrationsBackoff.on('ready', function onMigrationReady(number, delay) {
        self._runMigrations(function migrationsRan(dataMigrationErr) {
            if (dataMigrationErr) {
                self.log.error({err: dataMigrationErr},
                    'Error when running data migrations');

                if (dataMigrationErrorTransient(dataMigrationErr)) {
                    self.log.info('Error is transient, backing off');
                    dataMigrationsBackoff.backoff();
                } else {
                    self.log.error('Error is not transient, emitting ' +
                        'error');
                    self.emit('error', dataMigrationErr);
                }
            } else {
                self.log.info('All data migrations ran successfully');
                dataMigrationsBackoff.reset();
                self.emit('done');
            }
        });
    });

    dataMigrationsBackoff.backoff();
};

DataMigrationsController.prototype._runMigrations =
function _runMigrations(callback) {
    assert.object(this.log, 'this.log');
    assert.object(this.moray, 'this.moray');
    assert.func(callback, 'callback');

    var log = this.log;
    var moray = this.moray;
    var self = this;

    log.info('Starting data migrations');
    self.latestCompletedMigration = undefined;

    vasync.forEachPipeline({
        func: function runSingleMigration(migration, next) {
            var migrateRecordFunc = migration.migrateRecord;

            assert.func(migrateRecordFunc, 'migrateRecordFunc');
            assert.number(migration.DATA_VERSION, 'migration.DATA_VERSION');
            assert.ok(migration.DATA_VERSION >= 1,
                    'migration.DATA_VERSION >= 1');

            log.info('Running migration to data version: ' +
                migration.DATA_VERSION);

            _runSingleMigration(migration.DATA_VERSION, migrateRecordFunc, {
                log: log,
                moray: moray
            }, function onMigration(migrationErr) {
                if (migrationErr) {
                    log.error({err: migrationErr},
                        'Error when running migration to data version: ' +
                            migration.DATA_VERSION);
                } else {
                    self.latestCompletedMigration = migration.DATA_VERSION;
                    log.info('Data migration to data version: ' +
                        migration.DATA_VERSION + ' ran successfully');
                }

                next(migrationErr);
            });
        },
        inputs: self.migrations
    }, function onAllMigrationsDone(migrationsErr, results) {
        var err;

        if (migrationsErr) {
            err = new VError(migrationsErr, 'Failed to run data migrations');
        }

        callback(err);
    });
};

function _runSingleMigration(version, migrateRecordFunc, options, callback) {
    assert.number(version, 'version');
    assert.ok(version >= 1, 'version >= 1');
    assert.func(migrateRecordFunc, 'migrateRecordFunc');
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.object(options.moray, 'options.moray');
    assert.func(callback, 'callback');

    var context = {};
    var log = options.log;
    var moray = options.moray;

    function processNextChunk() {
        vasync.pipeline({arg: context, funcs: [
            function findRecordsToMigrate(ctx, next) {
                moray.findVmRecordsToMigrate(version, {
                    log: log
                }, function onFindRecords(findErr, records) {
                    if (findErr) {
                        log.error({err: findErr},
                            'Error when finding records not at version: ' +
                                version);
                    } else {
                        log.info('Found ' + records.length + ' records');
                        ctx.records = records;
                    }

                    next(findErr);
                });
            },
            function migrateRecords(ctx, next) {
                var migratedRecords;
                var records = ctx.records;

                assert.arrayOfObject(records, 'records');

                if (records.length === 0) {
                    next();
                    return;
                }

                migratedRecords = records.map(migrateRecordFunc);
                log.info({migratedRecords: migratedRecords},
                        'Migrated records');

                moray.putVmsBatch(migratedRecords, next);
            }
        ]}, function onChunkProcessed(chunkProcessingErr) {
            var records = context.records;

            if (chunkProcessingErr) {
                log.error({err: chunkProcessingErr},
                        'Error when processing chunk');
                callback(chunkProcessingErr);
                return;
            }

            if (!records || records.length === 0) {
                log.info('No more records at version: ' + version +
                    ', migration done');
                callback();
            } else {
                log.info('Processed ' + records.length + ' records, ' +
                    'scheduling processing of next chunk');
                setImmediate(processNextChunk);
            }
        });
    }

    processNextChunk();
}

module.exports = DataMigrationsController;