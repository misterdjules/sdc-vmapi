/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var EventEmitter = require('events');
var fs = require('fs');
var path = require('path');
var vasync = require('vasync');
var util = require('util');

var DataMigrationsController = require('./controller');

var DEFAULT_MIGRATIONS_DIR = path.join(__dirname, 'migrations');
var MIGRATION_FILE_RE = /^\d{3}.*\.js$/;

function loadMigrations(migrationsDirPath, options, callback) {
    var context = {};
    var log;

    if (typeof (migrationsDirPath) === 'object') {
        callback = options;
        options = migrationsDirPath;
        migrationsDirPath = undefined;
    }

    assert.optionalString(migrationsDirPath, 'migrationsDirPath');
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.func(callback, 'callback');

    log = options.log;

    assert.optionalString(migrationsDirPath, 'migrationsDirPath');
    assert.func(callback, 'callback');

    if (migrationsDirPath === undefined) {
        migrationsDirPath = DEFAULT_MIGRATIONS_DIR;
    }

    vasync.pipeline({arg: context, funcs: [
        function readMigrationsDir(ctx, next) {
            fs.readdir(migrationsDirPath,
                function onDirRead(dirReadErr, migrationFiles) {
                    var sortedAndFilteredMigrationFiles;

                    if (!dirReadErr) {
                        sortedAndFilteredMigrationFiles =
                            migrationFiles.filter(function migration(fileName) {
                                return MIGRATION_FILE_RE.test(fileName);
                            }).sort();

                    }

                    if (sortedAndFilteredMigrationFiles.length !==
                        migrationFiles.length) {
                        log.warn({migrationFiles: migrationFiles},
                            'Found migration files that do not match the ' +
                                'migration filename accepted format');
                    }

                    ctx.migrationFiles = sortedAndFilteredMigrationFiles;

                    next(dirReadErr);
                });
        },
        function loadMigrationModules(ctx, next) {
            assert.arrayOfString(ctx.migrationFiles, 'ctx.migrationFiles');

            ctx.migrations =
                ctx.migrationFiles.map(function loadModule(migrationFile) {
                    return require(path.join(migrationsDirPath, migrationFile));
                });

            next();
        }
    ]}, function onMigrationsLoaded(err, results) {
        callback(err, context.migrations);
    });
}

function NoopDataMigrationsController() {
    EventEmitter.call(this);
}
util.inherits(NoopDataMigrationsController, EventEmitter);

NoopDataMigrationsController.prototype.start = function start() {
    this.emit('done');
};

function createNoopDataMigrationsController() {
    return new NoopDataMigrationsController();
}

module.exports = {
    DataMigrationsController: DataMigrationsController,
    loadMigrations: loadMigrations,
    createNoopDataMigrationsController: createNoopDataMigrationsController
};