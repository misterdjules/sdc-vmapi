/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var assert = require('assert-plus');
var fs = require('fs');
var path = require('path');
var vasync = require('vasync');

var DataMigrationsController = require('./controller');

var DEFAULT_MIGRATIONS_DIR = path.join(__dirname, 'migrations');

function loadMigrations(migrationsDirPath, callback) {
    var context = {};

    if (typeof (migrationsDirPath) === 'function') {
        callback = migrationsDirPath;
        migrationsDirPath = undefined;
    }

    assert.optionalString(migrationsDirPath, 'migrationsDirPath');
    assert.func(callback, 'callback');

    if (migrationsDirPath === undefined) {
        migrationsDirPath = DEFAULT_MIGRATIONS_DIR;
    }

    vasync.pipeline({arg: context, funcs: [
        function readMigrationsDir(ctx, next) {
            fs.readdir(migrationsDirPath,
                function onDirRead(dirReadErr, migrationFiles) {
                    ctx.migrationFiles = migrationFiles;
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

module.exports = {
    DataMigrationsController: DataMigrationsController,
    loadMigrations: loadMigrations
};