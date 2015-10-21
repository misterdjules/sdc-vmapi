var child_process = require('child_process');
var path = require('path');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var vasync = require('vasync');

var configLoader = require('../../lib/config-loader');

var config = configLoader.loadConfig();
assert.object(config, 'config');

var log = new bunyan({
    name: 'add-all-indices',
    level: config.logLevel || 'info',
    serializers: bunyan.stdSerializers
});

log.debug({config: config}, 'config');

var addIndexMigrations = [
    './add-index/add-docker-index.js',
    './add-index/add-transitive-state-index.js'
];

function runAddIndexMigration(scriptFilePath, callback) {
    assert.string(scriptFilePath, 'scriptFilePath');
    assert.func(callback, 'callback');

    var execArgs = [
        process.argv[0],
        path.resolve(__dirname, scriptFilePath)
    ];

    child_process.exec(execArgs.join(' '), function onIndexMigrationDone(err, stdout, stderr) {
        log.debug({
            stdout: stdout,
            stderr: stderr
        }, 'output from ' + scriptFilePath + ' migration');

        return callback(err);
    });
}

vasync.forEachPipeline({
    func: runAddIndexMigration,
    inputs: addIndexMigrations
}, function onAllMigrationsRan(err, results) {
    if (err) {
        log.error({err: err}, 'Error when running indices migrations');
        process.exit(1);
    } else {
        log.info('All indices migrations ran successfully');
    }
});
