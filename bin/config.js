var fwk = require('fwk');

var config = fwk.baseConfig();

config['PIPE_BOOTSTRAP_SERVER'] = '127.0.0.1';
config['PIPE_BOOTSTRAP_PORT'] = 1984;

config['PIPE_CONFIG_REG'] = 'config';
config['PIPE_CONFIG_TAG'] = 'undefined';

config['MONGO_DB'] = 'cellar';
config['MONGO_HOST'] = 'localhost';
config['MONGO_PORT'] = 27017;
config['MONGO_WRITEBACK_INTERVAL'] = 2000;

config['TINT_NAME'] = 'cellar';

/** export merged configuration */
exports.config = config;
