var fwk = require('fwk');

var config = fwk.baseConfig();

config['PIPE_REGISTRATION'] = 'all';
config['PIPE_TAG'] = 'cellar';
config['MONGO_DB'] = 'cellar';
config['MONGO_HOST'] = 'localhost';
config['MONGO_PORT'] = 27017;
config['MONGO_WRITEBACK_INTERVAL'] = 2000;


/** export merged configuration */
exports.config = config;
