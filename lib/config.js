var fwk = require('pipes');

var config = fwk.baseConfig();

config['MONGO_HOST'] = 'localhost';
config['MONGO_PORT'] = 27017;
config['MONGO_CONSISTENCY_TIMEOUT'] = 10000;

/** export merged configuration */
exports.config = config;
