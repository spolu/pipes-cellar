var fwk = require('fwk');

var config = fwk.baseConfig();

config['PIPE_REGISTRATION'] = 'all';
config['PIPE_TAG'] = 'cellar';

/** export merged configuration */
exports.config = config;
