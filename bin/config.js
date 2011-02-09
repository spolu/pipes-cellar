var fwk = require('fwk');

var config = fwk.baseConfig();

config['PIPE_BOOTSTRAP_SERVER'] = '127.0.0.1';
config['PIPE_BOOTSTRAP_PORT'] = 1984;

config['PIPE_CONFIG_REG'] = 'config';
config['PIPE_CONFIG_TAG'] = 'undefined';

config['TINT_NAME'] = 'cellar';

/** export merged configuration */
exports.config = config;
