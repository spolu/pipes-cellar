var fwk = require('pipes');

var config = fwk.baseConfig();

config['PIPES_BOOTSTRAP_SERVER'] = '127.0.0.1';
config['PIPES_BOOTSTRAP_PORT'] = 1984;

config['PIPES_CONFIG_REG'] = 'config';
config['PIPES_CONFIG_TAG'] = 'undefined';

config['TINT_NAME'] = 'cellar';

config['CELLAR_DBNAME'] = 'cellar';

/** export merged configuration */
exports.config = config;
