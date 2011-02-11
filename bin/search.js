var util = require('util');
var fwk = require('fwk');
var cellar = require('cellar');
var mongo = require('mongo');

var cfg = require("./config.js");

/**
 * The Search Object
 * 
 * Carries on SRH requests
 * 
 * @extends {}
 * 
 * @param spec {mongo, config}
 */ 
var search = function(spec, my) {
  my = my || {};
  var _super = {};
  
  my.cfg = spec.config || cfg.config;
  my.mongo = spec.mongo;
  
  var that = {};
  
  var search;
  
  /** cb_(res) */  
  search = function(pipe, action, cb_) {
    if(action.targets().length === 0) {
      action.error(new Error('No target defined'));
      return;      
    }
    if(action.targets().length !== 1) {
      var msg = 'Multiple targets defined:';
      for(var i = 0; i < action.targets().length; i ++) {
	msg += ' ' + action.targets()[i];
      }
      action.error(new Error(msg));
      return;      
    }

    my.mongo.find(
      action, 
      action.targets()[0],
      action.body(),
      function(result) {
	cb_(result);
      });
  };
  
  that.method('search', search);
  
  return that;
};

exports.search = search;
