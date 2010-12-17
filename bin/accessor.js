var util = require('util');
var fwk = require('fwk');
var cellar = require('cellar');

var cfg = require("./config.js");
var mongo = require("./mongo.js");

/**
 * The Accessor Object
 * 
 * Carries on ACC request and store the updaters sent over the network
 * with UPD actions
 *
 * @extends {}
 *  
 * @param spec {pipe, mongo, config}
 */
var accessor = function(spec, my) {
  my = my || {};
  var _super = {};
  
  my.cfg = spec.config || cfg.config;
  my.pipe = spec.pipe;  
  my.mongo = spec.mongo;

  my.getters = {};

  var that = {};
  
  var getter, accessor;
  
  getter = function(action) {
    if(!action.body() ||
       !action.subject() ||
       action.subject().length == 0) {
      action.error(new Error('Missing body or subject'));
      return;           
    }
    
    action.log.debug('getter evaluating: ' + action.body());
    eval("var getterfun = " + action.body());
    
    if(typeof getterfun === 'function') {
      my.getters[action.subject()] = function(spec, cb_) {
	try {
	  return getterfun(spec, cb_);
	} catch (err) { 
	  action.log.error(err, true);
	  return null;
	}
      };
    } 
    else
      action.error(new Error('Updater is not a function'));    
  };

  /** cb_(res) */  
  accessor = function(action, cb_) {
    if(action.targets().length === 0) {
      action.error(new Error('No target defined'));
      return;      
    }
    if(action.targets().length !== 1) {
      action.error(new Error('Multiple target'));
      return;      
    }
    if(!my.getters[action.subject()]) {
      action.error(new Error('No getter for subject: ' + action.subject()));
      return;            
    }
    
    my.mongo.get(
      action, action.targets()[0],
      function(object) {
	my.getters[action.subject()](
	  { pipe: my.pipe,
	    action: action,
	    target: action.targets()[0],
	    object: object },
	  /** @param result */ 
	  function(result) {
	    if(result)
	      cb_(result);
	    else {
	      action.error(new Error('action unsupported by getter:' + target));
	      return;
	    }
	  });
      });    
  };

  that.method('getter', getter);
  that.method('accessor', accessor);

  return that;
};

exports.accessor = accessor;