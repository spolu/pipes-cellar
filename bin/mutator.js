var util = require('util');
var fwk = require('fwk');
var cellar = require('cellar');

var cfg = require("./config.js");
var mongo = require("./mongo.js");

/**
 * The Mutator Object
 * 
 * Carries on MUT request and store the updaters sent over the network
 * with UPD actions
 *
 * @extends {}
 *  
 * @param spec {pipe, config}
 */
var mutator = function(spec, my) {
  my = my || {};
  var _super = {};
  
  my.cfg = spec.config || cfg.config;
  my.pipe = spec.pipe;  
  my.mongp = spec.mongo || mongo.mongo({ config: my.cfg });

  my.updaters = {};
  
  var that = {};
  
  var updater, mutator;
  
  updater = function(action) {
    if(action.body() && 
       action.subject() && 
       action.subject().length > 0) {
      
      action.log.debug('updater evaluating: ' + action.body());
      eval("var updaterfun = " + action.body());

      if(typeof updaterfun === 'function') {
	my.updaters[action.subject()] = function(spec) {
	  try {
	    return updaterfun(spec);
	  } catch (err) { 
	    action.log.error(err, true);
	    return {};
	  }
	};
      }            
    }    
  };
  
  /** cb_(res) */
  mutator = function(action, cb_) {
    if(action.targets().length !== 1) {
      action.error(new Error('Multiple target update'));
      return;      
    }
    if(!my.updaters[action.subject()]) {
      action.error(new Error('No updater for subject: ' + action.subject()));
      return;            
    }
        
    var loopfun = function(target) {
      my.mongo.get(
	action, target,
	function(object) {
	  var hash = object._hash;
	  my.updaters[action.subject()](
	    { pipe: my.pipe,
	      action: action,
	      target: target,
	      object: object },
	    /** @param update {object, result} */ 
	    function(update) {
	      if(update) {
		my.mongo.set(
		  action, target, hash, update.object,
		  function(status) {
		    if(status === 'RETRY') loopfun(target);
		    else cb_(update.result);
		  });
	      }
	      else {
		action.error(new Error('action unsupported by updater:' + target));
		return;
	      }
	    });
	});
    };
    
    looopfun(action.targets()[0]);    
  };

  return that;
};