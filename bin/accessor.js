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
 * @param spec {mongo, config}
 */
var accessor = function(spec, my) {
  my = my || {};
  var _super = {};
  
  my.cfg = spec.config || cfg.config;
  my.mongo = spec.mongo;

  my.getters = {};

  var that = {};
  
  var getter, accessor;
  
  getter = function(ctx, subject, fun) {
    if(!subject || subject.length == 0 || !fun) {
      ctx.error(new Error('Missing subject or fun'));
      return;           
    }
    
    ctx.log.debug('getter evaluating: ' + fun);
    eval("var getterfun = " + fun);
    
    if(typeof getterfun === 'function') {
      my.getters[subject] = function(spec, cb_) {
	try {
	  return getterfun(spec, cb_);
	} catch (err) { 
	  ctx.log.error(err, true);
	  return null;
	}
      };
    } 
    else
      ctx.error(new Error('Updater is not a function'));    
  };

  /** cb_(res) */  
  accessor = function(pipe, action, cb_) {
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
    if(!my.getters[action.subject()]) {
      action.error(new Error('No getter for subject: ' + action.subject()));
      return;            
    }
    
    my.mongo.get(
      action, action.targets()[0],
      function(object) {
	my.getters[action.subject()](
	  { pipe: pipe,
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