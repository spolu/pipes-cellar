var util = require('util');
var fwk = require('fwk');
var cellar = require('cellar');

var cfg = require("./config.js");
var mongo = require("./mongo.js");

/**
 * An updater Object
 * 
 * Applies the update using the function it is constructed with
 * 
 * @param spec {subject, updater}
 */
var updater = function(spec, my) {
  my = my || {};
  var _super = {};   
  
  my.subject = spec.subject;

  if(spec.updater && typeof spec.updater === 'function') {
    my.updater = function(spec, cb_) {
      try {
	return spec.updater(spec, cb_);
      } catch (err) { 
	ctx.log.error(err, true);
	return null;
      }
    };
    my.updaterdata = spec.updater.toString();
  }
  else
    my.updater = function(spec, cout_) {
      cont_();
    };
  
  var update, describe;
  
  update = function(spec, cb_) {
    my.updater(spec, cb_);
  };
  
  describe = function() {
    var data = { subject: my.subject,
		 updater: my.updaterdata };
    return data;
  };
  
  that.method('update', update);
  that.method('describe', describe);
  
  that.getter('subject', my, 'subject');  

  return that;
};


/**
 * The Mutator Object
 * 
 * Carries on MUT request and store the updaters sent over the network
 * with UPD actions
 *
 * @extends {}
 *  
 * @param spec {mongo, config}
 */
var mutator = function(spec, my) {
  my = my || {};
  var _super = {};
  
  my.cfg = spec.config || cfg.config;
  my.mongo = spec.mongo;

  my.updaters = {};
  
  var that = {};
  
  var register, unregister, mutator, list;
  
  register = function(ctx, subject, updfun) {
    unregister(subject);
    my.updaters[subject] = updater({ subject: subject,
				     updater: updfun });
  };
  
  unregister = function(ctx, subject) {
    if(my.updaters.hasOwnProperty(subject)) {
      delete my.updaters[subject];
      ctx.log.out('unregister: ' + subject);
    }
  };

  /** cb_(res) */
  mutator = function(pipe, action, cb_) {
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
    if(!my.updaters[action.subject()]) {
      action.error(new Error('No updater for subject: ' + action.subject()));
      return;            
    }
        
    var loopfun = function(target) {
      my.mongo.get(
	action, target,
	function(object) {
	  var hash = object._hash;
	  my.updaters[action.subject()].update(
	    { pipe: pipe,
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
    
    loopfun(action.targets()[0]);    
  };
  
  list = function(subject) {
    var data = {};
    for(var i in my.updaters) {
      if(my.updaters.hasOwnProperty(i) && (!id || id === i)) {
	data[i] = my.updaters[i].describe();
      }	
    }   
    return data;
  };

  that.method('register', register);
  that.method('unregister', unregister);
  that.method('mutator', mutator);
  that.method('list', list);

  return that;
};

exports.mutator = mutator;