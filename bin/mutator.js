var util = require('util');
var fwk = require('fwk');
var cellar = require('cellar');
var mongo = require('mongo');

var cfg = require("./config.js");

/**
 * A mut Object
 * 
 * Applies the update using the function it is constructed with
 * 
 * @param spec {ctx, subject, mutfun}
 */
var mut = function(spec, my) {
  my = my || {};
  var _super = {};   
  
  my.ctx = spec.ctx;
  my.subject = spec.subject;
  
  if(spec.mutfun && typeof spec.mutfun === 'function') {
    my.mut = function(sp, cb_) {
      try {
	return spec.mutfun(sp, cb_);
      } catch (err) { 
	my.ctx.log.error(err, true);
	return null;
      }
    };
    my.mutdata = spec.mutfun.toString();
  }
  else
    my.mut = function(spec, cout_) {
      cont_();
    };
  
  var that = {};

  var update, describe;
  
  update = function(spec, cb_) {
    my.mut(spec, cb_);
  };
  
  describe = function() {
    var data = { subject: my.subject,
		 mut: my.mutdata };
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
 * with mut actions
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

  my.mutators = {};
  
  var that = {};
  
  var register, unregister, mutator, list;
  
  register = function(ctx, subject, mutfun) {
    unregister(subject);
    my.mutators[subject] = mut({ ctx: ctx,
				 subject: subject,
				 mutfun: mutfun });
    ctx.log.out('register: ' + subject);
  };
  
  unregister = function(ctx, subject) {
    if(my.mutators.hasOwnProperty(subject)) {
      delete my.mutators[subject];
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
    if(!my.mutators[action.subject()]) {
      action.error(new Error('No mutator for subject: ' + action.subject()));
      return;            
    }
        
    var loopfun = function(target) {
      my.mongo.get(
	action, target,
	function(object) {
	  action.log.debug('CBGET: ' + object._cid);
	  var hash = object._hash;
	  my.mutators[action.subject()].update(
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
		    else {
		      action.log.debug('CBSET: ' + object._cid);
		      cb_(update.result, update.headers);
		    }
		  });
	      }
	      else {
		action.error(new Error('action unsupported by mutator:' + target));
		return;
	      }
	    });
	});
    };
    
    loopfun(action.targets()[0]);    
  };
  
  list = function(subject) {
    var data = {};
    for(var i in my.mutators) {
      if(my.mutators.hasOwnProperty(i) && (!id || id === i)) {
	data[i] = my.mutators[i].describe();
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