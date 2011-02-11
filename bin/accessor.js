var util = require('util');
var fwk = require('fwk');
var cellar = require('cellar');
var mongo = require('mongo');

var cfg = require("./config.js");

/**
 * A Getter object
 * 
 * Fetches the data accordingly to the function it was constructed with
 *
 * @param spec {ctx, subject, getfun}
 */
var getter = function(spec, my) {
  my = my || {};
  var _super = {};   
  
  my.subject = spec.subject;
  my.ctx = spec.ctx;  

  if(spec.getfun && typeof spec.getfun === 'function') {
    my.getter = function(sp, cb_) {
      try {
	return spec.getfun(sp, cb_);
      } catch (err) { 
	my.ctx.log.error(err, true);
	return null;
      }
    };
    my.getterdata = spec.getfun.toString();
  }
  else
    my.getter = function(spec, cont_) {
      cont_();
    };
  
  var that = {};

  var get, describe;
  
  get = function(spec, cb_) {
    my.getter(spec, cb_);
  };
  
  describe = function() {
    var data = { subject: my.subject,
		 getter: my.getterdata };
    return data;
  };
  
  that.method('get', get);
  that.method('describe', describe);
  
  that.getter('subject', my, 'subject');  

  return that;
};


/**
 * The Accessor Object
 * 
 * Carries on ACC request and store the getters registered over the network
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
  
  var register, unregister, accessor, list;
  
  register = function(ctx, subject, getfun) {
    unregister(subject);
    my.getters[subject] = getter({ ctx: ctx,
				   subject: subject, 
				   getfun: getfun});
    ctx.log.out('register: ' + subject);
  };
  
  unregister = function(ctx, subject) {
    if(my.getters.hasOwnProperty(subject)) {
      delete my.getters[subject];
      ctx.log.out('unregister: ' + subject);
    }
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
	action.log.debug('CBGET: ' + object._cid);
	my.getters[action.subject()].get(
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
  
  list = function(subject) {
    var data = {};
    for(var i in my.getters) {
      if(my.getters.hasOwnProperty(i) && (!id || id === i)) {
	data[i] = my.getters[i].describe();
      }	
    }   
    return data;
  };

  that.method('register', register);
  that.method('unregister', unregister);
  that.method('accessor', accessor);
  that.method('list', list);

  return that;
};

exports.accessor = accessor;