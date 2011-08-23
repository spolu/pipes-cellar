// Copyright Stanislas Polu
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

var util = require('util');
var fwk = require('pipes');
var cellar = require('cellar');

var cfg = require("./config.js");

/**
 * A Acc object
 * 
 * Fetches the data accordingly to the function it was constructed with
 *
 * @param spec {ctx, subject, accfun}
 */
var acc = function(spec, my) {
  my = my || {};
  var _super = {};   
  
  my.subject = spec.subject;
  my.ctx = spec.ctx;  

  if(spec.accfun && typeof spec.accfun === 'function') {
    my.acc = function(sp, cb_) {
      try {
	return spec.accfun(sp, cb_);
      } catch (err) { 
	my.ctx.log.error(err, true);
	return null;
      }
    };
    my.accdata = spec.accfun.toString();
  }
  else
    my.acc = function(spec, cont_) {
      cont_();
    };
  
  var that = {};

  var get, describe;
  
  get = function(spec, cb_) {
    my.acc(spec, cb_);
  };
  
  describe = function() {
    var data = { subject: my.subject,
		 acc: my.accdata };
    return data;
  };
  
  fwk.method(that, 'get', get);
  fwk.method(that, 'describe', describe);
  
  fwk.getter(that, 'subject', my, 'subject');  

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

  my.accessors = {};

  var that = {};
  
  var register, unregister, accessor, list;
  
  register = function(ctx, subject, accfun) {
    unregister(subject);
    my.accessors[subject] = acc({ ctx: ctx,
				  subject: subject, 
				  accfun: accfun});
    ctx.log.out('register: ' + subject);
  };
  
  unregister = function(ctx, subject) {
    if(my.accessors.hasOwnProperty(subject)) {
      delete my.accessors[subject];
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
    if(!my.accessors[action.subject()]) {
      action.error(new Error('No accessor for subject: ' + action.subject()));
      return;            
    }
    
    my.mongo.get(
      action, action.targets()[0],
      function(object) {
	action.log.debug('CBGET: ' + object._cid);
	my.accessors[action.subject()].get(
	  { pipe: pipe,
	    action: action,
	    target: action.targets()[0],
	    object: object },
	  /** @param result */ 
	  function(result) {
	    if(result)
	      cb_(result);
	    else {
	      action.error(new Error('action unsupported by accessor: ' + target));
	      return;
	    }
	  });
      });    
  };
  
  list = function(subject) {
    var data = {};
    for(var i in my.accessors) {
      if(my.accessors.hasOwnProperty(i) && (!id || id === i)) {
	data[i] = my.accessors[i].describe();
      }	
    }   
    return data;
  };

  fwk.method(that, 'register', register);
  fwk.method(that, 'unregister', unregister);
  fwk.method(that, 'accessor', accessor);
  fwk.method(that, 'list', list);

  return that;
};

exports.accessor = accessor;
