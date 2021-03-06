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
var cellar = require('pipes-cellar');

var cfg = require("./config.js");

/**
 * A Mpr object
 * 
 * Constructs the map reduce request accordingly to the function it was
 * constructed with
 *
 * @param spec {ctx, subject, mprfun}
 */
var mpr = function(spec, my) {
  my = my || {};
  var _super = {};   
  
  my.subject = spec.subject;
  my.ctx = spec.ctx;  

  if(spec.mprfun && typeof spec.mprfun === 'function') {
    my.mpr = function(sp, cb_) {
      try {
	return spec.mprfun(sp, cb_);
      } catch (err) { 
	my.ctx.log.error(err, true);
	return null;
      }
    };
    my.mprdata = spec.mprfun.toString();
  }
  else
    my.mpr = function(spec, cont_) {
      cont_();
    };
  
  var that = {};

  var mpr, describe;
  
  mpr = function(spec, cb_) {
    my.mpr(spec, cb_);
  };
  
  describe = function() {
    var data = { subject: my.subject,
		 mpr: my.mprdata };
    return data;
  };
  
  fwk.method(that, 'mpr', mpr);
  fwk.method(that, 'describe', describe);
  
  fwk.getter(that, 'subject', my, 'subject');  

  return that;
};


/**
 * The Mapreduce Object
 * 
 * Carries on ACC request and store the getters registered over the network
 *
 * @extends {}
 *  
 * @param spec {mongo, config}
 */
var mapreduce = function(spec, my) {
  my = my || {};
  var _super = {};
  
  my.cfg = spec.config || cfg.config;
  my.mongo = spec.mongo;

  my.mprs = {};

  var that = {};
  
  var register, unregister, mapreduce, list;
  
  register = function(ctx, subject, mprfun) {
    unregister(subject);
    my.mprs[subject] = mpr({ ctx: ctx,
			     subject: subject, 
			     mprfun: mprfun});
    ctx.log.out('register: ' + subject);
  };
  
  unregister = function(ctx, subject) {
    if(my.mprs.hasOwnProperty(subject)) {
      delete my.mprs[subject];
      ctx.log.out('unregister: ' + subject);
    }
  };

  /** cb_(res) */  
  mapreduce = function(pipe, action, cb_) {
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
    if(!my.mprs[action.subject()]) {
      action.error(new Error('No mapreduce for subject: ' + action.subject()));
      return;            
    }
    
    /** generate mapreduce spec { map, reduce, ... } and call */
    my.mprs[action.subject()].mpr(
      { pipe: pipe,
	action: action,
	target: action.targets()[0] },
      function(spec) {
	if(spec && 
	   typeof spec.map === 'function' &&
	   typeof spec.reduce === 'function') {
	  my.mongo.mapreduce(
	    action, action.targets()[0],
	    spec.map, spec.reduce, spec.options,
	    function(result) {
	      cb_(result);
	    });
	}
	else {
	  action.error(new Error('action unsupported by mapreduce: ' + target));
	  return;
	}
      });        
  };
  
  list = function(subject) {
    var data = {};
    for(var i in my.mprs) {
      if(my.mprs.hasOwnProperty(i) && (!id || id === i)) {
	data[i] = my.mprs[i].describe();
      }	
    }   
    return data;
  };

  fwk.method(that, 'register', register);
  fwk.method(that, 'unregister', unregister);
  fwk.method(that, 'mapreduce', mapreduce);
  fwk.method(that, 'list', list);

  return that;
};

exports.mapreduce = mapreduce;
