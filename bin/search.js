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

      if(!action.body()) {
	  action.error(new Error('No body defined'));
	  return;      	  	  
      }
      
      if(typeof action.body().selector === 'undefined') {
	  action.error(new Error('No selector defined'));
	  return;      	  
      }
      var selector = action.body().selector || {};
      var options = action.body().options || {};

      my.mongo.find(
	  action, 
	  action.targets()[0],
	  selector,
	  options,	  
	  function(result) {
	      cb_(result);
	  });
  };
    
    fwk.method(that, 'search', search);
    
    return that;
};

exports.search = search;
