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

/**
 * An Action is a context holder for cellar
 * 
 * @extends fwk.context
 * 
 * @param spec {type, subject, targets, body, config, logger}
 */ 
var action = function(spec, my) {
  my = my || {};
  var _super = {};
  
  var that = fwk.context(spec, my);
  
  if(spec.type === 'MUT' || 
     spec.type === 'ACC' || 
     spec.type === 'SRH' ||
     spec.type === 'MPR')
    my.type = spec.type || 'NONE';
  else
    my.type = 'NOP';
    
  if((/^[a-z\-A-Z0-9]+$/).exec(spec.subject))
    my.subject = spec.subject;
  else
    my.subject = '';

  if(spec.targets instanceof Array)
    my.targets = spec.targets || [];
  else if (typeof spec.targets === 'string')
    my.targets = [spec.targets];    
  else
    my.targets = [];

  my.body = spec.body;
  my.meta = spec.meta;
  my.msg = spec.msg; 

  that.push(my.type);
  that.push(my.subject);

  var addTarget;
  
  toString = function() {
    var str = '';
    str += my.type;
    str += ' ' + my.subject;
    for(var i = 0; i < my.targets.length; i ++) {
      str += ((i == 0) ? ' {' : ', ') + my.targets[i] + 
	((i == my.targets.length - 1) ? '}' : '');
    }
    if(my.msg)
      str += ' ' + my.msg.type();
    return str;
  };

  addTarget = function(target) {
    my.targets.push(target);
  };
  
  fwk.getter(that, 'type', my, 'type');  
  fwk.getter(that, 'subject', my, 'subject');  
  fwk.getter(that, 'targets', my, 'targets');  
  fwk.getter(that, 'body', my, 'body');  
  fwk.getter(that, 'msg', my, 'msg');  
  fwk.getter(that, 'meta', my, 'meta');  
  
  fwk.method(that, 'addTarget', addTarget);
  fwk.method(that, 'toString', toString);

  return that;
};

/**
 * Regexp matching valid action fwk.message subject and builder
 */
var subject = { match: /^(MUT|ACC|SRH|MPR):([a-z\-A-Z0-9]*)$/,
		build: function(action) {
		  return action.type() + ":" + action.subject();
		} };
exports.subject = subject;

/**
 * Regexp matching valid target 
 */
var target = {
  match: /^([a-zA-Z0-9]+)\.([a-zA-Z0-9]+)$/,
  build: function(c, id) {
    return c + '.' + id;
  }
};
exports.target = target;


/**
 * Decapsulates an action from a received fwk.message
 * 
 * @param spec {msg, config, logger}
 */
action.decapsulate = function(spec) {
  var subj = subject.match.exec(spec.msg.subject());
  if(subj) {
    spec.type = subj[1];
    spec.subject = subj[2];
    spec.targets = spec.msg.targets();
    spec.body = spec.msg.body();
    spec.meta = spec.msg.meta();
  }
  var a = action(spec);
  /** tint forwarding */
  a.setTint(spec.msg.tint());

  return a;
};

/**
 * Encapsulate an action in a fwk.message
 * 
 * @param action the action to encapsulate
 * @param msgtype the fwk.message type to use (1w, 2w)
 */
action.encapsulate = function(a, msgtype) {
  /** tint forwarding */
  var msg = fwk.message({ctx: a});
  
  msg.setType(msgtype || '1w');
  
  msg.setSubject(subject.build(a));
  for(var i = 0; i < a.targets().length; i ++) {
    msg.addTarget(a.targets()[i]);
  }
  msg.setBody(a.body());  

  return msg;
};

exports.action = action;
