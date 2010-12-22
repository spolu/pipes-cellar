var util = require('util');
var fwk = require('fwk');

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
  
  if(spec.type === 'MUT' || spec.type === 'ACC' || 
     spec.type === 'GET' || spec.type === 'UPD')
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
  
  that.getter('type', my, 'type');  
  that.getter('subject', my, 'subject');  
  that.getter('targets', my, 'targets');  
  that.getter('body', my, 'body');  
  that.getter('msg', my, 'msg');  
  
  that.method('addTarget', addTarget);
  that.method('toString', toString);

  return that;
};

/**
 * Regexp matching valid action fwk.message subject and builder
 */
var subject = { match: /^(MUT|ACC|GET|UPD):([a-z\-A-Z0-9]*)$/,
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