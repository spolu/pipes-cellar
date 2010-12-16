var util = require('util');
var cellar = require('cellar');


var a = cellar.action({ type:'MUT',
			subject:'TEST',
			targets:'a',
			body: {test: 'asd', b: ['a','a']} });
a.addTarget('c');

var protocol = /^(MUT|GET):([a-zA-Z0-9]*)$/;
var res;

res = protocol.exec('GET:');

if(res[1] === 'GET')
  console.log('+');

if(res[2] === '')
  console.log('+');

if(a.subject() === 'TEST')
  console.log('+');


var dumpAction = function (a) {
  var str = a.toString();
  str += ' ' + a.type() + ' ' + a.subject() + ' ' + util.inspect(a.targets()) + ' ' + util.inspect(a.body());
  return str;
};


var dumpMsg = function (msg) {
  var str = msg.toString();
  str += ' ' + msg.type() + ' ' + msg.subject() + ' ' + util.inspect(msg.targets()) + ' ' + util.inspect(msg.body());
};

var stra = dumpAction(a);
var msg = cellar.action.encapsulate(a);
var strmsg = dumpMsg(msg);
var ab = cellar.action.decapsulate({msg: msg});
ab.setTint('testTint');
var strab = dumpAction(ab);
var msgb = cellar.action.encapsulate(ab);
var strmsgb = dumpMsg(msgb);


if(stra === strab)
  console.log('+');

if(strmsg === strmsgb)
  console.log('+');

