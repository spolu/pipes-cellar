var util = require('util');
var fwk = require('fwk');

var pipe = require('pipe').pipe({});
var cellar = require('cellar');

var updater1 = function(spec, cont_) {
  var my = {};
  
  my.pipe = spec.pipe;
  my.action = spec.action;
  my.target = spec.target;
  my.object = spec.object;
  
  cont_();
};

var action1 = cellar.action({ type:'UPD',
			      subject:'TEST-ERR',
			      body: updater1.toString() });

var msg1 = cellar.action.encapsulate(action1);

pipe.send(msg1, function(err, hdr, res) {
	    if(err)
	      console.log(err.stack);
	    else
	      console.log(res.body);
	  });

var updater2 = function(spec, cont_) {
  var my = {};
  
  my.pipe = spec.pipe;
  my.action = spec.action;
  my.target = spec.target;
  my.object = spec.object;
  
  cont_({ object: { data: my.action.body(),
		    target: my.target,
		    subject: my.action.subject() },
	  result: 'OK-updaterd2' });
};

var action2 = cellar.action({ type:'UPD',
			      subject:'TEST',
			      body: updater2.toString() });

var msg2 = cellar.action.encapsulate(action2);

pipe.send(msg2, function(err, hdr, res) {
	    if(err)
	      console.log(err.stack);
	    else
	      console.log(res.body);
	  });
