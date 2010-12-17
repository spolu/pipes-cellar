var util = require('util');
var fwk = require('fwk');

var pipe = require('pipe').pipe({});
var cellar = require('cellar');

var getter1 = function(spec, cont_) {
  var my = {};
  
  my.pipe = spec.pipe;
  my.action = spec.action;
  my.target = spec.target;
  my.object = spec.object;
  
  cont_(my.object);
};

var action1 = cellar.action({ type:'GET',
			      subject:'TEST',
			      body: getter1.toString() });

var msg1 = cellar.action.encapsulate(action1);

pipe.send(msg1, function(err, hdr, res) {
	    if(err)
	      console.log(err.stack);
	    else
	      console.log(res.body);
	  });
