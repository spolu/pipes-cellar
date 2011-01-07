var util = require('util');
var fwk = require('fwk');

var pipe = require('pipe').pipe({});
var cellar = require('cellar');

var subject = process.argv[2] || '';
var target = process.argv[3] || '';
var body = process.argv[4] || 'BODY';


var action = cellar.action({ config: { 'TINT_NAME' : 'test' },
			     type:'MUT',
			     subject: subject,
			     targets: target,
			     body: body });

var msg = cellar.action.encapsulate(action, '2w');

pipe.send(msg, function(err, hdr, res) {
	    if(err) {
	      console.log('error!');
	      console.log(err.stack);	      
	    }
	    else
	      console.log(res.body);
	  });

