function(spec, cont_) {
  var my = {};
  
  my.pipe = spec.pipe;
  my.action = spec.action;
  my.target = spec.target;
  my.object = spec.object;
  
  cont_({ object: { data: my.action.body(),
		    target: my.target,
		    subject: my.action.subject() },
	  result: 'OK-updaterd2' });
}