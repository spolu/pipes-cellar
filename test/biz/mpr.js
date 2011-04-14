function(spec, cont_) {
  var my = {};
    
  my.pipe = spec.pipe;
  my.action = spec.action;
  my.target = spec.target;

  var mprspec = { map: function() {},
		  reduce: function() {},
		  options: {} };
  
  cont_(mprspec);
}
