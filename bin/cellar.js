var util = require('util');
var fwk = require('fwk');

var cfg = require("./config.js");
var mongo = require("./mongo.js");

/**
 * The Cellar Object
 *
 * @extends {}
 *  
 * @param spec {port}
 */
var cellar = function(spec, my) {
  my = my || {};
  var _super = {};
  
  fwk.populateConfig(cfg.config);  
  my.cfg = cfg.config;
  my.logger = fwk.logger();
  
  my.port = spec.port || my.cfg['CELLAR_PORT'];
    
  /** Defaults will be overridden by configuration */
  my.pipe = {};
  my.mongo = mongo.mongo({ config: my.cfg });

  my.mutator = require('./mutator.js').mutator({ mongo: my.mongo,
						 config: my.cfg });  
  my.accessor = require('./accessor.js').accessor({ mongo: my.mongo,
						    config: my.cfg });

  var that = {};
  
  var handle, forward;
  var addnode, delnode, subscribe, stop, shutdown;
  var bootstrap;
  

  handle = function(action, pipe) {    
  };

  forward = function(pipe) {
    return function(id, msg) {
      var action = require('cellar')
	.action.decapsulate({ msg: msg,
			      config: my.cfg,
			      logger: my.logger });
      action.push(id);

      /** error handling */
      action.on('error', function(err) {
		  if(action.msg().type() === '2w') {
		    var reply = fwk.message.reply(action.msg());
		    reply.setBody({ error: err.message });
		    pipe.send(reply, function(err, hdr, res) {
				if(err)
				  action.log.error(err);
			      });
		  }
		  /** else nothing to do */
		  /** TODO push an error mesage */		  
		});
      
      try {      
	switch(action.type() + '-' + action.msg().type()) {
	case 'MUT-1w':  
	case 'MUT-2w':  
	  action.log.out(action.toString());
	  my.mutator.mutator(pipe, action, function(res) {
			       if(action.msg().type() === '2w') {
				 var reply = fwk.message.reply(action.msg());
				 reply.setBody(res);
				 pipe.send(reply, function(err, hdr, res) {
					     if(err)
					       action.log.error(err);
					   });
			       }
			     });
	  break;
	case 'ACC-2w':
	  action.log.out(action.toString());
	  my.accessor.accessor(pipe, action, function(res) {
				 if(action.msg().type() === '2w') {
				   var reply = fwk.message.reply(action.msg());
				   reply.setBody(res);
				   pipe.send(reply, function(err, hdr, res) {
					       if(err)
						 action.log.error(err);
					     });
				 }
			       });
	  break;
	case 'UPD-1w':
	  action.log.out(action.toString());
	  my.mutator.updater(action);
	  break;
	case 'GET-1w':
	  action.log.out(action.toString());
	  my.accessor.getter(action);
	  break;
	default:
	  action.log.out('ignored: ' + action.toString());
	  break;
	}        
      }
      catch(err) {
	action.error(err, true);      
      }    
    };
  };

  addnode = function(action) {    
    var spec = {};
    for(var i = 0; i < action.targets().length; i ++) {
      var targ = /^(.+):([0-9]+)$/.exec(action.targets()[i]);
      if(targ) {
	spec.server = targ[1];
	spec.port = targ[2];
      }
      else
	continue;

      if(!spec.server || !spec.port) {
	action.error(new Error('ADD: incomplete target: ' + action.targets()[i]));
	return;      
      }
      
      delnode(action);    

      my.pipe[spec.server + ':' + spec.port] = 
	require('pipe').pipe({ server: spec.server,
			       port: spec.port });
      var pipe = my.pipe[spec.server + ':' + spec.port];
      
      /** careful pipe will be modified by the loop
       *  works here since forward return a function
       *  that binds to pipe
       */
      pipe.on('1w', forward(pipe));
      pipe.on('2w', forward(pipe));    
      
      pipe.on('disconnect', function(id) {
		console.log('disconnect ' + id);
	      });
      
      pipe.on('connect', function(id) {
		console.log('connect ' + id); 
	      });
      
      pipe.on('error', function(err, id) {
		console.log('error ' + id + ':' + err.stack);
	      });
      
      pipe.subscribe(my.cfg['PIPE_CONFIG_REG'], 
		     my.cfg['PIPE_CONFIG_TAG']);      
    }
  };

  delnode = function(action) {
    var spec = {};
    for(var i = 0; i < action.targets().length; i ++) {
      var targ = /^(.+):([0-9]+)$/.exec(action.targets()[i]);
      if(targ) {
	spec.server = targ[1];
	spec.port = targ[2];
      }
      else
	continue;  

      if(!spec.server || !spec.port) {
	action.error(new Error('DEL: incomplete target: ' + action.targets()[i]));
	return;      
      }
      
      if(my.pipe.hasOwnProperty(spec.server + ':' + spec.port)) {
	my.pipe[spec.server + ':' + spec.port].stop();
	delete my.pipe[spec.server + ':' + spec.port];
      }    
    }
  };
  
  subscribe = function(action) {
    var spec = {};
    for(var i = 0; i < action.targets().length; i ++) {
      var targ = /^(.+):([0-9]+):(.+):(.+)$/.exec(action.targets()[i]);
      if(targ) {
	spec.server = targ[1];
	spec.port = targ[2];
	spec.id = targ[3];
	spec.tag = targ[4];
      }
      else
	continue;  
      
      if(!spec.server || !spec.port || 
	 !spec.id || !spec.tag) {
	action.error(new Error('SUB: incomplete target: ' + action.targets()[i]));
	return;      
      }
      
      if(my.pipe.hasOwnProperty(spec.server + ':' + spec.port)) {
	var pipe = my.pipe[spec.server + ':' + spec.port];
	pipe.subscribe(spec.id, spec.tag);
      }
    }
  };
  
  stop = function(action) {
    var spec = {};
    for(var i = 0; i < action.targets().length; i ++) {
      var targ = /^(.+):([0-9]+):(.+)$/.exec(action.targets()[i]);
      if(targ) {
	spec.server = targ[1];
	spec.port = targ[2];
	spec.id = targ[3];
      }
      else
	continue;  
      
      if(!spec.server || !spec.port || !spec.id) {
	action.error(new Error('STP: incomplete target: ' + action.targets()[i]));
	return;      
      }
      
      if(my.pipe.hasOwnProperty(spec.server + ':' + spec.port)) {
	var pipe = my.pipe[spec.server + ':' + spec.port];
	pipe.stop(spec.id);      
      }  
    }
  };
  
  shutdown = function(action) {
    for(var i in my.pipe) {
      if(my.pipe.hasOwnProperty(i)) {
	/** we close all registration. this will shutdown cellar? */
	my.pipe[i].stop();
      }
    } 
  };
  
  
  bootstrap = function() {    
    var msg = fwk.message({});
    msg.setType('1w');
    
    var action = require('cellar')
      .action({ type: 'ADD',
		subject: 'bootstrap',
		targets: [my.cfg['PIPE_BOOTSTRAP_SERVER'] + ':' + my.cfg['PIPE_BOOTSTRAP_PORT']],
		msg: msg,
		config: my.cfg,
		logger: my.logger });
    action.setTint('boot-' + my.cfg['PIPE_CONFIG_TAG']);

    /** We add the bootstrap node */
    addnode(action);    
  };
  

  bootstrap();

  return that;
};

/** main */
cellar({});