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
  
  var forward, config;
  var updater, getter, addnode, delnode, subscribe, stop, shutdown;
  var bootstrap;
  

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
  
  config = function(id, msg) {
    var ctx = fwk.context({ config: my.cfg,
			    logger: my.logger });
    ctx.push(id);
    
    /** error handling */
    ctx.on('error', function(err) {
	     if(msg().type() === '2w-c') {
	       var reply = fwk.message.reply(msg);
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
      switch(msg.subject() + '-' + msg.type()) {
	
      case 'UPDATER-1w-c':
	ctx.log.out(msg.toString());
	updater(ctx, msg);
	break;
      case 'GETTER-1w-c':
	ctx.log.out(msg.toString());
	getter(ctx, msg);
	break;
      case 'SUBSCRIBE-1w-c':
	ctx.log.out(msg.toString());
	subscribe(ctx, msg);
	break;
      case 'STOP-1w-c':
	ctx.log.out(msg.toString());
	stop(ctx, msg);
	break;
      case 'ADDNODE-1w-c':
	ctx.log.out(msg.toString());
	addnode(ctx, msg);
	break;
      case 'DELNODE-1w-c':
	ctx.log.out(msg.toString());
	delnode(ctx, msg);
	break;	  
      case 'SHUTDOWN-1w-c':
	ctx.log.out(msg.toString());
	shutdown(ctx, msg);
	break;	  
	
      case 'LIST-2w-c':
	ctx.log.out(msg.toString());
	/** TODO */
	break;
	
      default:
	ctx.log.out('ignored: ' + msg.toString());
	break;
      }        
    }
    catch(err) {
      ctx.error(err, true);      
    }          
  };    
  
  
  updater = function(ctx, msg) {
    var spec = msg.body();

    if(!spec || !spec.subject || !spec.updater) {
      ctx.error(new Error('UPDATER: incomplete body'));
      return;
    }    
    my.mutator.updater(ctx, spec.subject, spec.updater);    
  };
  
  getter = function(ctx, msg) {
    var spec = msg.body();

    if(!spec || !spec.subject || !spec.getter) {
      ctx.error(new Error('GETTER: incomplete body'));
      return;
    }
    my.accessor.getter(ctx, spec.subject, spec.getter);        
  };
  
  addnode = function(ctx, msg) {    
    var spec = msg.body();

    if(!spec || !spec.server || !spec.port) {
      ctx.error(new Error('ADDNODE: incomplete body'));
      return;
    }
    delnode(ctx, msg);    
    
    my.pipe[spec.server + ':' + spec.port] = 
      require('pipe').pipe({ server: spec.server,
			     port: spec.port });
    var pipe = my.pipe[spec.server + ':' + spec.port];
    
    pipe.on('1w', forward(pipe));
    pipe.on('2w', forward(pipe));    
    pipe.on('1w-c', config);        
    pipe.on('2w-c', config);        
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
  };

  delnode = function(ctx, msg) {
    var spec = msg.body();

    if(!spec || !spec.server || !spec.port) {
      ctx.error(new Error('DELNODE: incomplete body'));
      return;
    }
      
    if(my.pipe.hasOwnProperty(spec.server + ':' + spec.port)) {
      my.pipe[spec.server + ':' + spec.port].stop();
      delete my.pipe[spec.server + ':' + spec.port];
    }    
  };
  
  subscribe = function(ctx, msg) {
    var spec = msg.body();
    
    if(!spec || !spec.server || !spec.port ||
       !spec.id || !spec.tag) {
      ctx.error(new Error('SUBSCRIBE: incomplete body'));
      return;
    }
    
    if(!my.pipe.hasOwnProperty(spec.server + ':' + spec.port)) {
      addnode(ctx, msg);
    }
    
    var pipe = my.pipe[spec.server + ':' + spec.port];
    pipe.subscribe(spec.id, spec.tag);
  };
  
  stop = function(ctx, msg) {
    var spec = msg.body();
    
    if(!spec || !spec.server || !spec.port ||
       !spec.id) {
      ctx.error(new Error('STOP: incomplete body'));
      return;
    }
      
    if(my.pipe.hasOwnProperty(spec.server + ':' + spec.port)) {
      var pipe = my.pipe[spec.server + ':' + spec.port];
      pipe.stop(spec.id);      
    }  
  };
  
  shutdown = function(ctx, msg) {
    for(var i in my.pipe) {
      if(my.pipe.hasOwnProperty(i)) {
	/** we close all registration. this will shutdown cellar? */
	my.pipe[i].stop();
      }
    } 
  };
  
  
  bootstrap = function() {    
    var msg = fwk.message({});
    msg.setType('c')
      .setSubject('ADDNODE')
      .setBody({ server: my.cfg['PIPE_BOOTSTRAP_SERVER'],
		 port: my.cfg['PIPE_BOOTSTRAP_PORT'] });

    /** We add the bootstrap node */
    config('bootstrap', msg);
  };
  

  bootstrap();

  return that;
};

/** main */
cellar({});