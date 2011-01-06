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
  
  var send, forward, config;
  var register, unregister;
  var addnode, delnode;
  var subscribe, stop;
  var shutdown, list;
  var bootstrap;
  
  send = function(pipe, ctx, reply) {
    pipe.send(reply, function(err, hdr, res) {
		if(err)
		  ctx.log.error(err);
		/** TODO push an error message */
	      });    
    ctx.finalize();
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
		    send(pipe, action, reply);
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
				 send(pipe, action, reply);
			       }
			     });
	  break;

	case 'ACC-2w':
	  action.log.out(action.toString());
	  my.accessor.accessor(pipe, action, function(res) {
				 if(action.msg().type() === '2w') {
				   var reply = fwk.message.reply(action.msg());
				   reply.setBody(res);
				   send(pipe, action, reply);
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
  
  config = function(pipe) {    
    return function(id, msg) {
      var ctx = fwk.context({ config: my.cfg,
			      logger: my.logger });
      ctx.push(id);
      
      /** error handling */
      ctx.on('error', function(err) {
	       if(msg().type() === '2w-c') {
		 var reply = fwk.message.reply(msg);
		 reply.setBody({ error: err.message });
		 send(pipe, ctx, reply);
	       }
	       /** else nothing to do */
	       /** TODO push an error mesage */		  
	     });
      
      try {      
	switch(msg.subject() + '-' + msg.type()) {
	  
	case 'REGISTER-2w-c':
	  ctx.log.out(msg.toString());
	  register(pipe, ctx, msg);
	  break;
	case 'UNREGISTER-2w-c':
	  ctx.log.out(msg.toString());
	  unregister(pipe, ctx, msg);
	  break;

	case 'ADDNODE-2w-c':
	  ctx.log.out(msg.toString());
	  addnode(pipe, ctx, msg);
	  break;
	case 'DELNODE-2w-c':
	  ctx.log.out(msg.toString());
	  delnode(pipe, ctx, msg);
	  break;	  

	case 'SUBSCRIBE-2w-c':
	  ctx.log.out(msg.toString());
	  subscribe(pipe, ctx, msg);
	  break;
	case 'STOP-2w-c':
	  ctx.log.out(msg.toString());
	  stop(pipe, ctx, msg);
	  break;
	  
	case 'LIST-2w-c':
	  ctx.log.out(msg.toString());
	  list(pipe, ctx, msg);
	  break;

	case 'SHUTDOWN-1w-c':
	  ctx.log.out(msg.toString());
	  shutdown(ctx, msg);
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
  };

  
  register = function(pipe, ctx, msg) {
    var spec = msg.body();

    if(!spec || !spec.subject || !spec.fun || !spec.kind) {
      ctx.error(new Error('REGISTER: incomplete body'));
      return;
    }    
    
    ctx.log.debug('updater evaluating: ' + spec.fun);
    eval("var fun = " + spec.fun);
    
    if(typeof fun === 'function') {
      switch(spec.kind) {
      case 'updater':
	my.mutator.register(ctx, spec.subject, fun);    
	break;
      case 'getter':
	my.accessor.register(ctx, spec.subject, fun);        
	break;
      default:
	ctx.error(new Error('Unknown kind: ' + kind));      
	return;	
      }
    }
    else
      ctx.error(new Error('Function eval error'));      
    
    var reply = fwk.message.reply(msg);
    reply.setBody({ status: 'OK' });
    send(pipe, ctx, reply);
  };
  
  unregister = function(pipe, ctx, msg) {
    var spec = msg.body();

    if(!spec || !spec.subject || !spec.kind) {
      ctx.error(new Error('UNREGISTER: incomplete body'));
      return;
    }
    
    switch(spec.kind) {
    case 'updater':
      my.mutator.unregister(ctx, spec.subject);    
      break;
    case 'getter':
      my.accessor.unregister(ctx, spec.subject);        
      break;
    default:
      ctx.error(new Error('Unknown kind: ' + kind));      
      return;	
    }    
    
    var reply = fwk.message.reply(msg);
    reply.setBody({ status: 'OK' });
    send(pipe, ctx, reply);
  };
  
  addnode = function(pipe, ctx, msg) {    
    var spec = msg.body();

    if(!spec || !spec.server || !spec.port) {
      ctx.error(new Error('ADDNODE: incomplete body'));
      return;
    }
    delnode(pipe, ctx, msg);    
    
    my.pipe[spec.server + ':' + spec.port] = 
      require('pipe').pipe({ server: spec.server,
			     port: spec.port });
    var p = my.pipe[spec.server + ':' + spec.port];
    
    p.on('1w', forward(p));
    p.on('2w', forward(p));    
    p.on('1w-c', config(p));        
    p.on('2w-c', config(p));        
    p.on('disconnect', function(id) {
	   console.log('disconnect ' + id);
	 });
    
    p.on('connect', function(id) {
	   console.log('connect ' + id); 
	 });
    
    p.on('error', function(err, id) {
	   console.log('error ' + id + ':' + err.stack);
	 });
    
    p.subscribe(my.cfg['PIPE_CONFIG_REG'], 
		my.cfg['PIPE_CONFIG_TAG']);      

    if(pipe) {
      var reply = fwk.message.reply(msg);
      reply.setBody({ status: 'OK' });
      send(pipe, ctx, reply);      
    }
  };

  delnode = function(pipe, ctx, msg) {
    var spec = msg.body();

    if(!spec || !spec.server || !spec.port) {
      ctx.error(new Error('DELNODE: incomplete body'));
      return;
    }
      
    if(my.pipe.hasOwnProperty(spec.server + ':' + spec.port)) {
      my.pipe[spec.server + ':' + spec.port].stop();
      delete my.pipe[spec.server + ':' + spec.port];
    }    

    if(pipe) {
      console.log('REPLYING! A');
      var reply = fwk.message.reply(msg);
      reply.setBody({ status: 'OK' });
      send(pipe, ctx, reply);
    }
  };
  
  subscribe = function(pipe, ctx, msg) {
    var spec = msg.body();
    
    if(!spec || !spec.server || !spec.port ||
       !spec.id || !spec.tag) {
      ctx.error(new Error('SUBSCRIBE: incomplete body'));
      return;
    }
    
    if(!my.pipe.hasOwnProperty(spec.server + ':' + spec.port)) {
      addnode(null, ctx, msg);
    }
    
    var p = my.pipe[spec.server + ':' + spec.port];
    p.subscribe(spec.id, spec.tag);

    var reply = fwk.message.reply(msg);
    reply.setBody({ status: 'OK' });
    send(pipe, ctx, reply);
  };
  
  stop = function(pipe, ctx, msg) {
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

    var reply = fwk.message.reply(msg);
    reply.setBody({ status: 'OK' });
    send(pipe, ctx, reply);
  };
  
  shutdown = function(ctx, msg) {
    for(var i in my.pipe) {
      if(my.pipe.hasOwnProperty(i)) {
	/** we close all registration. this will shutdown cellar? */
	my.pipe[i].stop();
      }
    } 
  };
  
  
  /** list mutator, accessor, node */
  list = function(pipe, ctx, msg) {
    var spec = msg.body();
    
    if(!spec || !spec.kind) {
      ctx.error(new Error('LIST: incomplete body'));
      return;
    }
    
  };
  
  bootstrap = function() {    
    var msg = fwk.message({});
    msg.setType('2w-c')
      .setSubject('ADDNODE')
      .setBody({ server: my.cfg['PIPE_BOOTSTRAP_SERVER'],
		 port: my.cfg['PIPE_BOOTSTRAP_PORT'] });
    
    /** We add the bootstrap node */
    var ctx = fwk.context({ config: my.cfg,
			    logger: my.logger });
    ctx.push('bootstrap');
    addnode(null, ctx, msg);
  };
  

  bootstrap();

  return that;
};

/** main */
cellar({});