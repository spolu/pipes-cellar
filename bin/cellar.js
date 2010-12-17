var util = require('util');
var fwk = require('fwk');

var cfg = require("./config.js");
var mongo = require("./mongo.js");
var mutator = require("./mutator.js");

/**
 * The Cellar Object
 *
 * @extends {}
 *  
 * @param spec {registration, tag}
 */
var cellar = function(spec, my) {
  my = my || {};
  var _super = {};
  
  fwk.populateConfig(cfg.config);  
  my.cfg = cfg.config;
  my.logger = fwk.logger();
  
  my.registration = spec.registration || my.cfg['PIPE_REGISTRATION'];
  my.tag = spec.tag || my.cfg['PIPE_TAG'];
  
  /** Defaults will be overridden by configuration */
  my.pipe = require('pipe').pipe({});  
  my.mongp = mongo.mongo({ config: my.cfg });

  my.mutator = mutator.mutator({ pipe: my.pipe,
				 mongo: my.mongo,
				 config: my.cfg });

  var that = {};
  
  var forward;

  forward = function(id, msg) {
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
		  my.pipe.send(reply, function(err, hdr, res) {
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
	my.mutator.mutator(action, function(res) {
			     if(action.msg().type() === '2w') {
			       var reply = fwk.message.reply(action.msg());
			       reply.setBody(res);
			       my.pipe.send(reply, function(err, hdr, res) {
					      if(err)
						action.log.error(err);
					    });
			     }
			   });
	break;
      case 'GET-2w':
	action.log.out(action.toString());
	my.getter.getter(action, function(res) {
			   if(action.msg().type() === '2w') {
			     var reply = fwk.message.reply(action.msg());
			     reply.setBody(res);
			     my.pipe.send(reply, function(err, hdr, res) {
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
      default:
	action.log.out('Ignored: ' + action.toString());
	break;
      }        
    }
    catch(err) {
      action.error(err, true);      
    }
  };


  my.pipe.on('1w', forward);
  my.pipe.on('2w', forward);

  my.pipe.on('disconnect', function(id) {
	       console.log('disconnect ' + id);
	     });
  
  my.pipe.on('connect', function(id) {
	       console.log('connect ' + id); 
	     });
  
  my.pipe.on('error', function(err, id) {
	       console.log('error ' + id + ':' + err.stack);
	     });
  
  my.pipe.subscribe(my.registration, my.tag);    
  
  return that;
};

/** main */
cellar({});