var util = require('util');
var fwk = require('fwk');

var config = require("./config.js");


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
  
  my.registration = spec.registration || my.cfg['PIPE_REGISTRATION'];
  my.tag = spec.tag || my.cfg['PIPE_TAG'];
  
  /** Defaults will be overridden by configuration */
  my.pipe = require('pipe').pipe({});
  

  var that = {};
  
  pipe.on('1w', function(id, msg) { 
	    try {
	      var subj = {};
	      if(msg.subject())
		subj = JSON.parse(msg.subject());
	
	      switch(subj.action) {
	      case 'MUT':
		break;
	      case 'GET':
		break;
	      default:
		/** do nothing */
		break;
	      } 	      	      
	    } catch (err) {
	      console.log('error');
	    }
	  });
  pipe.on('2w', function(id, msg) { });

  pipe.on('disconnect', function(id) {
	    console.log('disconnect ' + id);
	  });
  
  pipe.on('connect', function(id) {
	    console.log('connect ' + id); 
	  });

  pipe.on('error', function(err, id) {
	    console.log('error ' + id + ':' + err.stack);
	  });

  pipe.subscribe(my.registration, my.tag);    
  
  return that;
};