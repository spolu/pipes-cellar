#!/usr/local/bin/node

var util = require('util');
var fs = require('fs');
var fwk = require('fwk');

var cfg = require("./config.js");

/** 
 * The Cellar Control Object
 * 
 * @extends {}
 * 
 * @param spec {}
 */ 
var pipectl = function(spec, my) {
  my = my || {};
  var _super = {};

  fwk.populateConfig(cfg.config);
  my.cfg = cfg.config;
  my.logger = fwk.logger();
  
  /** node can be picked up by using options */
  my.pipe = require('pipe').pipe({});
  
  var that = {};
  
  var parsenode, send;
  var usage, help, main;
  var register, unregister;
  var delnode, addnode;
  var subscribe, stop;
  var list, shutdown;
  
  parsenode = function(str) {
    var node = /^(.+):([0-9]+)$/.exec(str);
    if(node) {
      return { server: node[1],
	       port: node[2] };
    }
    else
      return undefined;
  };
  
  send = function(msg) {
    my.pipe.send(msg, function(err, hdr, res) {
		   console.log(util.inspect(res));
		   if(err) {
		     console.log(err.stack);
		     process.exit();		     
		   }
		   else {
		     console.log('OK!');
		   }
		 });    
  };
        
  help = function (cmd) {

    switch(cmd) {

    case 'register':
      console.log('Usage: cellarctl <cellar-tag> register <upd|get> <subject> <function.js>');
      console.log('');
      break;
      
    case 'unregister':
      console.log('Usage: cellarctl <cellar-tag> unregister <upd|get> <subject>');
      console.log('');
      break;
      
    case 'addnode':
      console.log('Usage: cellarctl <cellar-tag> addnode <server:port>');
      console.log('');
      break;
      
    case 'delnode':
      console.log('Usage: cellarctl <cellar-tag> delnode <server:port>');
      console.log('');
      break;

    case 'subscribe':
      console.log('Usage: cellarctl <cellar-tag> subscribe <server:port> <id> [tag]');
      console.log('');
      break;

    case 'stop':
      console.log('Usage: cellarctl <cellar-tag> stop <server:port> <id>');
      console.log('');
      break;
      
    case 'list':
      console.log('Usage: cellarctl <cellar-tag> list <mut|acc|node> [subject|server:port]');
      console.log('');
      break;

    case 'shutdown':
      console.log('Usage: cellarctl <cellar-tag> shutdown');
      console.log('');
      break;
      
    default:
      usage();
    }
  };
  
  usage = function() {
    console.log('Usage: cellarctl <cellar-tag> <command>');
    console.log('');
    console.log('<cellar-tag> is the tag used to subscribe');
    console.log('to pipe config registration');
    console.log('');
    console.log('<comand> is one of:');
    console.log('   register, unregister, addnode, delnode');
    console.log('   subscribe, stop, list, shutdown');
    console.log('');
    console.log('Config values can be specified in the ENV or');
    console.log('on the command line using:');
    console.log('  cellarctl <command> --KEY=VALUE');
    console.log('');
  };
  
  var node;

  main = function() {
    var args = fwk.extractArgvs();
    args = args.slice(2);
    
    if(args.length < 2) { usage(); return; }
    
    var ptag = args[0];
    var cmd = args[1];
    
    args = args.slice(2);    

    switch(cmd) {

    case 'register':
      if(args.length != 3) { 
	help('register'); 
	return; 
      }
      if(args[0] !== 'upd' && 
	 args[0] !== 'get') {
	help('register'); 
	return; 	
      }
      register(ptag, args[0], args[1], args[2]);     	
      break;

    case 'unregister':
      if(args.length != 2) { 
	help('unregister'); 
	return; 
      }
      if(args[0] !== 'upd' && 
	 args[0] !== 'get') {
	help('unregister'); 
	return; 	
      }
      unregister(ptag, args[0], args[1]);     	
      break;

    case 'addnode':
      if(args.length != 1) {
	help('addnode'); 
	return; 
      }
      node = parsenode(args[0]);
      if(typeof node === 'undefined') {
	help('addnode');
	return;
      }
      addnode(ptag, node.server, node.port);
      break;

    case 'delnode':
      if(args.length != 1) { 
	help('delnode'); 
	return; 
      }
      node = parsenode(args[0]);
      if(typeof node === 'undefined') {
	help('delnode');
	return;
      }
      delnode(ptag, node.server, node.port);     	
      break;
      
    case 'subscribe':
      if(args.length < 2 || args.length > 3) { 
	help('subscribe'); 
	return; 
      }
      node = parsenode(args[0]);
      if(typeof node === 'undefined') {
	help('subscribe');
	return;
      }
      subscribe(ptag, node.server, node.port, args[1], args[2]);     	
      break;

    case 'stop':
      if(args.length != 2) { 
	help('stop'); 
	return; 
      }
      node = parsenode(args[0]);
      if(typeof node === 'undefined') {
	help('stop');
	return;
      }
      stop(ptag, node.server, node.port, args[1]);     	
      break;      

    case 'list':
      console.log('Usage: cellarctl list <mut|acc|node> [subject|server:port]');
      if(args.length < 1 || args.length > 2) { 
	help('list'); 
	return; 
      }
      if(args[0] !== 'mut' && 
	 args[0] !== 'acc' &&
	 args[0] !== 'node') {
	help('list'); 
	return; 	
      }
      list(ptag, args[0], args[1]);
      break;
      
    case 'shutdown':
      if(args.length != 0) { 
	help('shutdown'); 
	return; 
      }
      shutdown(ptag);     	
      break;

    default:
      usage(); return;
    }
    
  }; 
  
  register = function(ptag, kind, subject, fpath) {
    fwk.readfile(
      fpath, 
      function(err, data) {
	if(err) {
	  console.log(err.stack);
	  process.exit();
	}
	if(kind === 'upd')
	  kind = 'updater';
	if(kind === 'get')
	  kind = 'getter';	
	var msg = fwk.message({});
	msg.setType('c')
	  .addTarget(ptag)
	  .setSubject('REGISTER')
	  .setBody({ kind: kind,
		     subject: subject,
		     fun: data });
	send(msg);
      });
  };

  unregister = function(ptag, kind, subject) {
    var msg = fwk.message({});
    if(kind === 'upd')
      kind = 'updater';
    if(kind === 'get')
      kind = 'getter';	
    msg.setType('c')
      .addTarget(ptag)
      .setSubject('UNREGISTER')
      .setBody({ kind: kind,
		 subject: subject });
    send(msg);
  };
  
  addnode = function(ptag, server, port) { 
    var msg = fwk.message({});
    msg.setType('c')
      .addTarget(ptag)
      .setSubject('ADDNODE')
      .setBody({ server: server,
		 port: port });
    send(msg);    
 };
  
  delnode = function(ptag, server, port) {
    var msg = fwk.message({});
    msg.setType('c')
      .addTarget(ptag)
      .setSubject('DELNODE')
      .setBody({ server: server,
		 port: port });
    send(msg);        
  };

  subscribe = function(ptag, server, port, id, tag) {
    var msg = fwk.message({});
    msg.setType('c')
      .addTarget(ptag)
      .setSubject('SUBSCRIBE')
      .setBody({ server: server,
		 port: port,
		 id: id,
		 tag: tag });
    send(msg);            
  };
  
  stop = function(ptag, server, port, id) {
    var msg = fwk.message({});
    msg.setType('c')
      .addTarget(ptag)
      .setSubject('STOP')
      .setBody({ server: server,
		 port: port,
		 id: id });
    send(msg);            
  };
  
  shutdown = function(ptag, server, port, id) {
    var msg = fwk.message({});
    msg.setType('c')
      .addTarget(ptag)
      .setSubject('SHUTDOWN');

    send(msg);    
  };

  that.method('main', main);
  
  return that;
};

/** main */
pipectl({}).main();
