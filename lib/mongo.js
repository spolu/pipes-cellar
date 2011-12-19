// Copyright Stanislas Polu
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

var util = require('util');
var events = require('events');
var fwk = require('pipes');
var mongodb = require('mongodb');

var cfg = require("./config.js");

/**
 * Regexp matching valid target 
 */
var targetfactory = {
  match: /^([a-zA-Z0-9]+)\.([a-zA-Z0-9\_]+)$/,
  build: function(c, id) {
    return c + '.' + id;
  }
};
exports.targetfactory = targetfactory;


/**
 * A MongoDB helper object.
 * 
 * Relies on callbacks to return objects or status of a set.
 * Errors are passed directly to the ctx and the callback is ignored.
 * 
 * @extends events.EventEmitter
 * 
 * @param spec {baseurl, dbname}
 */
var mongo = function(spec, my) {
  my = my || {};
  var _super = {};
  
  fwk.populateConfig(cfg.config);  
  my.cfg = cfg.config;
  my.url = spec.baseurl || my.cfg['MONGO_BASEURL'];
  my.url += spec.dbname || my.cfg['MONGO_DBNAME'];
  
  my.collection = {};
  
  my.lock = fwk.lock({});

  my.ctx = fwk.context({config: my.cfg});
  my.ctx.setTint('mongo-' + process.pid);
  
  my.connected = false;
  my.db = null;

  my.ctx.log.debug('MONGO OPEN DB: ' + my.url);
  
  /** 
   * Events emitted:
   * 'update' : target, obj, hash
   */
  var that = new events.EventEmitter();
  
  var open, get, set, find, mapreduce;
  
  /** cb_(collection) */
  open = function(ctx, name, cb_) {        
    my.lock.wlock(
      'master',  //TODO: parralelize if bottleneck (only needed for connect)
      function(unlock) {
	
	var openfun = function() {
	  if(my.collection[name]) {		  	    	    
	    unlock(); cb_(my.collection[name]);
	  }
	  else {
	    my.db.collection(
	      name, 
	      function(err, c) { 
		if(err) { unlock(); ctx.error(err); return; }
		var col = collection({ collection: c, name: name, config: my.cfg });
		my.ctx.log.debug('MONGO OPEN COL: ' + name);
		col.on('update', function(id, obj, hash) {
			 that.emit('update', name + '.' + id, obj, hash);
		       });
		my.collection[name] = col;
		unlock(); cb_(col);		
	      });		 
	  }
	};
	
	if(!my.connected || !my.db) {	  
	  mongodb.connect(my.url, {}, function(err, db) {
			    if(err) { unlock(); ctx.error(err, true); return; }
			    my.db = db; my.connected = true;
			    my.db.on('close', function(error) {
				       console.log('Connection to DB was closed: ' + error);
				       my.connected = false;
				       my.collection = {};
				     });
			    openfun();
			  });      
	}
	else {
	  openfun();
	}
      });    
  };
  
  /** cb_(obj) */
  get = function(ctx, target, cb_) {
    var targ = targetfactory.match.exec(target);
    if(targ) {
      open(ctx, targ[1], function(c) {
	     c.get(ctx, targ[2], cb_);
	   });
    }
    else {
      ctx.error(new Error('Invalid target: ' + target));      
    }
  };

  /** cb_(status) */
  set = function(ctx, target, prevhash, obj, cb_) {
    //util.debug('SET: ' + target /*+ ' ' + util.inspect(obj)*/);
    var targ = targetfactory.match.exec(target);
    if(targ) {
      open(ctx, targ[1], function(c) {
	     c.set(ctx, targ[2], prevhash, obj, cb_);
	   });
    }
    else
      ctx.error(new Error('Invalid target: ' + target));
  };
  
  /** cb_(result) */
  find = function(ctx, target, selector, cb_) {
    var targ = targetfactory.match.exec(target);
    if(targ) {
      open(ctx, targ[1], function(c) {
	     c.find(ctx, selector, cb_);
	   });
    }
    else
      ctx.error(new Error('Invalid target: ' + target));         
  };

  /** cb_(result) */
  mapreduce = function(ctx, target, map, reduce, options, cb_) {
    var targ = targetfactory.match.exec(target);
    if(targ) {
      open(ctx, targ[1], function(c) {
	     c.mapreduce(ctx, map, reduce, options, cb_);
	   });
    }
    else
      ctx.error(new Error('Invalid target: ' + target));         
  };
  
  
  fwk.method(that, 'get', get);
  fwk.method(that, 'set', set);
  fwk.method(that, 'find', find);
  fwk.method(that, 'mapreduce', mapreduce);

  return that;
};

/** possible return status */
mongo.status = {
  success: 'SUCCESS',
  retry: 'RETRY',
  noop: 'NOOP'
};

exports.mongo = mongo;

/**
 * A MongoDB Collection helper object
 * 
 * @extends events.EventEmitter
 * 
 * @param spec {collection, name, timeout, config}
 */
var collection = function(spec, my) {
  my = my || {};
  var _super = {};

  my.cfg = spec.config || cfg.config;
  my.collection = spec.collection;
  my.name = spec.name;
  my.timeout = spec.timeout || my.cfg['MONGO_CONSISTENCY_TIMEOUT'];

  my.lock = fwk.lock({});
  my.objects = {};
  
  my.ctx = fwk.context({config: my.cfg});
  my.ctx.setTint('col:' + my.name + '-' + process.pid);
  
  var that = new events.EventEmitter();
  
  var target, check, remoteget, get, set, find, mapreduce;    

  target = function (id) {
    return targetfactory.build(my.name, id);
  };
  
  check = function() {    
    var expired = [];
    fwk.forEach(my.objects, function(obj, id) {
		  if(((new Date).getTime() - obj._lastRead) > my.timeout) {
		    expired.push(id);
		    my.ctx.log.debug('CHECK: expired id: ' + id);
		  }
		});    
    //my.ctx.log.debug('CHECK: expired len: ' + expired.length);
    for(var i = 0; i < expired.length; i ++) {
      (function(id) {
	 my.lock.wlock(
	   id,
	   function(unlock) {
	     delete my.objects[id];
	     my.ctx.log.debug('CHECK: deleted id: ' + id);
	     unlock();
	   });		
       })(expired[i]);
    }
  };
  
  remoteget = function(ctx, id) {
    //util.debug('REMOTEGET: ' + id);
    my.lock.wlock(
      id,
      function(unlock) {
	my.collection.find(
	  {'_cid': id}, 
	  function(err, cursor) {
	    if(err) { unlock(); ctx.error(err); return; }
	    cursor.toArray(
	      function(err, objs) {
		if(err) { unlock(); ctx.error(err); return; }
		if(objs.length > 1) {
		  unlock(); 
		  ctx.error('non unique target: ' + target(id)); 
		  //util.debug('REMOTEGET: OBJ LENGTH >1 for _cid: ' + id + ' ' + util.inspect(objs[0]));
		}
		else if(objs.length === 1) {		  
		  //util.debug('REMOTEGET: OBJ LENGTH 1 for _cid: ' + id + ' ' + util.inspect(objs[0]));
		  my.objects[id] = objs[0];
		  my.objects[id]._lastRead = new Date;
		  unlock();
		}
		else {
		  //util.debug('REMOTEGET: OBJ LENGTH 0 for _cid: ' + id);
		  my.objects[id] = {_lastWrite: new Date,
				    _lastRead: new Date,
				    _cid: id};
		  my.objects[id]._hash = fwk.makehash(my.objects[id]);
		  unlock();
		}
	      });
	  });	
      });
  };

  /** cb_(obj) */
  get = function(ctx, id, cb_) {
    if(!id) { ctx.error('unspecified id'); return; }
    
    if (!my.objects[id]) 
      remoteget(ctx, id);
    my.lock.rlock(
      id,
      function(unlock) {
	if(my.objects[id]) {
	  my.objects[id]._lastRead = new Date;
	  var res = fwk.shallow(my.objects[id]);
	  delete res._id;
	  ctx.log.debug('GET ' + target(id) + ': ' + my.objects[id]._hash /*+ ' ' + util.inspect(my.objects[id])*/);
	  unlock(); cb_(res);
	}
	else {
	  unlock();
	  ctx.error(new Error('Object disappeared - RACE alert ' + target(id)));
	}

      });
  };
  
  /** cb_(status) */  
  set = function(ctx, id, prevhash, obj, cb_) {
    if(id && obj && typeof obj === 'object') {      
      obj._cid = id; obj._hash = fwk.makehash(obj);
      obj._lastWrite = new Date;
      obj._lastRead = new Date;
      
      my.lock.wlock(
	id,
	function(unlock) {
	  if(!my.objects[id]) {
	    unlock();
	    ctx.error(new Error('Set before get on target: ' + target(id)));
	  }
	  else if(my.objects[id]._hash === obj._hash) {
	    ctx.log.debug('NOOP ' + target(id) + ': ' + my.objects[id]._hash);
	    unlock();
	    cb_(mongo.status.noop);
	  }
	  else if(my.objects[id]._hash !== prevhash) {
	    ctx.log.debug('RETRY ' + target(id) + ': ' + my.objects[id]._hash);
	    unlock();
	    cb_(mongo.status.retry);
	  }
	  else {
	    /** we conserve mongoDB ids */
	    if(my.objects[id]._id)
	      obj._id = my.objects[id]._id;
	    my.objects[id] = obj;	    
	    my.collection.save(
	      obj, 
	      {safe: true},
	      function(err, doc) {
		//ctx.log.debug('WRITE id: ' + id + ' err: ' + err + 
		//	      '\nobj: ' + util.inspect(obj) + 
		//	      '\ndoc: ' + util.inspect(doc));
		if(err) { unlock(); ctx.error(err); return; }			  
		ctx.log.debug('SUCCESS ' + target(id) + ': ' + obj._hash);

		unlock();
		/** we emit and update */
		that.emit('update', id, obj, obj._hash);
		cb_(mongo.status.success);
	      });	    
	  }
	});
    }
    else
      cb_(mongo.status.noop);
  };  

  /** cb_(result) */
  find = function(ctx, selector, cb_) {
    my.collection.find (
      selector,
      function(err, cursor) {
	if(err) { ctx.error(err); return; }
	//util.debug('cursor: ' + util.inspect(cursor) + '\n\ncount: ' + cursor.count());
	cursor.toArray(
	  function(err, objs) {
	    if(err) { ctx.error(err); return; }
	    cb_(objs);	    
	  });
      });	    
  };

  /** cb_(result) */
  mapreduce = function(ctx, map, reduce, options, cb_) {
    options = options || {};
    var tmpcol = my.name + '-mpr-' + ((new Date).getTime() % 10000000000);
    options.out = { replace: tmpcol };
    my.collection.mapReduce(
      map, reduce, options, 
      function(err, rescol) {
	if(err) { ctx.error(err); return; }
	rescol.find(
	  {},
	  function(err, cursor) {
	    if(err) { ctx.error(err); return; }
	    cursor.toArray(
	      function(err, objs) {		
		if(err) { ctx.error(err); return; }
		rescol.drop(function(err, reply) {
			      cb_(objs);
			    });
	      });
	  });       
      });
  };
  
  my.timer = setInterval(check, 500);
  
  fwk.method(that, 'get', get);
  fwk.method(that, 'set', set);
  fwk.method(that, 'find', find);
  fwk.method(that, 'mapreduce', mapreduce);

  return that;
};


