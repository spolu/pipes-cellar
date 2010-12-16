var util = require('util');
var events = require('events');
var fwk = require('fwk');
var mongodb = require('mongodb');
var cellar = require('cellar');

var cfg = require("./config.js");

/**
 * A MongoDB helper object.
 * 
 * Relies on callbacks to return objects or status of a set.
 * Errors are passed directly to the action (as a context) and
 * the callback is ignored.
 * 
 * @extends events.EventEmitter
 * 
 * @param spec {db, host, port, config}
 */
var mongo = function(spec, my) {
  my = my || {};
  var _super = {};
  
  my.cfg = spec.config || cfg.config;
  my.db = spec.db || my.cfg['MONGO_DB'];
  my.host = spec.host || my.cfg['MONGO_HOST'];
  my.port = spec.port || my.cfg['MONGO_PORT'];

  my.collection = {};
  
  my.lock = fwk.lock({});
  
  my.db = new mongodb.Db(my.db,
			 new mongodb.Server(my.host,
					    my.port, {}),
			 {native_parser:true});
  
  /** 
   * Events emitted:
   * 'update' : target, obj, hash
   */
  var that = new events.EventEmitter();
  
  var collection, get, set;
  
  /** cb_(collection) */
  collection = function(action, name, cb_) {
    my.lock.wlock(
      name, 
      function(unlock) {
	if(my.collection[name]) {		  
	  unlock(); cb_(my.collection[name]);
	}
	else {
	  my.db.collection(
	    name, 
	    function(err, c) { 
	      if(err) { unlock(); action.error(err); return; }
	      action.debug('opened collection: ' + name);	      
	      var col = collection({ c: c, name: name, config: my.cfg });
	      col.on('update', function(id, obj, hash) {
		       that.emit('update', name + '.' + id, obj, hash);
		     });
	      unlock(); cb_(col);
	    });		 
	}
      });
  };
  
  /** cb_(obj) */
  get = function(action, target, cb_) {
    var targ = cellar.target.match(target);
    if(targ) {
      collection(action, targ[1], function(c) {
		   c.get(action, targ[2], cb_);
		 });
    }
    else
      action.error('Invalid target: ' + target);
  };

  /** cb_(status) */
  set = function(action, target, prevhash, obj, cb_) {
    var targ = cellar.target.match(target);
    if(targ) {
      collection(action, targ[1], function(c) {
		   c.set(action, targ[2], prevhash, obj, cb_);
		 });
    }
    else
      action.error('Invalid target: ' + target);
  };
  
  that.method('get', get);
  that.method('set', set);

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
 * @param spec {c, name, interval, config}
 */
var collection = function(spec, my) {
  my = my || {};
  var _super = {};

  my.cfg = spec.config || cfg.config;
  my.c = spec.c;
  my.name = spec.name;
  my.interval = spec.interval || my.cfg['MONGO_WRITEBACK_INTERVAL'];

  my.lock = fwk.lock({});
  my.objects = {};
  
  my.ctx = fwk.context({});
  
  var that = new events.EventEmitter();
  
  var target, writeback, remoteget, get, set;    

  target = function (id) {
    return cellar.target.build(my.name, id);
  };
  
  writeback = function() {
    for(var id in my.objects) {
      if(my.objects.hasOwnProperty(id) && my.objects[id]._dirty) {
	my.lock.wlock(
	  id, 
	  function(unlock) {
	    /** should still be dirty */				       
            if(!my.objects[id]._dirty) { unlock(); return; }
	    var obj = my.objects[id].shallow();
            delete obj._dirty;
            my.c.save(
	      obj, 
	      {upsert: true, safe: true},
              function(err, doc) {
		if(err) { unlock(); my.ctx.log.error(err); return; }			  
		/** so that we catch-up mongoDb _id */
                my.objects[id] = doc;
                my.objects[id]._dirty = false;
		unlock();
                my.ctx.log.debug('WRITEBACK ' + target(id) + my.objects[id]._hash);
              });
          });
      }
    }
  };
  
  remoteget = function(action, id) {
    my.lock.wlock(
      id,
      function(unlock) {
	my.c.find(
	  {'_cid': id}, 
	  function(err, cursor) {
	    if(err) { unlock(); action.error(err); return; }
	    cursor.toArray(
	      function(err, objs) {
		if(err) { unlock(); action.error(err); return; }
		if(objs.length > 1) {
		  unlock(); 
		  action.error('non unique target: ' + target(id)); 
		}
		else if(objs.length === 1) {
		  my.objects[id] = objs[0];
		  my.objects[id]._dirty = false;
		  unlock();
		}
		else {
		  my.objects[id] = {_lastWrite: new Date,
				    _dirty: true,
				    _cid: id};
                  my.objects[id]._hash = my.objects[id].hash('_cid');
		  unlock();
		}
	      });
	  });	
      });
  };

  /** cb_(obj) */
  get = function(action, id, cb_) {
    if(!id) { action.error('unspecified id'); return; }
    
    if (!my.objects[id]) 
      remoteget(action, id);
    my.lock.rlock(
      id,
      function(unlock) {
	if(my.objects[id]) {
	  var res = my.objects[id].shallow();
	  delete res._dirty;
	  delete res._id;
	  action.log.debug('GET ' + target(id) + ': ' + res._hash);
	  unlock(); cb_(res);
	}
      });
  };
  
  /** cb_(status) */  
  set = function(action, id, prevhash, obj, cb_) {
    if(id && obj && typeof obj === 'object') {      
      obj._cid = id; obj._hash = obj.hash('cid');
      obj._lastWrite = new Date; obj._dirty = true;
      
      my.lock.wlock(
	id,
	function(unlock) {
	  if(!my.objects[id]) {
	    unlock();
	    action.error('set before get on target: ' + target(id));
	  }
	  else if(my.objects[id]._hash === obj._hash) {
	    action.log.debug('NOOP ' + target(id) + ': ' + my.objects[id]._hash);
	    unlock();
	    cb_(mongo.status.noop);
	  }
	  else if(my.objects[id]._hash !== prevhash) {
	    action.log.debug('RETRY ' + target(id) + ': ' + my.objects[id]._hash);
	    unlock();
	    cb_(mongo.status.retry);
	  }
	  else {
	    /** we conserve mongoDB ids */
	    if(my.objects[id]._id)
              obj._id = my.objects[id]._id;
	    my.objects[id] = obj;	    
	    action.log.debug('SUCCESS ' + target(id) + ': ' + my.objects[id]._hash);
	    unlock();

	    /** we emit and update */
	    that.emit('update', id, obj, obj._hash);

	    cb_(mongo.status.success);
	  }
	});
    }
    else
      cb_(mongo.status.noop);
  };  

  
  my.timer = setInterval(writeback, my.interval);

  
  that.method('get', get);
  that.method('set', set);

  return that;
};


