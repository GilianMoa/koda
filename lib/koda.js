var events = require('events')
  , os = require('os')
  , util = require('util')
  , mongodb = require('mongodb')
  , V = require('vubsub')
  , Q = require('q')

function Koda(db, options) {
  events.EventEmitter.call(this);
  var self = this;
  self.db = db;
  self.options = options || {};
  self.ns = self.options.ns || 'kodajs';
  self.messages = db.collection(self.ns + '.messages');
  self.clientId = self.options.clientId || os.hostname();
  self._messageLogs={};
  Q.all([
    Q.ninvoke(self.messages, 'ensureIndex', { expires: 1 }, { expireAfterSeconds: 0 }),
    Q.ninvoke(self.messages, 'ensureIndex', { type: 1, state: 1 }),
    Q.ninvoke(self.messages, 'ensureIndex', { state: 1, creation_ts: 1 })
  ]).then(function() {
    return V.create(db, { ns: self.ns });
  }).then(function(client) {
    self.client = client;
    return self.client.channel('default');
  }).then(function(channel) {
    self.channel = channel;
    self.channel.on('data', function(data) {
      self._handleMessage(data);
    });
    self.channel.once('ready', function() {
      console.log('ready');
      self._ready=true;
      self.emit('ready');
    });
  }).fail(function(err) {
    self.emit('error', err);
  });
}
util.inherits(Koda, events.EventEmitter);
Koda.DEFAULT_EXPIRATION = 60 * 60 * 1000; // one hour
Koda.prototype.onReady= function (cb) {
  var self = this;
  if (self._ready) {
    cb();
  } else {
    this.on('ready', cb);
  }
}
Koda.prototype._handleMessage = function(message) {
  if (!message || !message.type || !message.data || !message.data.message_id) return false;
  var self = this;

  // We need to keep track of messages that may be missed while the 'next'
  // call can't attach an event handler to the 'once' event
  self._messageLogPush(message);

  if (message.type == 'kresult') {
    self.emit(message.data.message_id.toString(), !message.data.result);
    return true;
  }
  
  var _h = this.listeners(message.type);
  if (!_h || !_h.length) return false;

  // tell clients that there's a new job and they can fetch it.
  self.emit(message.type);
  
  return true;
}
// keeps record of messages while 'next' executes async code and has no event
// handler attached
Koda.prototype._newMessageLog = function(type) {
  var self = this;
  var log = [];
  if (!self._messageLogs[type])
    self._messageLogs[type]=[];
  self._messageLogs[type].push(log);
  return log;
}
Koda.prototype._messageLogPush = function(message) {
  var self = this;
  if(self._messageLogs[message.type] && self._messageLogs[message.type].length) {
    for(var i = 0; i < self._messageLogs[message.type].length; i++) {
      self._messageLogs[message.type][i].push(message);
    }
  }
}
Koda.prototype._closeMessageLog = function(type, log) {
  var self = this;
  if (!self._messageLogs[type])
    self._messageLogs[type]=[];
  var i = self._messageLogs[type].indexOf(log);
  if (i>-1) {
    self._messageLogs[type].splice(i,1);
  }
}
Koda.prototype.next = function(type, cb) {
  if (!type || !cb || typeof cb !== 'function') throw new Error('invalid aguments');
  var self = this;
  // collect messages that may arrive between the database fetch and its call back
  var log = self._newMessageLog(type);
  self.messages.findAndModify({
    type: type,
    state: 'pending'
  }, {
    _id: 1
  }, {
    $set: {
      state: 'assigned',
      assigned_to: self.clientId,
      assigned_ts: new Date()
    }
  }, {
    'new': true
  }, function(err, data) {
       // Stop collecting messages. While inside this function the message
       // handler can't be called because there isn't async code here. It is
       // safe to analyze the log at this point because no messages are
       // "arriving".
       self._closeMessageLog(type, log);

       // Carries on even if there's an error because we need to attach the
       // event handler or fetch recently arrived messages.
       if (err) {
         self.emit('error', err);
       }

       if (data) {
         // found a job to process

         // 'finish' callback passed to 'cb' to close the job
         var finish = function(err) {
           self.messages.update({
             _id: data._id
           }, {
             $set: {
               state: (err ? 'failed' : 'completed'),
               err: err,
               finished_ts: new Date()
             }
           }, { w: 0 });
           self.channel.send('kresult', { message_id: data._id, result: !err });
         };

         cb(data, finish);

       } else {
         if (log.length) {
           // Some message arrived between the call to the db and this callback,
           // go to fetch it.
           self.next(type, cb);
         } else {
           // No jobs found, attach event handler to know when a new job arrived
           self.once(type, function () { self.next(type, cb); });
         }
       }
     });
}
Koda.prototype.enqueue = function(type, data, options, cb) {
  if (typeof data === 'function') {
    cb = data;
    options = null;
    data = null;
  }
  if (typeof options === 'function') {
    cb = options;
    options = null;
  }
  if (!type || !cb) {
    throw new Error('invalid arguments');
  }
  options = options || {};
  data = data || {};
  var now = new Date();
  var self = this;
  var id = null;
  Q.ninvoke(self.messages, 'insert', {
    type: type,
    data: data,
    state: 'pending',
    created_by: self.clientId,
    creation_ts: new Date(),
    expires: new Date(+now + (options.expires || Koda.DEFAULT_EXPIRATION))
  }, { w: 1 }).then(function(docs) {
    if (!util.isArray(docs) || docs.length != 1) {
      throw new Error('failed to create message');
    }
    id = docs[0]._id;
    return self.channel.send(type, { message_id: id });
  }).then(function() {
    if (!options.ignoreResult) {
      self.once(id.toString(), cb);
    } else {
      cb && cb();
    }
  }).fail(function(err) {
    cb && cb(err);
  });
}
Koda.prototype.requeueStale = function (types, options, cb) {

  var self = this;

  if (!cb) {
    cb=options;
    options={};
  }
  options.timeout = options.timeout || 0;
  options.attempts = options.attempts || 5;
  options.states = options.states || [ "assigned" ];

  if (options.dryRun) {
    cb && cb(null, { maxRequeue: [], requeued: [], errors: [] })
    return;
  }

  var ts = new Date()

  ts.setSeconds(ts.getSeconds()-options.timeout)

  var query = {
    type: {$in: types},
    state: { $in: options.states },
    creation_ts: { $lt: ts }
  };

  // undefined -> clientId defined at creation
  // null -> all
  // !null -> custom clientId
  if (options.clientId === undefined) {
    query.assigned_to = self.clientId;
  } else if (options.clientId !== null) {
    query.assigned_to = options.clientId;
  }

  Q.ninvoke(self.messages, 'find', query).then(function (cursor) {
    return Q.ninvoke(cursor, 'toArray')
  }).then(function (stales){

    var maxRequeue = [];
    var requeued = [];
    var errors = [];

    function next() {
      if (!stales.length) {
        cb && cb(null, { maxRequeue: maxRequeue, requeued: requeued, errors: errors })
      } else {
        var item = stales.pop()
        if (item.requeued < options.attempts) {
          self.messages.update({
            _id: item._id
          }, {
            $set: {
              state: 'pending',
              requeued_ts: new Date()
            },
            $inc: {
              requeued: 1
            },
            $unset: {
              assigned_to: 1,
              assigned_ts: 1
            }
          }, {
            w: 1
          }, function (err) {                             
            if (!err) {
              self.channel.send(item.type, { message_id: item._id });
              requeued.push(item);
              next()
            } else {
              self.channel.send('kresult', { message_id: item._id, result: false });
              errors.push({err:err, job:item});
              next();
            }
          });
        } else {
          self.messages.update({
            _id: item._id
          }, {
            $set: {
              state: 'failed',
              err: 'maxrequeue',
              finished_ts: new Date()
            }
          }, { w: 0 });
          self.channel.send('kresult', { message_id: item._id, result: false });
          maxRequeue.push(item);
          next()
        }  
         
      }
    }

    next()
    
  }).fail(function(err){
    cb(err)
  })
  
}

Koda.prototype.stats = function (states, cb) {
  var self = this
  if (!cb) {
    cb = states
    states = null
  }
  var aggr = [{
    $project: {
      type:1,
      state: 1,
      creation_ts: 1,
      finished_ts: 1,
      duration: { $subtract: [ { $ifNull: [ "$finished_ts", new Date() ] }, "$creation_ts" ] }
    }
  },{
    $group: {
      _id: {
        type: "$type",
        state: "$state"
      },
      count: { $sum: 1 },
      oldest_ts: { $min: "$creation_ts"},
      newest_ts: { $max: "$creation_ts"},
      last_finished: { $max: "$finished_ts"},
      min_duration: { $min: "$duration" },
      max_duration: { $max: "$duration" },
      avg_duration: { $avg: "$duration" }
    }              
  }, {
    $project: {
      _id: 0,
      type: "$_id.type",
      state: "$_id.state",
      count: 1,
      oldest_ts: 1,
      newest_ts: 1,
      last_finished: 1,
      min_duration: 1,
      max_duration: 1,
      avg_duration: 1
    }
  }, {
    $sort: {
      state: 1,
      type: 1
    }
  }];

  if (states) {
    aggr.unshift({
      $match: {
        state: { $in: states }
      }
    })
  }

  self.messages.aggregate(aggr, cb)
}

exports.create = function(db, options) {
  return new Koda(db, options);
}


