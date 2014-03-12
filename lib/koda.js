var events = require('events')
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

  Q.all([
    Q.ninvoke(self.messages, 'ensureIndex', { expires: 1 }, { expireAfterSeconds: 0 }),
    Q.ninvoke(self.messages, 'ensureIndex', { type: 1, state: 1 })
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
      self.emit('ready');
    });
  }).fail(function(err) {
    self.emit('error', err);
  });
}
util.inherits(Koda, events.EventEmitter);
Koda.DEFAULT_EXPIRATION = 60 * 60 * 1000; // one hour
Koda.prototype._handleMessage = function(message) {
  if (!message || !message.type || !message.data || !message.data.message_id) return false;
  var _h = this.listeners(message.type);
  if (!_h || !_h.length) return false;

  var self = this;
  self.messages.findAndModify({
    _id: message.data.message_id,
    state: 'pending'
  }, {
    _id: 1
  }, {
    $set: {
      state: 'assigned',
      assigned_ts: new Date()
    }
  }, {
    'new': true
  }, function(err, data) {
    if (err) {
      console.error('query failed', err);
    } else if (data) {
      self.emit(message.type, data);
    }
  });
  return true;
}
Koda.prototype.next = function(type, cb) {
  if (!type || !cb || typeof cb !== 'function') throw new Error('invalid aguments');
  var self = this;
  self.messages.findAndModify({
    type: type,
    state: 'pending'
  }, {
    _id: 1
  }, {
    $set: {
      state: 'assigned',
      assigned_ts: new Date()
    }
  }, {
    'new': true
  }, function(err, data) {
    if (err) {
      self.emit('error', err);
      cb();
    } else if (data) {
      cb(data);
    } else {
      self.once(type, cb);
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
  Q.ninvoke(self.messages, 'insert', {
    type: type,
    data: data,
    state: 'pending',
    creation_ts: new Date(),
    expires: options.expires || new Date(+now + Koda.DEFAULT_EXPIRATION)
  }, { w: 1 }).then(function(docs) {
    if (util.isArray(docs) && docs.length == 1) {
      return self.channel.send(type, { message_id: docs[0]._id });
    } else {
      return null;
    }
  }).then(function() {
    cb();
  }).fail(function(err) {
    cb(err);
  });
}

exports.create = function(db, options) {
  return new Koda(db, options);
}