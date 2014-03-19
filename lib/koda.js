var events = require('events')
  , util = require('util')
  , uuid = require('node-uuid')

function Koda(mq, namespace) {
  events.EventEmitter.call(this);
  this.mq = mq;
  this.namespace = namespace
}
util.inherits(Koda, events.EventEmitter);

function KodaServer(mq, namespace) {
  Koda.call(this, mq, namespace);
  var self = this;
  self.mq.queue(self.namespace, { }, function(q) {
    self.queue = q;
    self.queue.bind('#');
    self.queue.subscribe({ ack: true }, function(message, headers, deliveryInfo) {
      //console.log('got request', message, headers, deliveryInfo);
      if (message.type && message.data) {
        function _respond(err, data) {
          //console.log('_respond', err, data);
          if (deliveryInfo && deliveryInfo.replyTo && deliveryInfo.correlationId) {
            //console.log('sending response', deliveryInfo.replyTo, { result: !err, error: err, data: data });
            self.mq.publish(deliveryInfo.replyTo, { result: !err, error: err, data: data }, {
              correlationId: deliveryInfo.correlationId
            });
          }
          self.queue.shift();
        }
        if (self.listeners(message.type).length) {
          self.emit(message.type, message.data, _respond);
        } else {
          _respond();
        }
      }
    });
  });
}
util.inherits(KodaServer, Koda);

function KodaClient(mq, namespace) {
  Koda.call(this, mq, namespace);
  var self = this;
  self.queueId = uuid.v4();
  self.pending = {};
  self.mq.queue(self.queueId, { exclusive: true }, function(q) {
    self.queue = q;
    self.queue.bind('#');
    self.queue.subscribe(function(message, headers, deliveryInfo) {
      //console.log('got response', message, headers, deliveryInfo);
      if (deliveryInfo && deliveryInfo.correlationId) {
        var cb = self.pending[deliveryInfo.correlationId];
        delete self.pending[deliveryInfo.correlationId];
        cb && cb(message.result ? null : message.error, message.data);
      }
    });
  });
}
util.inherits(KodaClient, Koda);
KodaClient.prototype.send = function(type, data, cb) {
  var opts = {};
  if (cb) {
    opts.correlationId = uuid.v4();
    opts.replyTo = this.queueId;
    this.pending[opts.correlationId] = cb;
  }
  this.mq.publish(this.namespace, {
    type: type,
    data: data
  }, opts);
}

module.exports.Server = KodaServer;
module.exports.Client = KodaClient;
