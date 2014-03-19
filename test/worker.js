var k = require('../lib/koda.js')
  , amqp = require('amqp')

var connection = amqp.createConnection({ host: 'localhost' });

connection.on('ready', function () {
  var server = new k.Server(connection, 'my-namespace');
  server.on('my-message', function(request, cb) {
    console.log('received request', request);
    cb(null, { foo: 'bar', x: 1, y: 2 });
  });
});
