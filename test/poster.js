var k = require('../lib/koda.js')
  , amqp = require('amqp')

var connection = amqp.createConnection({ host: 'localhost' });

connection.once('ready', function () {
  var client = new k.Client(connection, 'my-namespace');
  client.send('my-message', { some_data: true }, function(err, response) {
    if (err) {
      console.error('request failed', err);
    } else {
      console.log('request succeeded', response);
    }
    process.exit(0);
  });
});
connection.on('error', function(err) {
  console.error('error', err);
});
