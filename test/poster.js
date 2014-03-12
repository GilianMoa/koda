var k = require('../lib/koda.js')
  , mongodb = require('mongodb')

mongodb.MongoClient.connect('mongodb://localhost/test', function(err, db) {
  if (err || !db) {
    console.error('failed to connect to db', err, db);
    process.exit(1);
  }
  
  var koda = k.create(db);
  koda.on('ready', function() {
    console.log('koda ready');
    koda.enqueue('test', { blabla: true }, function(err, data) {
      console.log('enqueue callback', err, data);
      process.exit(0);
    });
  });
  koda.on('error', function(err) {
    console.error('coda init failed', err, err.stack);
    process.exit(1);
  });
});
