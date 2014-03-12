var k = require('../lib/koda.js')
  , mongodb = require('mongodb')

mongodb.MongoClient.connect('mongodb://localhost/test', function(err, db) {
  if (err || !db) {
    console.error('failed to connect to db', err, db);
    process.exit(1);
  }
  
  var koda = k.create(db);
  var handler = function(event) {
    console.log('got test event', event);
    setTimeout(function() {
      koda.next('test', handler);
    }, 5000)
  }
  koda.next('test', handler);
});
