var k = require('../lib/koda.js')
  , mongodb = require('mongodb')
  , util = require('util')

mongodb.MongoClient.connect('mongodb://localhost/test', function(err, db) {
  if (err || !db) {
    console.error('failed to connect to db', err, db);
    process.exit(1);
  }
  
  var koda = k.create(db);
  koda.on('error', function(err) {
    console.error('coda init failed', err, err.stack);
    process.exit(1);
  });
  //console.log(util.inspect(koda, true, null, true))
  koda.onReady(function() {
    console.log('koda ready');
    koda.enqueue('job_a', { blabla: true }, function(err, data) {
      console.log('enqueue job_a callback', err, data);
    });
    koda.enqueue('job_b', { oops: true }, { expires: 5000 /*milliseconds*/, ignoreResult: true }, function(err, data) {
      console.log('enqueue job_b callback', err, data);
    });
    function print_queue_stats() {
      koda.stats(function (err, result) {
        console.log(err, result);
        setTimeout(print_queue_stats, 5000) ;
      })
    }

    print_queue_stats()

  });

});
