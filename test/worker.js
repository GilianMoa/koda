var k = require('../lib/koda.js')
  , mongodb = require('mongodb')

mongodb.MongoClient.connect('mongodb://localhost/test', function(err, db) {

  if (err || !db) {
    console.error('failed to connect to db', err, db);
    process.exit(1);
  }
  
  var koda = k.create(db);

  koda.onReady(function () {

    console.log('ready');

    koda.on('error', function (err) {
      console.error('queue has problems, check database', err)
      process.exit(1)
    })

    // "requeues" jobs that were in assigned state in case of restart due to crashing
    koda.requeueStale(['job_a', 'job_b'], function (err, result) {
      
      console.log('requeueStale');

      function next_job_a() {

        console.log('next_job_a');

        // blocks while new job doesn't arrive
        koda.next('job_a',function(event, finish) {
          console.log('got job A event', event);
          finish(null); // marks jobs as completed (null) or failed (!null)
          next_job_a(); 
        }); 
      }

      function next_job_b() {

        console.log('next_job_b');

        // blocks while new job doesn't arrive
        koda.next('job_b',function(event, finish) {
          console.log('got job B event', event);
          finish(null);
          next_job_b(); 
        }); 
      }

      next_job_a();
      next_job_b();

    })

  })

});
