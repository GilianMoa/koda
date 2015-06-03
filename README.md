koda
====

Message Queue for Node.js and RabbitMQ


Producer
========

```
var k = require('koda')
  , mongodb = require('mongodb')

mongodb.MongoClient.connect('mongodb://localhost/test', function(err, db) {
  if (err || !db) {
    console.error('failed to connect to db', err, db);
    process.exit(1);
  }
  
  var koda = k.create(db);
  koda.on('ready', function() {
    console.log('koda ready');
    koda.enqueue('job_a', { blabla: true }, function(err, data) {
      console.log('enqueue job_a callback', err, data);
    });
    koda.enqueue('job_b', { oops: true }, { expires: 5000 /*milliseconds*/, ignoreResult: true }, function(err, data) {
      console.log('enqueue job_b callback', err, data);
    });
  });

  koda.on('error', function(err) {
    console.error('coda init failed', err, err.stack);
    process.exit(1);
  });


  function print_queue_stats() {
    koda.stats(function (err, result) {
      console.log(err, result);
      setTimeout(print_queue_stats, 5000) ;
    })
  }

  print_queue_stats()

});

```


Worker
======

```
var k = require('koda')
  , mongodb = require('mongodb')

mongodb.MongoClient.connect('mongodb://localhost/test', function(err, db) {
  if (err || !db) {
    console.error('failed to connect to db', err, db);
    process.exit(1);
  }
  
  var koda = k.create(db);

  koda.on('error', function (err) {
    console.error('queue has problems, check database', err)
    process.exit(1)
  })

  koda.on('ready', function () {

    // "requeues" jobs that were in assigned state in case of an unexpected restart
    koda.requeueStale(['job_a', 'job_b'], function (err, result) {
      
      function next_job_a() {

        // blocks while new job doesn't arrive
        koda.next('job_a',function(event, finish) {
          console.log('got job A event', event);
          finish(null); // marks jobs as completed (null) or failed (!null)
          next_job_a(); 
        }); 
      }

      function next_job_b() {

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

```
