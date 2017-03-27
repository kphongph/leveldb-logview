var request = require('request');
var JSONStream = require('JSONStream');
var logview = require('..');
var config = require('./config');

var stream = logview({
  db:{path:'./test'}
});

stream.on('insert',function(d) {
 console.log(d.value.CODE);
});

stream.on('update',function(d) {
 console.log(d.value.CODE);
});


request.get({
  url:'http://maas.nuqlis.com:44300/log/dmc59_1',
  qs:{'start':'1490593619426','limit':'1'},
  headers: {
    'authorization':'JWT '+config.token
  }})
.pipe(JSONStream.parse('*'))
.pipe(stream);
