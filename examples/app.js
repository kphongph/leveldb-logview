var connect = require('connect');
var through2 = require('through2');
var config = require('./config');
var logview = require('..');
var http = require('http');
var app = connect();
var myStream = require('./myStream');

logview.config({
  'url':'https://maas.nuqlis.com:9000/api/log/attendance',
  'jwtToken':config.token,
  'streamHandler': myStream(config),
});

app.use(logview.monitor);

app.use(function(req,res) {
  res.end('Hello for Connect!\n');
});

http.createServer(app).listen(3000);

