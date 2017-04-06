var connect = require('connect');
var through2 = require('through2');
var config = require('./config');
var logview = require('..');
var http = require('http');
var bodyParser = require('body-parser');
var app = connect();

var myStream = require('./myStream');

var streamHandler = myStream.createStreamHandlers(config);

logview.config({
  'url':'https://maas.nuqlis.com:9000/api/log/attendance',
  'jwtToken':config.token,
  'mainDb':myStream.db,
  'streamHandler': streamHandler,
});

app.use(bodyParser.json());
app.use(logview.handle_match);
app.use(logview.monitor);
app.use(logview.serve);

http.createServer(app).listen(3000);

