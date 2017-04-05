var through2 = require('through2');
var levelup = require('levelup');
var sublevel = require('level-sublevel');
var path = require('path');
var diff = require('changeset');
var request = require('request');
var bytewise = require('bytewise');
var JSONStream = require('JSONStream');
var locks = require('locks');

var logview = exports;

var _revSize = 100;

var mutex = locks.createMutex();

logview.config = function(config) {
  logview.config = config;
  var dbPath = config.dbPath?config.dbPath:'_view'; 
  logview.config.db = sublevel(levelup(dbPath,{'valueEncoding':'json'}));
  logview.config.configDb = logview.config.db.sublevel('_config'); 
  logview.config.mirrorDb = logview.config.db.sublevel('_mirror'); 
}

var _updateMirror = function(self,db,chunk,cb) {
  var value = diff.apply(chunk.value.changes,{});
  db.get(chunk.value.key,function(err,obj) { 
    var _pushObj = { 
      value:value,
      _rev:chunk.key,
      key:chunk.value.key
    };
    if(!err) { 
      _pushObj['_value'] = obj;
    }
    db.put(_pushObj.key,_pushObj.value,function(err) {
      if(!err) {
        self.push(_pushObj);
      }
      cb();
    });
  });
}

var _request = function(config,_rev,callback) {
  var stream = request.get({
    url:config.url,
    qs:{'gt':_rev,'limit':_revSize},
    headers: {
     'authorization':'JWT '+config.jwtToken
    }
  }).pipe(JSONStream.parse('*'))
  .pipe(through2.obj(function(chunk,enc,cb) {
    _updateMirror(this,config.mirrorDb,chunk,cb);
  }));

  var handlers = config.streamHandler();

  for(var i=0;i<handlers.length;i++) {
    stream = stream.pipe(handlers[i]());
  }

  stream.pipe(through2.obj(function(chunk,enc,cb) {
   // console.log('put',chunk._rev);
    config.configDb.put('_rev',{'ts':chunk._rev});
    cb();
  })).on('finish',function() {
   // console.log('finish');
    callback();
  });
};

logview.monitor = function(req,res,next) {
  // get current rev
  var config = logview.config;
  var configDb = config.configDb;
  mutex.lock(function() {
    console.log('process');
    configDb.get('_rev',function(err,value) {
      if(err) {
        _request(config,'0',function() {
          mutex.unlock();
        });
      } else {
        _request(config,value.ts,function() {
          mutex.unlock();
        });
      }
    });
  });
  console.log('next');
  next();
}

var getOpts = function(opts) {
  if(opts.limit) opts.limit = Number(opts.limit);
  if(opts.start) opts.start = bytewise.encode(opts.start,'hex');
  if(opts.end) opts.end = bytewise.encode(opts.end,'hex');
  return opts;
}

logview.serve = function(req,res,next) {
  if(req.method == "POST") {
    var db = logview.config.mainDb;
    res.setHeader('content-type','application/json');
    db.createReadStream(getOpts(req.body))
    .pipe(through2.obj(function(chunk,enc,cb) {
      this.push({
        'key':bytewise.decode(chunk.key,'hex'),
        'value':chunk.value
      });
      cb();
    }))
    .pipe(JSONStream.stringify())
    .pipe(res);
  } else {
    res.setHeader('content-type','application/json');
    logview.config.configDb.get('_rev',function(err,value) {
      if(!err) {
        res.end(JSON.stringify(value));
      } else {
        res.end(JSON.stringify({'ts':0}));
      }
    });
  }
};
