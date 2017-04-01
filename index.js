var through2 = require('through2');
var levelup = require('levelup');
var sublevel = require('level-sublevel');
var path = require('path');
var diff = require('changeset');
var request = require('request');
var bytewise = require('bytewise');
var JSONStream = require('JSONStream');

var logview = exports;

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
    if(!err) { 
      var changes = diff(obj,value);
      self.push({value:value,
        _rev:chunk.key,
        key:chunk.value.key,
        _value:obj
      });
      cb();
    } else {
      var obj = {'key':chunk.value.key,'value':value};
      db.put(obj.key,obj.value,function(err) {
        if(!err) {
          self.push({value:value,
           _rev:chunk.key,
           key:chunk.value.key});
        }
        cb();
      });
    }
  });
}

var _request = function(config,_rev,next) {
  var stream = request.get({
    url:config.url,
    qs:{'gt':_rev,'limit':100},
    headers: {
     'authorization':'JWT '+config.jwtToken
    }
  }).pipe(JSONStream.parse('*'))
  .pipe(through2.obj(function(chunk,enc,cb) {
    _updateMirror(this,config.mirrorDb,chunk,cb);
  }));

  var handlers = config.streamHandler(next);

  for(var i=0;i<handlers.length;i++) {
    stream = stream.pipe(handlers[i]());
  }

  stream.pipe(through2.obj(function(chunk,enc,cb) {
    console.log('put',chunk._rev);
    config.configDb.put('_rev',{'ts':chunk._rev});
    cb();
  })).on('finish',function() {
    console.log('finish');
    next();
  });
};

logview.monitor = function(req,res,next) {
  // get current rev
  var config = logview.config;
  var configDb = config.configDb;
  configDb.get('_rev',function(err,value) {
    if(err) {
      configDb.put('_rev',{'ts':0},function(err) {
        _request(config,0,next);
      });
    } else {
      _request(config,value.ts,next);
    }
  });
}

var getOpts = function(opts) {
  if(opts.limit) opts.limit = Number(opts.limit);
  if(opts.start) opts.start = bytewise.encode(opts.start,'hex');
  if(opts.end) opts.end = bytewise.encode(opts.end,'hex');
  return opts;
}

logview.serve = function(req,res,next) {
  if(req.method == "POST") {
    console.log('serve');
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
