var through2 = require('through2');
var levelup = require('levelup');
var sublevel = require('level-sublevel');
var path = require('path');
var diff = require('changeset');
var request = require('request');
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
  var updateMirror = through2.obj(function(chunk,enc,cb) {
    _updateMirror(this,config.mirrorDb,chunk,cb);
  });

  request.get({
    url:config.url,
    qs:{'gt':_rev,'limit':1},
    headers: {
     'authorization':'JWT '+config.jwtToken
    }
  }).pipe(JSONStream.parse('*'))
  .pipe(updateMirror);

  var handlers = config.streamHandler(next);
  console.log('handlers',handlers.length);
  updateMirror.pipe(handlers[0]);

  for(var i=0;i<handlers.length-1;i++) {
    var next = handlers[i+1];
    handlers[i].pipe(next);
  }

  handlers[handlers.length-1].pipe(through2.obj(function(chunk,enc,cb) {
    console.log('put',chunk._rev);
    config.configDb.put('_rev',{'ts':chunk._rev});
  }));
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



logview.initial = function(config) {

  var get = function(db,key) {
    return new Promise(function(fulfill,reject) {
      db.get(key,function(err,obj) {
        if(err) reject(err);
        else fullfill(obj);
      });
    });
  };

  var update_config = function(chunk) {
    return new Promise(function(fulfill,reject) {
      configDb.put('_config',{'revision':chunk.key},function(err) {
        if(!err) fulfill(chunk.key);
      });
    });
  }

  var jsonStream = through2.obj(function(chunk,encoding,cb) {
    var self = this;
    update_config(chunk);
    db.get(chunk.value.key,function(err,obj) {
      if(!err) { 
        var value = diff.apply(chunk.value.changes,{});
        var changes = diff(obj,value);
       // if(changes.length!=0) {
          self.push({
            event:'update',
            value:value,
            rev:chunk.key,
            key:chunk.value.key,
            _value:obj
          });
       // }
        cb();
      } else {
        var value = diff.apply(chunk.value.changes,{});
        var obj = {'key':chunk.value.key,'value':value};
        db.put(obj.key,obj.value,function(err) {
          if(!err) {
            self.push({event:'insert',value:value,rev:chunk.key,key:chunk.value.key});
          }
          cb();
        });
      }
    });
  });
  return jsonStream;
};

