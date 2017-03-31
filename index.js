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
}

var _request = function(config,_rev,next) {
  request.get({
    url:config.url,
    qs:{'gt':_rev,'limit':1},
    headers: {
     'authorization':'JWT '+config.jwtToken
    }
  }).pipe(JSONStream.parse('*'))
  .pipe(through2.obj(config.streamHandler,function(cb) {
    next();
    cb();
  })).pipe(through2.obj(function(chunk,enc,cb) {
    console.log('put',chunk.key);
    config.configDb.put('_rev',{'ts':chunk.key});
    cb();
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
      console.log(value);
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

