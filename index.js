var through2 = require('through2');
var levelup = require('levelup');
var sublevel = require('level-sublevel');
var path = require('path');
var diff = require('changeset');

var logview = function(config) {
  var db = sublevel(levelup(config.db.path,{'valueEncoding':'json'}));
  var configDb = db.sublevel('_config'); 

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


module.exports = logview;
