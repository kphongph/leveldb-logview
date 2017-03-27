var through2 = require('through2');
var levelup = require('levelup');
var diff = require('changeset');

var logview = function(dbPath) {
  var db = levelup(dbPath,{'valueEncoding':'json'});
  var jsonStream = through2.obj(function(chunk,encoding,cb) {
    db.get(chunk.value.key,function(err,obj) {
      if(!err) { 
        var value = diff.apply(chunk.value.changes,{});
        var changes = diff(obj,value);
        if(changes.length!=0) {
          jsonStream.emit('update',value,changes);
        }
        cb();
      } else {
        console.log(chunk.key);
        var value = diff.apply(chunk.value.changes,{});
        var obj = {'key':chunk.value.key,'value':value};
        db.put(obj.key,obj.value,function(err) {
          if(!err) jsonStream.emit('insert',obj);
          cb();
        });
      }
    });
  });
  return jsonStream;
};


module.exports = logview;
