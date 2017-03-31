var request = require('request');
var JSONStream = require('JSONStream');
var logview = require('..');
var config = require('./config');
var through2 = require('through2');
var levelup = require('levelup');
var sublevel = require('level-sublevel');
var bytewise = require('bytewise');

var stream = logview({
  db:{path:'./test'}
});

var viewdb = sublevel(levelup('./view',{'valueEncoding':'json'}));
var teachdb = viewdb.sublevel('teach');
var schoolTimeDb = viewdb.sublevel('schooltime');
var studentDb = viewdb.sublevel('student');

var azureConfig = {
  domain: "https://newtestnew.azurewebsites.net",
}

var get_course = function(obj) {
  var attendance  = obj;
  return new Promise(function(fullfill,reject) {
    var key = bytewise.encode([attendance.hostid,attendance.year,attendance.staffid]);
    teachdb.get(key,function(err,value) {
      if(err) {
        request({
          method:'GET',
          url:azureConfig.domain+'/ServiceControl/GetEduService.svc/getCourseFromStaff',
          qs:{'hostID':attendance.hostid,'staffID':attendance.staffid,'year':attendance.year}
        },function(err,response,body) {
          var _obj = JSON.parse(body);
          teachdb.put(key,_obj);
          fullfill({'attendance':obj,'courses':_obj});
        });
      } else {
        fullfill({'attendance':obj,'courses':value});
      }
    });
  });
}

var find_schooltime = function(obj) {
  var attendance  = obj.attendance;
  
  var get_level = function(attendance) {
    var _type = {'มัธยมศึกษาปีที่':9,'ประถมศึกษาปีที่':3};
    var _label = attendance['class'];
    var _value = _label.split(' ');
    var level = _type[_value[0]]+Number(_value[1]);
    return _type[_value[0]]+Number(_value[1]);
  }

  return new Promise(function(fullfill,reject) {
    var filtered = obj.courses.filter(function(course) {
      return Number(course.Semester) == attendance.semester && 
          Number(course.EducationClass) == get_level(attendance) &&
          course.Room == attendance.room &&
          course.SubjectName == attendance.subject;
    });
    if(filtered.length == 1) {
      fullfill({'attendance':attendance,'schoolTime':filtered[0].SchoolTimeID});
    } else {
      reject('Not Found SchoolTime', obj.attendance);
    }
  });
}

var get_schooltime = function(obj) {
  var attendance = obj.attendance;
  var schoolTime = obj.schoolTime;
  return new Promise(function(fullfill,reject) {
    var key = bytewise.encode([attendance.hostid,attendance.year,schoolTime]);
    schoolTimeDb.get(key,function(err,value) {
      if(err) {
        request({
          method:'GET',
          url:azureConfig.domain+'/ServiceControl/GetEduService.svc/getCoursePerson',
          qs:{'hostid':attendance.hostid,'course':schoolTime,'Years':attendance.year}
        },function(err,response,body) {
          var _obj = JSON.parse(body);
          schoolTimeDb.put(key,_obj);
          fullfill({'attendance':attendance,'students':_obj});
        });
      } else {
        fullfill({'attendance':attendance,'students':value});
      }
    });
  });
}

var reverse = through2.obj(function(chunk,enc,cb) {
  var self = this;
  if(chunk._value) {
    get_course(chunk._value).then(find_schooltime).then(get_schooltime).then(function(obj) {
      var attendance = obj.attendance;
      var students = obj.students.map(function(_obj) {
        return _obj.cid;
      });
      attendance.data.forEach(function(slot) {
        var absentList = slot.student.map(function(_obj) {
          return _obj.cid;
        });
        var presentList = students.filter(function(cid) {
          return absentList.indexOf(cid) < 0;
        });
        absentList.forEach(function(cid) {
          self.push({'attendance':attendance,'student':cid,
            'present':0,'absent':-1,'total':-1});
        });
        presentList.forEach(function(cid) {
          self.push({'attendance':attendance,'student':cid,
            'present':-1,'absent':0,'total':-1});
        });
      });
      self.push({'chunk':chunk});
      cb();
    }).catch(function(err) {
      console.log('re',chunk.rev,chunk.key,err);
      self.push({'chunk':chunk});
      cb();
    })
  } else {
    self.push({'chunk':chunk});
    cb();
  }
});

var follow = through2.obj(function(chunk,enc,cb) {
  var self = this;
  if(chunk.chunk) {
    var chunk = chunk.chunk;
    get_course(chunk.value).then(find_schooltime).then(get_schooltime).then(function(obj) {
      var attendance = obj.attendance;
      var students = obj.students.map(function(_obj) {
        return _obj.cid;
      });
      attendance.data.forEach(function(slot) {
        var absentList = slot.student.map(function(_obj) {
          return _obj.cid;
        });
        var presentList = students.filter(function(cid) {
          return absentList.indexOf(cid) < 0;
        });
        absentList.forEach(function(cid) {
          self.push({'attendance':attendance,'student':cid,
            'present':0,'absent':1,'total':1});
        });
        presentList.forEach(function(cid) {
          self.push({'attendance':attendance,'student':cid,
            'present':1,'absent':0,'total':1});
        });
      });
      cb();
     }).catch(function(err) {
        console.log('fl',chunk.rev,chunk.key,err);
        cb();
     })
  } else {
    self.push(chunk);
    cb();
  }
});

request.get({
  url:'https://maas.nuqlis.com:9000/api/log/attendance',
  qs:{'start':'14','limit':100},
  headers: {
    'authorization':'JWT '+config.token
  }})
.pipe(JSONStream.parse('*'))
.pipe(stream)
.pipe(reverse)
.pipe(follow)
.pipe(through2.obj(function(chunk,enc,cb) {
  // console.log(chunk);
  var self = this;
  var attendance = chunk.attendance;
  var key = [attendance.hostid,attendance.year,chunk.student];
  var change = {'present':chunk.present,'absent':chunk.absent,'total':chunk.total};
  var obj = {key:key,value:change};
  viewdb.get(bytewise.encode(obj.key),function(err,_obj) { 
    if(err) {
      viewdb.put(bytewise.encode(obj.key),obj.value,function(err) { 
        cb();
      });
    } else {
      _obj.present+=obj.value.present;
      _obj.absent+=obj.value.absent;
      _obj.total+=obj.value.total;
      console.log(obj.key,_obj);
      viewdb.put(bytewise.encode(obj.key),_obj,function(err) {
        cb();
      });
    }
  });
}));
