var through2 = require('through2');
var sublevel = require('level-sublevel');
var levelup = require('levelup');
var bytewise = require('bytewise');
var request = require('request');
var _ = require('lodash');

var viewdb = sublevel(levelup('./myView',{'valueEncoding':'json'}));
var teachdb = viewdb.sublevel('teach');
var schoolTimeDb = viewdb.sublevel('schooltime');
var studentDb = viewdb.sublevel('student');

var azureConfig = {
  domain: "https://newtestnew.azurewebsites.net",
};

var get_course = function(obj) {
  console.log('get_course');
  var attendance  = obj;
  return new Promise(function(fullfill,reject) {
    var key = bytewise.encode([
       attendance.hostid,
       attendance.year,
       attendance.staffid]);
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
};

var find_schooltime = function(obj) {
  console.log('find_schooltime');
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
  console.log('get_schooltime',obj);
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
  console.log('reverse',chunk);
  var self = this;
  if(chunk._value) {
    get_course(chunk._value).then(find_schooltime).then(get_schooltime).then(function(obj) {
      var attendance = obj.attendance;
      var students = _.map(obj.students,'cid');
      attendance.data.forEach(function(slot) {
        var absentList = _.map(slot.student,'cid');
        var presentList = _.difference(students,absentList);

        _.forEach(absentList,function(cid) {
          self.push({'attendance':attendance,'student':cid,
            'present':0,'absent':-1,'total':-1});
        });
        _.forEach(presentList,function(cid) {
          self.push({'attendance':attendance,'student':cid,
            'present':-1,'absent':0,'total':-1});
        });
      });
      console.log('115 callback');
      self.push(chunk);
      cb();
    }).catch(function(err) {
      console.log('re',chunk._rev,chunk.key,err);
      self.push(chunk);
      console.log('120 callback');
      cb();
    })
  } else {
    self.push(chunk);
    console.log('124 callback');
    cb();
  }
});

var forward = through2.obj(function(chunk,enc,cb) {
  console.log('forward',chunk);
  var self = this;
  if(chunk.value) {
    get_course(chunk.value).then(find_schooltime).then(get_schooltime).then(function(obj) {
      var attendance = obj.attendance;
      var students = obj.students.map(function(_obj) {
        return _obj.cid;
      });
      attendance.data.forEach(function(slot) {
        var absentList = _.map(slot.student,'cid');
        var presentList = _.difference(students,absentList);
        absentList.forEach(function(cid) {
          self.push({'attendance':attendance,'student':cid,
            'present':0,'absent':1,'total':1});
        });
        presentList.forEach(function(cid) {
          self.push({'attendance':attendance,'student':cid,
            'present':1,'absent':0,'total':1});
        });
      });
      self.push(chunk);
      cb();
     }).catch(function(err) {
        console.log('fl',chunk._rev,chunk.key,err);
        self.push(chunk);
        console.log('154 callback');
        cb();
     })
  } else {
    self.push(chunk);
    cb();
  }
});

module.exports = function(config) {
  return function(next) {
    var myStream = through2.obj(function(chunk,enc,cb) {
      console.log('+myStream');
      this.push(chunk);
      console.log('-myStream');
      console.log('169 callback');
      cb();
    });

    var endStream = through2.obj(function(chunk,enc,cb) {
      // console.log(chunk);
      if(chunk.student) {
        console.log(chunk.student);
      }
      if(chunk._rev) {
        console.log('end',chunk._rev);
        this.push(chunk);
        next();
      }
      cb();
    });

    return [myStream,reverse,forward,endStream];
  };
}