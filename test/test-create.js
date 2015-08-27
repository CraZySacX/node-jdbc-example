var _ = require('lodash');
var asyncjs = require('async');
var nodeunit = require('nodeunit');
var ex = require('../lib/jdbc-example');

var pcreate = function(db, n, callback) {
  ex.update(db,
            "CREATE TABLE blah" + n + " "
          + "(id int, name varchar(10), created_date DATE, "
          +  "created_time TIME, "
          +  "created_timestamp TIMESTAMP)",
          function(err, result) {
            if (err) {
              callback(err);
            } else {
              callback(null, result);
            }
          });
};

exports.derby = {
  create: function(test) {
    ex.derby(function(err, derby) {
      if (err) {
        console.log(err);
      } else {
        ex.update(derby,
                  "CREATE TABLE BLAH "
                + "(id int, name varchar(10), created_date DATE, "
                +  "created_time TIME, "
                +  "created_timestamp TIMESTAMP)",
                function(err, result) {
                  test.expect(1);
                  test.equal(result, 0);
                  test.done();
                });
      }
    });
  },
  parallelcreates: function(test) {
    ex.derby(function(err, derby) {
      if (err) {
        console.log(err);
      } else {
        var hrstart = process.hrtime();
        asyncjs.parallel([
          function(callback) { pcreate(derby, 0, callback); },
          function(callback) { pcreate(derby, 1, callback); },
          function(callback) { pcreate(derby, 2, callback); },
          function(callback) { pcreate(derby, 3, callback); },
          function(callback) { pcreate(derby, 4, callback); },
        ], function(err, results) {
          hrend = process.hrtime(hrstart);
          test.expect(6);
          test.equal(results.length, 5);
          _.each(results, function(result) {
            test.equal(result, 0);
          });
          console.info("\u2714 derby - parallelcreates: Execution time (hr): %ds %dms", hrend[0], hrend[1]/1000000);
          test.done();
        });
      }
    });
  },
};

exports.hsqldb = {
  create: function(test) {
    ex.hsqldb(function(err, hsqldb) {
      if (err) {
        console.log(err);
      } else {
        ex.update(hsqldb,
                  "CREATE TABLE BLAH "
                + "(id int, name varchar(10), created_date DATE, "
                +  "created_time TIME, "
                +  "created_timestamp TIMESTAMP)",
                function(err, result) {
                  test.expect(1);
                  test.equal(result, 0);
                  test.done();
                });
      }
    });
  },
  parallelcreates: function(test) {
    ex.hsqldb(function(err, hsqldb) {
      if (err) {
        console.log(err);
      } else {
        var hrstart = process.hrtime();
        asyncjs.parallel([
          function(callback) { pcreate(hsqldb, 0, callback); },
          function(callback) { pcreate(hsqldb, 1, callback); },
          function(callback) { pcreate(hsqldb, 2, callback); },
          function(callback) { pcreate(hsqldb, 3, callback); },
          function(callback) { pcreate(hsqldb, 4, callback); },
        ], function(err, results) {
          hrend = process.hrtime(hrstart);
          test.expect(6);
          test.equal(results.length, 5);
          _.each(results, function(result) {
            test.equal(result, 0);
          });
          console.info("\u2714 hsqldb - parallelcreates: Execution time (hr): %ds %dms", hrend[0], hrend[1]/1000000);
          test.done();
        });
      }
    });
  },
};
