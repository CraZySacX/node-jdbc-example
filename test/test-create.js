var _ = require('underscore');
var asyncjs = require('async');
var nodeunit = require('nodeunit');
var ex = require('../lib/jdbc-example');

var pcreate = function(n, callback) {
  ex.update("CREATE TABLE blah" + n + " "
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

exports.create = {
  initderby: function(test) {
    ex.initDerby(function(err, result) {
      test.expect(1);
      test.ok(result);
      test.done();
    });
  },
  inithsqldb: function(test) {
    ex.initHSQLDB(function(err, result) {
      test.expect(1);
      test.ok(result);
      test.done();
    });
  },
  create: function(test) {
    ex.update("CREATE TABLE BLAH "
            + "(id int, name varchar(10), created_date DATE, "
            +  "created_time TIME, "
            +  "created_timestamp TIMESTAMP)",
            function(err, result) {
              test.expect(1);
              test.equal(result, 0);
              test.done();
            });
  },
  createh: function(test) {
    ex.updateh("CREATE TABLE BLAH "
             + "(id int, name varchar(10), created_date DATE, "
             +  "created_time TIME, "
             +  "created_timestamp TIMESTAMP)",
            function(err, result) {
              test.expect(1);
              test.equal(result, 0);
              test.done();
            });
  },
  parallelcreates: function(test) {
    asyncjs.parallel([
      function(callback) { pcreate(0, callback); },
      function(callback) { pcreate(1, callback); },
      function(callback) { pcreate(2, callback); },
      function(callback) { pcreate(3, callback); },
      function(callback) { pcreate(4, callback); },
    ], function(err, results) {
      test.expect(6);
      test.equal(results.length, 5);
      _.each(results, function(result) {
        test.equal(result, 0);
      });
      test.done();
    });
  },
  storedprocedure: function(test) {
    ex.updateh("CREATE PROCEDURE NEW_BLAH(id int, name varchar(10)) "
             + "MODIFIES SQL DATA "
             + "BEGIN ATOMIC "
             + "  INSERT INTO BLAH VALUES (id, name, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP); "
             + "END;",
            function(err, result) {
              test.expect(1);
              test.equal(null, err);
              test.done();
            });
  },
  callproc: function(test) {
    ex.prepareCall("{ call NEW_BLAH(2, 'Another')}", function(err, callablestatement) {
      if (err) {
        console.log(err);
      } else {
        callablestatement.executeUpdate(function(err, result) {
          test.expect(1);
          test.equal(null, err);
          test.done();
        });
      }
    });
  },
};
