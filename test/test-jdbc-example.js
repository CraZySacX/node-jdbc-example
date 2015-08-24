var _ = require('underscore');
var asyncjs = require('async');
var nodeunit = require('nodeunit');
var ex = require('../lib/jdbc-example');

module.exports = {
  testinitialize: function(test) {
    ex.initialize(function(err, result) {
      test.expect(1);
      test.ok(result);
      test.done();
    });
  },
  testcreate: function(test) {
    ex.create("CREATE TABLE blah "
            + "(id int, name varchar(10), created_date DATE, "
            +  "created_time TIME, "
            +  "created_timestamp TIMESTAMP)",
            function(err, result) {
              test.expect(1);
              test.equal(result, 0);
              test.done();
            });
  },
  testparallelcreates: function(test) {
    asyncjs.parallel([
      function(callback) {
        ex.create("CREATE TABLE blah0 "
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
      },
      function(callback) {
        ex.create("CREATE TABLE blah1 "
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
      },
      function(callback) {
        ex.create("CREATE TABLE blah2 "
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
      },
      function(callback) {
        ex.create("CREATE TABLE blah3 "
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
      },
      function(callback) {
        ex.create("CREATE TABLE blah4 "
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
      },
    ], function(err, results) {
      test.expect(6);
      test.equal(results.length, 5);
      _.each(results, function(result) {
        test.equal(result, 0);
      });
      test.done();
    });
  },
};
