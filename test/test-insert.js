var _ = require('underscore');
var asyncjs = require('async');
var nodeunit = require('nodeunit');
var ex = require('../lib/jdbc-example');

var insertUser = function(id, callback) {
  ex.update("INSERT INTO BLAH "
          + "VALUES "
          + "(" + id +", 'Jason_" + id + "', CURRENT_DATE, CURRENT_TIME, "
          + "CURRENT_TIMESTAMP)",
          function(err, result) {
            if (err) {
              callback(err);
            } else {
              callback(null, result);
            }
          });
};

exports.insert = {
  initialize: function(test) {
    ex.initialize(function(err, result) {
      test.expect(1);
      test.ok(result);
      test.done();
    });
  },
  insert: function(test) {
    ex.tableexists(null, null, 'BLAH', function(err, exists) {
      if (exists) {
        ex.update("INSERT INTO blah "
                + "VALUES "
                + "(1, 'Jason', CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP)",
                function(err, result) {
                  test.expect(3);
                  test.equal(err, null);
                  test.ok(result);
                  test.equal(result, 1);
                  test.done();
                });
      } else {
        console.log("TABLE 'BLAH' DOES NOT EXIST");
        test.done();
      }
    });
  },
  onethousandinserts: function(test) {
    asyncjs.times(1000, function(n, next) {
      insertUser(n, function(err, result) {
        next(err, result);
      });
    }, function(err, results) {
      if (err) {
        console.log("ERROR: " + err);
        test.done();
      } else {
        console.log("RESULTS: " + results.length);
        asyncjs.filter(results, function(n, callback) {
            if (n != 1) {
              console.log("BAD N: " + n);
              callback(true);
            } else {
              callback(false);
            }
        }, function(results) {
          test.done();
        });
      }
    });
  },
};
