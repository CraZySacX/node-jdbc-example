var _ = require('underscore');
var asyncjs = require('async');
var nodeunit = require('nodeunit');
var ex = require('../lib/jdbc-example');

var insertUser = function(ps, id, callback) {
  ps.setInt(1, id, function(err) {
    if (err) {
      callback(err);
    } else {
      ps.setString(2, 'Jason_' + id, function(err) {
        if (err) {
          callback(err);
        } else {
          ps.executeUpdate(function(err, result) {
            if (err) {
              callback(err);
            } else {
              callback(null, result);
            }
          });
        }
      });
    }
  });
};

var notone = function(n, c) { if (n != 1) { c(true); } else { c(false); }};

exports.insert = {
  insert: function(test) {
    ex.derby(function(err, derby) {
      if (err) {
        console.log(err);
      } else {
        ex.tableexists(derby, null, null, 'BLAH', function(err, exists) {
          test.expect(3);
          if (exists) {
            ex.update(derby,
                      "INSERT INTO BLAH "
                    + "VALUES "
                    + "(1, 'Jason', CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP)",
                    function(err, result) {
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
      }
    });
  },
  _1000inserts: function(test) {
    ex.derby(function(err, derby) {
      if (err) {
        console.log(err);
      } else {
        ex.prepare(derby,
                   "INSERT INTO BLAH "
                 + "VALUES "
                 + "(?, ?, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP)",
          function(err, preparedstatement) {
            asyncjs.times(1000, function(n, next) {
              insertUser(preparedstatement, n, function(err, result) {
                next(err, result);
              });
            }, function(err, results) {
              if (err) {
                console.log(err);
              } else {
                test.expect(2);
                test.equal(results.length, 1000);
                asyncjs.filter(results, notone, function(results) {
                  test.equal(results.length, 0);
                  test.done();
                });
              }
            });
          });
      }
    });
  },
};
