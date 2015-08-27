var asyncjs = require('async');
var nodeunit = require('nodeunit');
var ex = require('../lib/jdbc-example');

var new_blah = function(cs, id, callback) {
  cs.setInt(1, id, function(err) {
    if (err) {
      callback(err);
    } else {
      cs.setString(2, 'Jason_' + id, function(err) {
        if (err) {
          callback(err);
        } else {
          cs.executeUpdate(function(err, result) {
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

var notzero = function(n, c) { if (n != 0) { c(true); } else { c(false); }};

exports.hsqldb = {
  storedprocedure: function(test) {
    ex.hsqldb(function(err, hsqldb) {
      if (err) {
        console.log(err);
      } else {
        ex.update(hsqldb,
                  "CREATE PROCEDURE NEW_BLAH(id int, name varchar(10)) "
                + "MODIFIES SQL DATA "
                + "BEGIN ATOMIC "
                + "  INSERT INTO BLAH VALUES (id, name, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP); "
                + "END;",
                function(err, result) {
                  test.expect(1);
                  test.equal(null, err);
                  test.done();
                });
      }
    });
  },
  callproc: function(test) {
    ex.hsqldb(function(err, hsqldb) {
      if (err) {
        console.log(err);
      } else {
        ex.prepareCall(hsqldb, "{ call NEW_BLAH(2, 'Another')}", function(err, callablestatement) {
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
      }
    });
  },
  _1000calls: function(test) {
    ex.hsqldb(function(err, hsqldb) {
      if (err) {
        console.log(err);
      } else {
        ex.prepareCall(hsqldb, "{call NEW_BLAH(?, ?)}",
          function(err, callablestatement) {
            var hrstart = process.hrtime();
            asyncjs.times(1000, function(n, next) {
              new_blah(callablestatement, n, function(err, result) {
                next(err, result);
              });
            }, function(err, results) {
              hrend = process.hrtime(hrstart);
              if (err) {
                console.log(err);
              } else {
                test.expect(2);
                test.equal(results.length, 1000);
                asyncjs.filter(results, notzero, function(results) {
                  test.equal(results.length, 0);
                  console.info("\u2714 hsqldb - _1000calls: Execution time (hr): %ds %dms", hrend[0], hrend[1]/1000000);
                  test.done();
                });
              }
            });
          });
      }
    });
  },
};
