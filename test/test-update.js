var _ = require('lodash');
var asyncjs = require('async');
var nodeunit = require('nodeunit');
var ex = require('../lib/jdbc-example');

var updateUser = function(ps, id, callback) {
  ps.setInt(1, id, function(err) {
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
};

var notone = function(n, c) { if (n != 1) { c(true); } else { c(false); }};

exports.derby = {
  update: function(test) {
    ex.derby(function(err, derby) {
      if (err) {
        console.log(err);
      } else {
        ex.tableexists(derby, null, null, 'BLAH', function(err, exists) {
          test.expect(3);
          if (exists) {
            ex.update(derby, "UPDATE blah SET id = 2 WHERE name = 'Jason'",
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
  _1000updates: function(test) {
    ex.derby(function(err, derby) {
      if (err) {
        console.log(err);
      } else {
        ex.prepare(derby, "UPDATE BLAH SET id = ? where name = 'Jason'",
          function(err, preparedstatement) {
            var hrstart = process.hrtime();
            asyncjs.times(1000, function(n, next) {
              updateUser(preparedstatement, n, function(err, result) {
                next(err, result);
              });
            }, function(err, results) {
              hrend = process.hrtime(hrstart);
              if (err) {
                console.log(err);
              } else {
                test.expect(2);
                test.equal(results.length, 1000);
                asyncjs.filter(results, notone, function(results) {
                  test.equal(results.length, 0);
                  console.info("\u2714 derby - _1000updates: Execution time (hr): %ds %dms", hrend[0], hrend[1]/1000000);
                  test.done();
                });
              }
            });
          });
      }
    });
  },
};

exports.hsqldb = {
  update: function(test) {
    ex.hsqldb(function(err, hsqldb) {
      if (err) {
        console.log(err);
      } else {
        ex.tableexists(hsqldb, null, null, 'BLAH', function(err, exists) {
          test.expect(3);
          if (exists) {
            ex.update(hsqldb, "UPDATE blah SET id = 2 WHERE name = 'Jason'",
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
  _1000updates: function(test) {
    ex.hsqldb(function(err, hsqldb) {
      if (err) {
        console.log(err);
      } else {
        ex.prepare(hsqldb, "UPDATE BLAH SET id = ? where name = 'Jason'",
          function(err, preparedstatement) {
            var hrstart = process.hrtime();
            asyncjs.times(1000, function(n, next) {
              updateUser(preparedstatement, n, function(err, result) {
                next(err, result);
              });
            }, function(err, results) {
              hrend = process.hrtime(hrstart);
              if (err) {
                console.log(err);
              } else {
                test.expect(2);
                test.equal(results.length, 1000);
                asyncjs.filter(results, notone, function(results) {
                  test.equal(results.length, 0);
                  console.info("\u2714 hsqldb - _1000updates: Execution time (hr): %ds %dms", hrend[0], hrend[1]/1000000);
                  test.done();
                });
              }
            });
          });
      }
    });
  },
};
