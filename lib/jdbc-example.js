var JDBC = require('jdbc');
var jinst = require('jdbc/lib/jinst');

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
  jinst.setupClasspath(['./drivers/derbyclient.jar']);
}

var derby = new JDBC({
  url: 'jdbc:derby://localhost:1527/testdb;create=true',
  minpoolsize: 5,
  maxpoolsize: 10
});

var testconn = null;

exports.initialize = function(callback) {
  derby.initialize(function(err) {
    if (err) {
      return callback(err);
    } else {
      derby.status();
      return callback(null, derby._pool.length);
    }
  });
};

exports.create = function(sql, callback) {
  derby.reserve(function(err, connobj) {
    derby.status();
    connobj.conn.createStatement(function(err, statement) {
      if (err) {
        return callback(null);
      } else {
        statement.executeUpdate(sql, function(err, result) {
          derby.release(connobj, function(err) {
            derby.status();
            return callback(null, result);
          });
        });
      }
    });
  });
};
