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

var initialized = false;

exports.initialize = function(callback) {
  if (!initialized) {
    derby.initialize(function(err) {
      if (err) {
        return callback(err);
      } else {
        initialized = true;
        return callback(null, initialized);
      }
    });
  } else {
    return callback(null, initialized);
  }
};

exports.update = function(sql, callback) {
  derby.reserve(function(err, connobj) {
    connobj.conn.createStatement(function(err, statement) {
      if (err) {
        return callback(err);
      } else {
        statement.executeUpdate(sql, function(err, result) {
          derby.release(connobj, function(err) {
            return callback(null, result);
          });
        });
      }
    });
  });
};

exports.tableexists = function(catalog, schema, name, callback) {
  derby.reserve(function(err, connobj) {
    connobj.conn.getMetaData(function(err, metadata) {
      if (err) {
        return callback(err);
      } else {
        metadata.getTables(catalog, schema, name, null, function(err, resultset) {
          if (err) {
            return callback(err);
          } else {
            resultset.toObjArray(function(err, results) {
              if (err) {
                return callback(err);
              } else {
                return callback(null, results.length > 0);
              }
            });
          }
        });
      }
    });
  });
};

exports.metadata = function(callback) {
  derby.reserve(function(err, connobj) {
    connobj.conn.getMetaData(function(err, metadata) {
      if (err) {
        return callback(err);
      } else {
        return callback(null, metadata);
      }
    });
  });
};
