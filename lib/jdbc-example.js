var JDBC = require('jdbc');
var jinst = require('jdbc/lib/jinst');

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
  jinst.setupClasspath(['./drivers/hsqldb.jar', './drivers/derbyclient.jar']);
}

var derby = new JDBC({
  url: 'jdbc:derby://localhost:1527/testdb;create=true',
  minpoolsize: 5,
  maxpoolsize: 10
});

var hsqldb = new JDBC({
  url: 'jdbc:hsqldb:hsql://localhost/xdb',
  minpoolsize: 5,
  maxpoolsize: 10,
  properties: {
    user : 'SA',
    password: ''
  }
});

var derbyInit = false;
var hsqldbInit = false;

function reserve(db, callback) {
  db.reserve(function(err, connobj) {
    if (err) {
      return callback(err);
    } else {
      return callback(null, connobj, connobj.conn);
    }
  });
};

function release(db, connobj, err, result, callback) {
  db.release(connobj, function(e) {
    if (err) {
      return callback(err);
    } else {
      return callback(null, result);
    }
  });
};

exports.derby = function(callback) {
  if (!derbyInit) {
    derby.initialize(function(err) {
      if (err) {
        return callback(err);
      } else {
        derbyInit = true;
        return callback(null, derby);
      }
    });
  } else {
    return callback(null, derby);
  }
};

exports.hsqldb = function(callback) {
  if (!hsqldbInit) {
    hsqldb.initialize(function(err) {
      if (err) {
        return callback(err);
      } else {
        hsqldbInit = true;
        return callback(null, hsqldb);
      }
    });
  } else {
    return callback(null, hsqldb);
  }
};

exports.prepare = function(db, sql, callback) {
  reserve(db, function(err, connobj, conn) {
    conn.prepareStatement(sql, function(err, preparedstatement) {
      release(db, connobj, err, preparedstatement, callback);
    });
  });
};

exports.prepareCall = function(db, sql, callback) {
  reserve(db, function(err, connobj, conn) {
    conn.prepareCall(sql, function(err, callablestatement) {
      release(db, connobj, err, callablestatement, callback);
    });
  });
};

exports.update = function(db, sql, callback) {
  reserve(db, function(err, connobj, conn) {
    conn.createStatement(function(err, statement) {
      if (err) {
        release(db, connobj, err, null, callback);
      } else {
        statement.executeUpdate(sql, function(err, result) {
          release(db, connobj, err, result, callback);
        });
      }
    });
  });
};

exports.tableexists = function(db, catalog, schema, name, callback) {
  reserve(db, function(err, connobj, conn) {
    conn.getMetaData(function(err, metadata) {
      if (err) {
        release(db, connobj, err, null, callback);
      } else {
        metadata.getTables(catalog, schema, name, null, function(err, resultset) {
          if (err) {
            release(db, connobj, err, null, callback);
          } else {
            resultset.toObjArray(function(err, results) {
              release(db, connobj, err, results.length > 0, callback);
            });
          }
        });
      }
    });
  });
};

exports.metadata = function(db, callback) {
  reserve(db, function(err, connobj, conn) {
    conn.getMetaData(function(err, metadata) {
      release(db, connobj, err, metadata, callback);
    });
  });
};
