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
    ex.create(null, function(err, result) {
      test.expect(1);
      test.equal(result, 0);
      test.done();
    });
  },
};
