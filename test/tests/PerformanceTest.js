var should = require("should")
  , dynamo = require("../../")
  , uuid = require("node-uuid")
  , client = dynamo.createClient()
  , db = client.get("us-east-1");

var events = require('events');

var TABLE_NAME = "DYNAMO_PERF_TEST_TABLE";
var CLEAN_TABLES = false;
var OBJECTS = [];
var WRITE_CAPACITY = 20;
var READ_CAPACITY = 20;

function createTestTable(cb) {
  db.add({
      name: TABLE_NAME,
      schema: [ ['id', String] ],
      throughput: {read: 20, write: 20}
    })
    .save(function(err, table) {
      should.not.exist(err);
      should.exist(table);

      table.should.have.property("TableName", TABLE_NAME);

      table.should.have.property("KeySchema");
      table.KeySchema.should.have.property("HashKeyElement");
      table.KeySchema.HashKeyElement.should.have.property("AttributeName", "id");
      table.KeySchema.HashKeyElement.should.have.property("AttributeType", String);

      table.should.have.property("ProvisionedThroughput");
      table.ProvisionedThroughput.should.have.property("ReadCapacityUnits", READ_CAPACITY);
      table.ProvisionedThroughput.should.have.property("WriteCapacityUnits", WRITE_CAPACITY);

      // Wait for table to become active
      db.get(TABLE_NAME).watch(function() {
        cb();
      });
  });
}

function putOne(i, ee) {
  var obj = {
    id: uuid.v4(),
    data: i + "_" + Date.now()
  };
  OBJECTS.push(obj);

  db.put(TABLE_NAME, obj).save(function(err, result) {
    if (err) {
      console.warn('put error: ' + i + ' : %j', err);
      ee.emit('error', err);
      return;
    }

    ee.emit('done', result);
  });
}

function getItem(id, ee) { 
  db.get(TABLE_NAME, { id: id }).fetch(function(err, item) {
    if (err) {
      console.warn('get error: %j', err);
      ee.emit('error', err);
      return;
    }

    ee.emit('done', item);
  });
}

function putItems(times, cb) {
  var ee = new events.EventEmitter();
  var completedPuts = 0;
  var errors = 0;
  var st = setTimeout(function() {
    cb(new Error('put did not complete ' + completedPuts + "/" + times +
      '. Error Rate: ' + errors / times), completedPuts);
  }, Math.ceil(times / WRITE_CAPACITY * 1200) + 3 * 1000);

  ee.on('done', function(result) {
    completedPuts++;
    if (completedPuts === times) {
      clearTimeout(st);
      cb(null, completedPuts);
    }
  });

  ee.on('error', function(error) {
    errors++;
  });

  for (var i = 0; i < times; i++) {
    putOne(i, ee);
  }
}

function getObjects(objs, cb) {
  var ee = new events.EventEmitter();
  var completedGets = 0;
  var errors = 0;
  var st = setTimeout(function() {
    cb(new Error('get did not complete ' + completedGets + "/" + objs.length + 
      '. Error Rate: ' + errors / objs.length), completedGets);
  }, Math.ceil(objs.length / WRITE_CAPACITY * 1200) + 3 * 1000);
  
  var results = [];
  ee.on('done', function(result) {
    completedGets++;
    results.push(result.id);
    if (completedGets === objs.length) {
      clearTimeout(st);
      cb(null, results);
    }
  });

  ee.on('error', function(error) {
    errors++;
  });

  for (var o in objs) {
    getItem(objs[o].id, ee);
  }
}

describe("Performance", function() {
  before(function(done) {
    db.fetch(function(err) {
      should.not.exist(err);
      var perfTable = db.tables[TABLE_NAME];
      if (!perfTable || perfTable.TableName !== TABLE_NAME) {
        createTestTable(done);
      }
      else {
        // Wait for table to become active
        db.get(TABLE_NAME).watch(function() {
          done();
        });
      }
    });
  });

  describe("test", function() {
    var ITEMS = 20;
    it('adds ' + ITEMS + ' items to the table', function(done) {
      // Create X clients to add Y items into the table
      putItems(ITEMS, function(err) {
        should.not.exist(err);
        done();
      });
    });

    var ids;
    it('checks that all ' + ITEMS + ' items exist', function(done) {
      getObjects(OBJECTS, function(err, results) {
        should.not.exist(err);
        results.length.should.equal(OBJECTS.length);
        ids = results;
        done();
      })
    });

    it('checks that results match up', function() {
      for (var i in OBJECTS) {
        var id = OBJECTS[i].id;
        ids.indexOf(id).should.be.above(-1);
      }
    });
  });  

  after(function(done) {
    if (!CLEAN_TABLES) {
      done();
    }
    db.get(TABLE_NAME).watch(function() {
      db.remove(TABLE_NAME, function(err) {
        if (err) {
          throw err;
        }
        done();
      });
    });
  });
});
