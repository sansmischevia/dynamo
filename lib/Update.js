var Predicates = require("./Predicates")
  , Attributes = require("./Attributes")
  , Value = require("./Value")

function Update(table, database) {
  this.TableName = table

  Object.defineProperty(
    this, "database",
    {value: database, enumerable: false}
  )
}

Update.prototype = {
  when: function(name, operator, value) {
    var predicates = new Predicates(name, operator, value)
      , expectation = this.Expected = {}
      , names = Object.keys(predicates)

    if (names.length > 1) {
      throw new Error("Only one condition allowed per expectation.")
    }

    name = names[0]
    operator = predicates[name].ComparisonOperator
    value = predicates[name].AttributeValueList[0]

    switch (operator) {
      case "NULL":
        expectation[name] = {Exists: false}
        return this

      case "NOT_NULL":
        expectation[name] = {Exists: true}
        return this

      case "EQ":
        expectation[name] = {Value: value}
        return this
    }

    throw new Error("Invalid expectation: " + [name, operator, value])
  },

  returning: function(constant) {
    this.ReturnValues = constant

    return this
  },

  update: function(action, key, value) {
    var updates = this.AttributeUpdates || (this.AttributeUpdates = {})

    if (updates[key]) {
      throw new Error("Attribute '" + key + "' cannot be updated more than once.")
    }

    updates[key] = {Action: action}

    if (value != null) updates[key].Value = Value(value)

    return this
  },

  put: function(key, value) {
    return this.update("PUT", key, value)
  },

  add: function(key, value) {
    return this.update("ADD", key, value)
  },

  remove: function(key, value) {
    return this.update("DELETE", key, value)
  },

  retryCount: function(retryCount) {
    if (retryCount && retryCount > 0) {
      this.retryCount = retryCount;
    }
    else {
      this.retryCount = 0;
    }
      
    return this;
  },

  errorEmitter: function(ee) {
    if (ee && ee.listeners) {
      var l = ee.listeners('error');
      if (l.length > 0) {
        this.ee = ee;
      }
    }

    return this;
  },

  save: function(cb) {
    if (this.Item) {
      var self = this;
      (function retry(c) {
        self.database.request(
          "PutItem",
          self,
          function(err, data) {
            if (err && self.retryCount > 0) {
              if (c <= self.retryCount) {
                setTimeout(function() {
                  retry(c + 1);
                }, Math.pow(2, c - 1) * 10 * Math.random() + 1);
                if (self.ee) self.ee.emit('error', err);
              }
              else {
                return cb(err);
              }
            }
            else if (err) {
              return cb(err);
            }
            else {
              if (data = data.Attributes) {
                cb(null, Attributes.prototype.parse(data))
              }

              else cb()  
            }
          }
        )
      })(1);


      return this
    }

    if (this.AttributeUpdates) {
      var self = this;
      (function retry(c) {
        self.database.request("UpdateItem", self, function(err, result) {
          if (err && self.retryCount > 0) {
            if (c <= self.retryCount) {
              setTimeout(function() {
                retry(c + 1);
              }, Math.pow(2, c - 1) * 10 * Math.random() + 1);
              if (self.ee) self.ee.emit('error', err);
            }
            else {
              cb(err);
            }
          }
          else {
            cb(err, result);  
          }
        });
      })(1);

      return this
    }

    throw new Error("Nothing to save.")
  }
}

module.exports = Update
