/**
 * Module dependencies
 */

var _ = require('lodash');
var async = require('async');
var waterlineCriteria = require('waterline-criteria');
var Aggregate = require('./aggregates');
var Errors = require('waterline-errors').adapter;

/**
 * An In-Memory Datastore
 *
 * @param {Object} config
 * @param {Object} collections
 * @return {Object}
 * @api public
 */

var Database = module.exports = function(config, collections) {
  var self = this;

  // Hold Config values for each collection, this allows each collection
  // to define which file the data is synced to
  this.config = config || {};

  // Hold Collections
  this.collections = collections || {};

  // Build an object to hold the data
  this.data = {};

  // Build a Counters Object for Auto-Increment
  this.counters = {};

  // Hold Schema Objects to describe the structure of an object
  this.schema = {};

  return this;
};

/**
 * Initialize Database
 *
 */

Database.prototype.initialize = function(cb) {
  var self = this;

  async.eachSeries(Object.keys(self.collections), function(key, nextCollection) {
    var collection = self.collections[key];
    self.registerCollection(key, collection, nextCollection);
  }, cb);

};

/**
 * Register Collection
 *
 * @param {String} collectionName
 * @param {Object} collection
 * @param {Function} callback
 * @api public
 */

Database.prototype.registerCollection = function(collectionName, collection, cb) {
  this.setCollection(collectionName, collection, cb);
};

/**
 * Set Collection
 *
 * @param {String} collectionName
 * @param {Object} definition
 * @return Object
 * @api private
 */

Database.prototype.setCollection = function(collectionName, options, cb) {

  // Set Defaults
  var data = this.data[collectionName] || [];
  data.concat(options.data || []);

  // Ensure data is set
  this.data[collectionName] = data || [];

  // Set counters
  var counters = this.counters[collectionName] = this.counters[collectionName] || {};

  if(options.definition) options.definition = _.cloneDeep(options.definition);
  var schema = this.schema[collectionName] = options.definition || {};

  var obj = {
    data: data,
    schema: schema,
    counters: counters
  };

  setTimeout(function() {
    cb(null, obj);
  }, 0);
};

/**
 * Get Collection
 *
 * @param {String} collectionName
 * @return {Object}
 * @api private
 */

Database.prototype.getCollection = function(collectionName, cb) {

  var obj = {
    data: this.data[collectionName] || {},
    schema: this.schema[collectionName] || {},
    counters: this.counters[collectionName] || {}
  };

  setTimeout(function() {
    cb(null, obj);
  }, 0);
};

///////////////////////////////////////////////////////////////////////////////////////////
/// DDL
///////////////////////////////////////////////////////////////////////////////////////////

/**
 * Register a new Collection
 *
 * @param {String} collectionName
 * @param {Object} definition
 * @param {Function} callback
 * @return Object
 * @api public
 */

Database.prototype.createCollection = function(collectionName, definition, cb) {
  var self = this;

  this.setCollection(collectionName, { definition: definition }, function(err, collection) {
    if(err) return cb(err);
    cb(null, collection.schema);
  });
};

/**
 * Describe a collection
 *
 * @param {String} collectionName
 * @param {Function} callback
 * @api public
 */

Database.prototype.describe = function(collectionName, cb) {

  this.getCollection(collectionName, function(err, data) {
    if(err) return cb(err);

    var schema = Object.keys(data.schema).length > 0 ? data.schema : null;
    cb(null, schema);
  });
};

/**
 * Drop a Collection
 *
 * @param {String} collectionName
 * @api public
 */

Database.prototype.dropCollection = function(collectionName, relations, cb) {
  var self = this;

  if(typeof relations === 'function') {
    cb = relations;
    relations = [];
  }

  delete this.data[collectionName];
  delete this.schema[collectionName];

  relations.forEach(function(relation) {
    delete self.data[relation];
    delete self.schema[relation];
  });

  setTimeout(function() {
    cb();
  }, 0);
};

///////////////////////////////////////////////////////////////////////////////////////////
/// DQL
///////////////////////////////////////////////////////////////////////////////////////////

/**
 * Select
 *
 * @param {String} collectionName
 * @param {Object} options
 * @param {Function} cb
 * @api public
 */

Database.prototype.select = function(collectionName, options, cb) {

  // Filter Data based on Options criteria
  var resultSet = waterlineCriteria(collectionName, this.data, options);

  // Process Aggregate Options
  var aggregate = new Aggregate(options, resultSet.results);

  setTimeout(function() {
    if(aggregate.error) return cb(aggregate.error);
    cb(null, aggregate.results);
  }, 0);
};

/**
 * Insert A Record
 *
 * @param {String} collectionName
 * @param {Object} values
 * @param {Function} callback
 * @return {Object}
 * @api public
 */

Database.prototype.insert = function(collectionName, values, cb) {

  var self = this;

  var originalValues = _.clone(values);
  if(!Array.isArray(values)) values = [values];

  // To hold any uniqueness constraint violations we encounter:
  var constraintViolations = [];

  // Iterate over each record being inserted, deal w/ auto-incrementing
  // and checking the uniquness constraints.
  for (var i in values) {
    var record = values[i];

    // Check Uniqueness Constraints
    // (stop at the first failure)
    constraintViolations = constraintViolations.concat(self.enforceUniqueness(collectionName, record));
    if (constraintViolations.length) break;

    // Auto-Increment any values that need it
    record = self.autoIncrement(collectionName, record);
    record = self.serializeValues(collectionName, record);

    if (!self.data[collectionName]) return cb(Errors.CollectionNotRegistered);
    self.data[collectionName].push(record);
  }

  // If uniqueness constraints were violated, send back a validation error.
  if (constraintViolations.length) {
    return cb(new UniquenessError(constraintViolations));
  }

  setTimeout(function() {
    cb(null, Array.isArray(originalValues) ? values : values[0]);
  }, 0);
};

/**
 * Update A Record
 *
 * @param {String} collectionName
 * @param {Object} options
 * @param {Object} values
 * @param {Function} callback
 * @api public
 */

Database.prototype.update = function(collectionName, options, values, cb) {
  var self = this;

  // Filter Data based on Options criteria
  var resultSet = waterlineCriteria(collectionName, this.data, options);
  var resultIds = _.pluck(resultSet.results, 'id');

  // Enforce uniqueness constraints, indicating which records are updated
  // in case `values` doesn't contain an id.
  // If uniqueness constraints were violated, send back a validation error.
  var violations = self.enforceUniqueness(collectionName, values, resultIds);
  if (violations.length) {
    return cb(new UniquenessError(violations));
  }

  // Otherwise, success!
  // Build up final set of results.
  var results = [];
  for (var i in resultSet.indices) {
    var matchIndex = resultSet.indices[i];
    var _values = self.data[collectionName][matchIndex];

    // Clone the data to avoid providing raw access to the underlying
    // in-memory data, lest a user makes inadvertent changes in her app.
    self.data[collectionName][matchIndex] = _.extend(_values, values);
    results.push(_.cloneDeep(self.data[collectionName][matchIndex]));
  }

  setTimeout(function() {
    cb(null, results);
  }, 0);
};

/**
 * Destroy A Record
 *
 * @param {String} collectionName
 * @param {Object} options
 * @param {Function} callback
 * @api public
 */

Database.prototype.destroy = function(collectionName, options, cb) {
  var self = this;

  // Filter Data based on Options criteria
  var resultSet = waterlineCriteria(collectionName, this.data, options);

  this.data[collectionName] = _.reject(this.data[collectionName], function (model, i) {
    return _.contains(resultSet.indices, i);
  });

  setTimeout(function() {
    cb(null, resultSet.results);
  }, 0);
};

///////////////////////////////////////////////////////////////////////////////////////////
/// CONSTRAINTS
///////////////////////////////////////////////////////////////////////////////////////////

/**
 * Auto-Increment values based on schema definition
 *
 * @param {String} collectionName
 * @param {Object} values
 * @return {Object}
 * @api private
 */

Database.prototype.autoIncrement = function(collectionName, values) {

  for (var attrName in this.schema[collectionName]) {
    var attrDef = this.schema[collectionName][attrName];

    if(!attrDef.autoIncrement) continue;

    // Save many look-ups and many chars after minification
    var counters = this.counters[collectionName];

    // Only apply autoIncrement if value is not specified
    if(values[attrName]) {
      // If it is and is larger, set the counter to this value so the next increment will continue after it.
      // Test for an undefined counter, since `1 > undefined === false`
      if (!counters[attrName] || values[attrName] > counters[attrName]) {
        counters[attrName] = values[attrName];
      }
      continue;
    }

    // Set Initial Counter Value to 0 for this attribute if not set
    if(!counters[attrName]) counters[attrName] = 0;

    // Increment AI counter
    counters[attrName]++;

    // Set data to current auto-increment value
    values[attrName] = counters[attrName];
  }

  return values;
};

/**
 * Serialize Values
 *
 * Serializes/Casts values before inserting.
 *
 * @param {Object} values
 * @return {Object}
 * @api private
 */

Database.prototype.serializeValues = function(collectionName, values) {
  var self = this;

  Object.keys(values).forEach(function(key) {

    // Check if a type exist in the schema
    if(!self.schema[collectionName]) return;
    if(!self.schema[collectionName].hasOwnProperty(key)) return;

    var type = self.schema[collectionName][key].type,
        val;

    if(type === 'json') {
      try {
        val = JSON.parse(values[key]);
      } catch(e) {
        return;
      }
      values[key] = val;
    }
  });

  return values;
};

/**
 * enforceUniqueness
 *
 * Enforces uniqueness constraint.
 *
 * PERFORMANCE NOTE:
 * This is O(N^2) - could be easily optimized with a logarithmic algorithm,
 * but this is a development-only database adapter, so this is fine for now.
 *
 * @param {String} collectionName
 * @param {Object} values           - attribute values for a single record
 * @param {Array} updatedIds        - when updating the id is not necessarily included
 *     in the values to update, the ids of the records that will be updated is used when
 *     testing uniqueness of relevant attributes.
 * @return {Object}
 * @api private
 */

Database.prototype.enforceUniqueness = function(collectionName, values, updatedIds) {

  var errors = [];

  // Get the primary key attribute name, so as not to inadvertently check
  // uniqueness on something that doesn't matter.
  var pkAttrName = getPrimaryKey(this.schema[collectionName]);

  for (var attrName in this.schema[collectionName]) {
    var attrDef = this.schema[collectionName][attrName];

    if(!attrDef.unique) continue;

    for (var index in this.data[collectionName]) {

      // Ignore uniqueness check on undefined values
      // (they shouldn't have been stored anyway)
      if (_.isUndefined(values[attrName])) continue;

      // Does it look like a "uniqueness violation"?
      if (values[attrName] === this.data[collectionName][index][attrName]) {

        // It isn't actually a uniqueness violation if the record(s)
        // we're checking is the same as the record(s) we're updating/creating
        if (_.isUndefined(values[pkAttrName])) {
          if (updatedIds && updatedIds.indexOf(this.data[collectionName][index][pkAttrName]) > -1) {
            continue; // Id was found in the list of records being updated.
          }
        } else if (values[pkAttrName] === this.data[collectionName][index][pkAttrName]) {
          continue; // This is the data of the single record being updated.
        }

        var uniquenessError = {
          attribute: attrName,
          value: values[attrName],
          rule: 'unique'
        };

        errors.push(uniquenessError);
      }
    }
  }

  return errors;
};

/**
 * @param  {String} collectionIdentity
 * @return {String}
 * @api private
 */
Database.prototype.getPKField = function (collectionIdentity) {
  return getPrimaryKey(this.schema[collectionIdentity]);
};


/**
 * Convenience method to grab the name of the primary key attribute,
 * given the schema.
 *
 * @param  {Object} schema
 * @return {String}
 * @api private)
 */
function getPrimaryKey (schema) {
  var pkAttrName;
  _.each(schema, function (def, attrName) {
    if (def.primaryKey) pkAttrName = attrName;
  });
  return pkAttrName;
}

/**
 * Given an array of errors, create a WLValidationError-compatible
 * error definition.
 *
 * @param {Array} errors
 * @constructor
 * @api private
 */
function UniquenessError ( errors ) {

  // If no uniqueness constraints were violated, return early-
  // there are no uniqueness errors to worry about.
  if ( !errors.length ) return false;

  //
  // But if constraints were violated, we need to build a validation error.
  //

  // First, group errors into an object of single-item arrays of objects:
  // e.g.
  // {
  //   username: [{
  //     attribute: 'username',
  //     value: 'homeboi432'
  //   }]
  // }
  //
  errors = _.groupBy(errors, 'attribute');
  //
  // Then remove the `attribute` key.
  //
  errors = _.mapValues(errors, function (err) {
    delete err[0].attribute;
    return err;
  });

  // Finally, build a validation error:
  var validationError = {
    code: 'E_UNIQUE',
    invalidAttributes: errors
  };

  // and return it:
  return validationError;

}
