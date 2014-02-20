/**
 * Module dependencies
 */

var _ = require('lodash'),
    async = require('async'),
    waterlineCriteria = require('waterline-criteria'),
    Aggregate = require('./aggregates'),
    Errors = require('waterline-errors').adapter;

/**
 * A File-Backed Datastore
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

  cb(null, obj);
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

  cb(null, obj);
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

  cb();
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

  if(aggregate.error) return cb(aggregate.error);
  cb(null, aggregate.results);
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

  // Check Uniqueness Constraints
  var errors = this.uniqueConstraint(collectionName, values);
  if(errors && errors.length > 0) return cb(errors);

  // Auto-Increment any values
  values = this.autoIncrement(collectionName, values);
  values = this.serializeValues(collectionName, values);

  if (!this.data[collectionName]) return cb(Errors.CollectionNotRegistered);
  this.data[collectionName].push(values);

  cb(null, values);
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

  // Get a list of any attributes we're updating that:
  // i) are in the schema
  // ii) have uniqueness constraints
  var uniqueAttrs = _.where(_.keys(values), function(attrName) {
    var attributeDef = self.schema[collectionName][attrName];
    return attributeDef && attributeDef.unique;
  });

  // If we're updating any attributes that are supposed to be unique, do some additional checks
  if (uniqueAttrs.length && resultSet.indices.length) {

    // If we would be updating more than one record, then uniqueness constraint automatically fails
    if (resultSet.indices.length > 1) {
      var error = new Error('Uniqueness check failed on attributes: ' + uniqueAttrs.join(','));
      return cb(error);
    }

    // Otherwise for each unique attribute, ensure that the matching result already has the value
    // we're updating it to, so that there wouldn't be more than one record with the same value.
    else {
      var result = self.data[collectionName][resultSet.indices[0]];
      var errors = [];
      _.each(uniqueAttrs, function(uniqueAttr) {
        if (result[uniqueAttr] != values[uniqueAttr]) {
          errors.push(new Error('Uniqueness check failed on attribute: ' + uniqueAttr + ' with value: ' + values[uniqueAttr]));
        }
      });
      if (errors.length) {
        return cb(errors);
      }
    }
  }

  var results = [];

  resultSet.indices.forEach(function(matchIndex) {
    var _values = self.data[collectionName][matchIndex];
    self.data[collectionName][matchIndex] = _.extend(_values, values);
    results.push(_.cloneDeep(self.data[collectionName][matchIndex]));
  });

  cb(null, results);
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

  cb(null, resultSet.results);
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

    // Only apply autoIncrement if value is not specified
    if(!attrDef.autoIncrement) continue;
    if(values[attrName]) continue;

    // Set Initial Counter Value to 0 for this attribute if not set
    if(!this.counters[collectionName][attrName]) this.counters[collectionName][attrName] = 0;

    // Increment AI counter
    this.counters[collectionName][attrName]++;

    // Set data to current auto-increment value
    values[attrName] = this.counters[collectionName][attrName];
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
 * Unique Constraint
 *
 * @param {String} collectionName
 * @param {Object} values
 * @return {Array}
 * @api private
 */

Database.prototype.uniqueConstraint = function(collectionName, values) {

  var errors = [];

  for (var attrName in this.schema[collectionName]) {
    var attrDef = this.schema[collectionName][attrName];

    if(!attrDef.unique) continue;

    for (var index in this.data[collectionName]) {

      // Ignore uniquness check on undefined values
      if (_.isUndefined(values[attrName])) continue;

      if (values[attrName] === this.data[collectionName][index][attrName]) {
        var error = new Error('Uniqueness check failed on attribute: ' + attrName +
          ' with value: ' + values[attrName]);

        errors.push(error);
      }
    }
  }

  return errors;
};
