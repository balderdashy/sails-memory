
/**
 * Module dependencies
 */

var _ = require('lodash'),
    waterlineCriteria = require('waterline-criteria'),
    Aggregate = require('./aggregates');

/**
 * A In-Memory Datastore
 *
 * @return {Object}
 * @api public
 */

var Database = module.exports = function() {
  var self = this;

  // Hold Config values for each collection
  this.config = {};

  // Build an object to hold the data
  this.data = {};

  // Build a Counters Object for Auto-Increment
  this.counters = {};

  // Hold Schema Objects to describe the structure of an object
  this.schema = {};

  return this;
};

/**
 * Register Collection
 *
 * @param {String} collectionName
 * @param {Object} config
 * @param {Function} callback
 * @api public
 */

Database.prototype.registerCollection = function(collectionName, config, cb) {
  var name = collectionName.toLowerCase();

  // Set Empty Defaults
  if(!this.config[name]) this.config[name] = config;
  if(!this.data[name]) this.data[name] = {};
  if(!this.counters[name]) this.counters[name] = {};
  if(!this.schema[name]) this.schema[name] = {};

  cb();
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
  var name = collectionName.toLowerCase();

  // Set Defaults
  var data = this.data[name] = options.data || [];
  var counters = this.counters[name] = options.counters || {};

  if(options.definition) options.definition = _.cloneDeep(options.definition);
  var schema = this.schema[name] = options.definition || {};

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
  var name = collectionName.toLowerCase();

  var obj = {
    data: this.data[name] || {},
    schema: this.schema[name] || {},
    counters: this.counters[name] || {}
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
  var self = this,
      name = collectionName.toLowerCase();

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

Database.prototype.dropCollection = function(collectionName, cb) {
  var name = collectionName.toLowerCase();

  delete this.data[name];
  delete this.schema[name];
  delete this.counters[name];

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
  var name = collectionName.toLowerCase();

  // Filter Data based on Options criteria
  var resultSet = waterlineCriteria(name, this.data, options);

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
  var name = collectionName.toLowerCase();

  // Check Uniqueness Constraints
  var errors = this.uniqueConstraint(collectionName, values);
  if(errors && errors.length > 0) return cb(errors);

  // Auto-Increment any values
  values = this.autoIncrement(collectionName, values);
  values = this.serializeValues(collectionName, values);
  this.data[name].push(values);

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
  var self = this,
      name = collectionName.toLowerCase();

  // Filter Data based on Options criteria
  var resultSet = waterlineCriteria(name, this.data, options);
  var results = [];

  resultSet.indicies.forEach(function(matchIndex) {
    var _values = self.data[name][matchIndex];
    self.data[name][matchIndex] = _.merge(_values, values);
    results.push(_.cloneDeep(self.data[name][matchIndex]));
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
  var self = this,
      name = collectionName.toLowerCase();

  // Filter Data based on Options criteria
  var resultSet = waterlineCriteria(name, this.data, options);

  this.data[name] = _.reject(this.data[name], function (model, i) {
    return _.contains(resultSet.indicies, i);
  });

  cb();
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
  var name = collectionName.toLowerCase();

  for (var attrName in this.schema[name]) {
    var attrDef = this.schema[name][attrName];

    // Only apply autoIncrement if value is not specified
    if(!attrDef.autoIncrement) continue;
    if(values[attrName]) continue;

    // Set Initial Counter Value to 0 for this attribute if not set
    if(!this.counters[name][attrName]) this.counters[name][attrName] = 0;

    // Increment AI counter
    this.counters[name][attrName]++;

    // Set data to current auto-increment value
    values[attrName] = this.counters[name][attrName];
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
  var self = this,
      name = collectionName.toLowerCase();

  Object.keys(values).forEach(function(key) {
    var type = self.schema[name][key].type,
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
  var name = collectionName.toLowerCase();

  var errors = [];

  for (var attrName in this.schema[name]) {
    var attrDef = this.schema[name][attrName];

    if(!attrDef.unique) continue;

    for (var index in this.data[name]) {

      // Ignore uniquness check on undefined values
      if (_.isUndefined(values[attrName])) continue;

      if (values[attrName] === this.data[name][index][attrName]) {
        var error = new Error('Uniqueness check failed on attribute: ' + attrName +
          ' with value: ' + values[attrName]);

        errors.push(error);
      }
    }
  }

  return errors;
};
