/*---------------------------------------------------------------
	:: MemoryAdapter
	-> adapter

	This memory adapter is for development only!
---------------------------------------------------------------*/

var _ = require('lodash');


module.exports = (function () {

	// Load criteria module
	var getMatchIndices = require('waterline-criteria');

	// Maintain connections to open file and memory stores
	var connections = {	};

	// In memory representation of the data model
	var data = { };
	var schema = { };
	var counters = { };

	var adapter = {

		// Whether this adapter is syncable (yes)
		syncable: true,

		// How this adapter should be synced
		migrate: 'alter',

		// Default configuration for collections
		defaults: {
			schema: false
		},

		registerCollection: function (collection, cb) {
			// Save reference to collection so we have it
			schema[collection.identity] = collection;
			cb();
		},

		// Return attributes
		describe: function (collectionName, cb) {
			cb(null, schema[collectionName].attributes);
		},

		// Adapters are not responsible for checking for existence of the collection
		define: function (collectionName, definition, cb) {

			data[collectionName] = [];
			counters[collectionName] = {};
			schema[collectionName].attributes = _.clone(definition);

			cb(null, schema[collectionName].attributes);
		},

		drop: function (collectionName, cb) {
			
			delete data[collectionName];
			delete schema[collectionName].attributes;
			delete counters[collectionName];

			cb();
		},

		find: function (collectionName, options, cb) {

			// Get indices from original data which match, in order
			var matchIndices = getMatchIndices(data[collectionName],options);

			var resultSet = [];
			_.each(matchIndices,function (matchIndex) {
				resultSet.push(_.clone(data[collectionName][matchIndex]));
			});

			cb(null, resultSet);
		},

		create: function (collectionName, values, cb) {

			for (var attrName in schema[collectionName].attributes) {

				var attrDef = schema[collectionName].attributes[attrName];

				if (attrDef.unique) {
					for (var index in data[collectionName]) {
						
						// Ignore uniquness check on undefined values
						// ('required' check is taken care of in waterline core)
						if (_.isUndefined(values[attrName])) {
							continue;
						}

						if (values[attrName] === data[collectionName][index][attrName]) {
							return cb('Uniqueness check failed on attribute: ' + attrName + ' with value: ' + values[attrName]);
						}
					}
				}

				// Only apply autoIncrement if value is not specified
				if (attrDef.autoIncrement && !values[attrName]) {

					// Increment AI counter
					if (counters[collectionName][attrName]) {
						counters[collectionName][attrName]++;
					}
					else counters[collectionName][attrName] = 1;

					// Set data to current auto-increment value
					values[attrName] = counters[collectionName][attrName];

				}
			}

			data[collectionName].push(values);

			cb(null, values);
		},

		update: function (collectionName, options, values, cb) {

			// Get indices from original data which match, in order
			var matchIndices = getMatchIndices(data[collectionName],options);

			var resultSet = [];
			_.each(matchIndices,function (matchIndex) {
				data[collectionName][matchIndex] = _.extend(data[collectionName][matchIndex], values);
				resultSet.push(_.clone(data[collectionName][matchIndex]));
			});

			cb(null, resultSet);
		},

		destroy: function (collectionName, options, cb) {

			// Get indices from original data which match, in order
			var matchIndices = getMatchIndices(data[collectionName], options);

			// Delete data which matches the criteria
			data[collectionName] = _.reject(data[collectionName], function (model, i) {
				return _.contains(matchIndices, i);
			});

			cb();
		}

	};

	return adapter;

})();


