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
          
            // If we're grouping
            if(options.groupBy || options.sum || options.average || options.min || options.max) {
              // Check if we have calculations to do
              if(!options.sum && !options.average && !options.min && !options.max) {
                return cb(new Error('Cannot groupBy without a calculation'));
              }
              
              // First we groupBy
              
              // grouped results is our current resultSet, split up by group
              var groupedResults = [];
              
              // finished results is our generated results (with sums, evgs, etc)
              var finishedResults = [];
              
              if(options.groupBy) {
                var groups = [];
                var groupCollector = {};
                
                // Go through the results
                resultSet.forEach(function(item){
                  var key = '';
                  options.groupBy.forEach(function(groupKey){
                    key += item[groupKey] + '---';
                  });
                  if(groupCollector[key]) {
                    groupCollector[key].push(item);
                  } else {
                    groupCollector[key] = [item];
                  }
                });

                for(var key in groupCollector) {
                  groups.push(groupCollector[key]);
                }
                
                groupedResults = groups;
                
                // Then we generate stub objects for adding/averaging
                groups.forEach(function(group){
                  var stubResult = {};
                  
                  // Groupresult will look like this: { type: 'count', a2: 'test' }
                  options.groupBy.forEach(function(groupKey) {
                    
                    // Set the grouped by value to the value of the first results
                    stubResult[groupKey] = group[0][groupKey];
                  });
                  
                 finishedResults.push(stubResult);
                });
                
              } else {
                groupedResults = [resultSet];
                finishedResults = [{}];
              }
              
              // sum all the things (specified)
              if(options.sum) {
                
                // fill in our stub object with those keys, set to sum 0
                options.sum.forEach(function(sumKey) {
                  finishedResults.forEach(function(stub) {
                    stub[sumKey] = 0;
                  });
                });
                
                // iterate over all groups of data
                groupedResults.forEach(function(group, i) {
                  
                  // sum for each item
                  group.forEach(function(item) {
                    options.sum.forEach(function(sumKey) {
                      if(typeof item[sumKey] === 'number') {
                        finishedResults[i][sumKey]+=item[sumKey];
                      }
                    });
                  });
                });
                
              }
              
              if(options.average) {
                
                // fill in our stub object with those keys, set to sum 0
                options.average.forEach(function(sumKey) {
                  finishedResults.forEach(function(stub) {
                    stub[sumKey] = 0;
                  });
                });
                
                // iterate over all groups of data
                groupedResults.forEach(function(group, i) {
                  options.average.forEach(function(sumKey) {
                    
                    // count up how many numbers we have, so we know how much to divide by
                    var cnt = 0;

                    // average for each item
                    group.forEach(function(item) {
                      if(typeof item[sumKey] === 'number') {
                        finishedResults[i][sumKey]+=item[sumKey];
                        cnt+=1;
                      }
                    });
                    
                    finishedResults[i][sumKey]/=cnt;
                  });
                });
              }
              
              if(options.min) {
                
                // iterate over all groups of data
                groupedResults.forEach(function(group, i) {
                  options.min.forEach(function(sumKey) {
                    
                    // keep track of current minimum
                    var min = Infinity;

                    // update min
                    group.forEach(function(item) {
                      if(typeof item[sumKey] === 'number') {
                        if(item[sumKey] < min) {
                          min = item[sumKey];
                        }
                      }
                    });
                    
                    finishedResults[i][sumKey] = isFinite(min) ? min : null;
                  });
                });
              }
              
              if(options.max) {
                
                // iterate over all groups of data
                groupedResults.forEach(function(group, i) {
                  options.max.forEach(function(sumKey) {
                    
                    // keep track of current maximum
                    var max = -Infinity;

                    // update max
                    group.forEach(function(item) {
                      if(typeof item[sumKey] === 'number') {
                        if(item[sumKey] > max) {
                          max = item[sumKey];
                        }
                      }
                    });
                    
                    finishedResults[i][sumKey] = isFinite(max) ? max : null;
                  });
                });
              }
              
              resultSet = finishedResults;
            }

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


