var _ = require('underscore');
_.str = require('underscore.string');

// Find models in data which satisfy the options criteria, 
// then return their indices in order
module.exports = function getMatchIndices(data, options) {
	// Remember original indices
	var origIndexKey = '_waterline_dirty_origindex';
	var matches = _.clone(data);
	_.each(matches, function(model, index) {
		// Determine origIndex key
		// while (model[origIndexKey]) { origIndexKey = '_' + origIndexKey; }
		model[origIndexKey] = index;
	});

	// Query and return result set using criteria
	matches = applyFilter(matches, options.where);
	matches = applySort(matches, options.sort);
	matches = applySkip(matches, options.skip);
	matches = applyLimit(matches, options.limit);
	var matchIndices = _.pluck(matches, origIndexKey);

	// Remove original index key which is keeping track of the index in the unsorted data
	_.each(data, function(datum) {
		delete datum[origIndexKey];
	});
	return matchIndices;
};

// Run criteria query against data set
function applyFilter(data, criteria) {
	if(!data) return data;
	else {
		return _.filter(data, function(model) {
			return matchSet(model, criteria);
		});
	}
}

function applySort(data, sort) {
	if(!sort || !data) return data;
	else {
		var sortedData = _.clone(data);

		// Sort through each sort attribute criteria
		_.each(sort, function(direction, attrName) {

			var comparator;

			// Basic MongoDB-style numeric sort direction
			if(direction === 1 || direction === -1) comparator = function(model) {
				return model[attrName];
			};
			else comparator = comparator;

			// Actually sort data
			sortedData = _.sortBy(sortedData, comparator);


			// Reverse it if necessary (if -1 direction specified)
			if(direction === -1) sortedData.reverse();
		});
		return sortedData;
	}
}

// Ignore the first *skip* models
function applySkip(data, skip) {
	if(!skip || !data) return data;
	else {
		return _.rest(data, skip);
	}
}

function applyLimit(data, limit) {
	if(!limit || !data) return data;
	else {
		return _.first(data, limit);
	}
}


// Match a model against each criterion in a criteria query

function matchSet(model, criteria, parentKey) {

	// Null or {} WHERE query always matches everything
	if(!criteria || _.isEqual(criteria, {})) return true;

	// By default, treat entries as AND
	return _.all(criteria, function(criterion, key) {
		return matchItem(model, key, criterion, parentKey);
	});
}


function matchOr(model, disjuncts) {
	var outcome = false;
	_.each(disjuncts, function(criteria) {
		if(matchSet(model, criteria)) outcome = true;
	});
	return outcome;
}

function matchAnd(model, conjuncts) {
	var outcome = true;
	_.each(conjuncts, function(criteria) {
		if(!matchSet(model, criteria)) outcome = false;
	});
	return outcome;
}

function matchLike(model, criteria) {
	for(var key in criteria) {

		var matchString = criteria[key];

		if(_.isRegExp(matchString)) {
			// awesome
		} else if(_.isString(matchString)) {
			// Handle escaped percent (%) signs
			matchString = matchString.replace(/%%%/g, '%');

			// Replace SQL % match notation with something the ECMA regex parser can handle
			matchString = matchString.replace(/([^%]*)%([^%]*)/g, '$1.*$2');

			// Case insensitive by default
			var modifiers = 'i';
			// TODO: make this overridable
			matchString = new RegExp('^' + matchString + '$', modifiers);
		}
		// Unexpected match string!
		else {
			console.error('matchString:');
			console.error(matchString);
			throw new Error("Unexpected match string: " + matchString + " Please use a regexp or string.");
		}

		// Deal with non-strings by creating index
		var index = model[key];
		if(_.isNumber(index)) index = "" + index;
		else if(_.isBoolean(index)) index = index ? "true" : "false";
		else if(!_.isString(index)) {
			// Ignore objects, arrays, null, and undefined data for now
			// (and maybe forever)
			return false;
		}

		// Check that criterion attribute and is at least similar to the model's value for that attr
		if(!index.match(matchString)) {
			return false;
		}
	}
	return true;
}

function matchNot(model, criteria) {
	return !matchSet(model, criteria);
}

function matchItem(model, key, criterion, parentKey) {

	// Handle special attr query
	if (parentKey) {

		if (key.toLowerCase() === 'not' || key === '!') {
			return matchLiteral(model,parentKey,criterion, function (a,b) {
				return !_.isEqual(a,b);
			});
		}
		else if (key === 'greaterThan' || key === '>') {
			return matchLiteral(model,parentKey,criterion, function (a,b) {
				return a > b;
			});
		}
		else if (key === 'greaterThanOrEqual' || key === '>=')  {
			return matchLiteral(model,parentKey,criterion, function (a,b) {
				return a >= b;
			});
		}
		else if (key === 'lessThan' || key === '<')  {
			return matchLiteral(model,parentKey,criterion, function (a,b) {
				return a < b;
			});
		}
		else if (key === 'lessThanOrEqual' || key === '<=')  {
			return matchLiteral(model,parentKey,criterion, function (a,b) {
				return a <= b;
			});
		}
		else if (key === 'startsWith') return matchLiteral(model,parentKey,criterion, _.str.startsWith);
		else if (key === 'endsWith') return matchLiteral(model,parentKey,criterion, _.str.endsWith);
		else if (key === 'contains') return matchLiteral(model,parentKey,criterion, _.str.contains);
		else throw new Error ('Invalid query syntax!');
	}
	else if(key.toLowerCase() === 'or') {
		return matchOr(model, criterion);
	} else if(key.toLowerCase() === 'not') {
		return matchNot(model, criterion);
	} else if(key.toLowerCase() === 'and') {
		return matchAnd(model, criterion);
	} else if(key.toLowerCase() === 'like') {
		return matchLike(model, criterion);
	}
	// IN query
	else if(_.isArray(criterion)) {
		return _.any(criterion, function(val) {
			return model[key] === val;
		});
	}

	// Special attr query
	else if (_.isObject(criterion) && validSubAttrCriteria(criterion)) {
		// Attribute is being checked in a specific way
		return matchSet(model, criterion, key);
	}

	// Otherwise, try a literal match
	else return matchLiteral(model,key,criterion, _.isEqual);
	
}

// Return whether this criteria is valid as an object inside of an attribute
function validSubAttrCriteria(c) {
	return _.isObject(c) && (
		c.not || c.greaterThan || c.lessThan || 
		c.greaterThanOrEqual || c.lessThanOrEqual ||
		c['<'] || c['<='] || c['!'] || c['>'] || c['>='] ||
		c.startsWith || c.endsWith || c.contains
	);
}


// matchFn => the function that will be run to check for a match between the two literals
function matchLiteral(model, key, criterion, matchFn) {
	// If the criterion is a parsable finite number, cast it
	if(Math.pow(+criterion, 2) > 0) {
		criterion = +criterion;
	}

	// ensure the key attr exists in model
	if(_.isUndefined(model[key])) {
		return false;
	}

	// ensure the key attr matches model attr in model
	else if((! matchFn(model[key],criterion))) {
		return false;
	}

	// Otherwise this is a match
	return true;
}