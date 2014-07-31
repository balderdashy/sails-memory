/*---------------------------------------------------------------
  :: DiskAdapter
  -> adapter

  This disk adapter is for development only!
---------------------------------------------------------------*/

var Database = require('./database');
var Errors = require('waterline-errors').adapter;
var _runJoins = require('waterline-cursor');

module.exports = (function () {

  // Hold connections for this adapter
  var connections = {};

  var adapter = {

    identity: 'sails-memory',

    // Which type of primary key is used by default
    pkFormat: 'integer',

    // Whether this adapter is syncable (yes)
    syncable: true,

    // How this adapter should be synced
    migrate: 'alter',

    // Allow a schemaless datastore
    defaults: {
      schema: false
    },

    // Register A Connection
    registerConnection: function (connection, collections, cb) {

      if(!connection.identity) return cb(Errors.IdentityMissing);
      if(connections[connection.identity]) return cb(Errors.IdentityDuplicate);

      connections[connection.identity] = new Database(connection, collections);
      connections[connection.identity].initialize(cb);
    },

    teardown: function (conn, cb) {

      if (typeof conn == 'function') {
        cb = conn;
        conn = null;
      }
      if (conn == null) {
        connections = {};
        return cb();
      }
      if(!connections[conn]) return cb();
      delete connections[conn];
      cb();
    },

    // Return attributes
    describe: function (conn, coll, cb) {
      grabConnection(conn).describe(coll, cb);
    },

    define: function (conn, coll, definition, cb) {
      grabConnection(conn).createCollection(coll, definition, cb);
    },

    drop: function (conn, coll, relations, cb) {
      grabConnection(conn).dropCollection(coll, relations, cb);
    },

    join: function (conn, coll, criteria, _cb) {

      // Ensure nextTick
      var cb = AFTERDELAY(_cb);

      var db = grabConnection(conn);

      var parentIdentity = coll;

      // Populate associated records for each parent result
      // (or do them all at once as an optimization, if possible)
      _runJoins({

        instructions: criteria,
        parentCollection: parentIdentity,

        /**
         * Find some records directly (using only this adapter)
         * from the specified collection.
         *
         * @param  {String}   collectionIdentity
         * @param  {Object}   criteria
         * @param  {Function} cb
         */
        $find: function (collectionIdentity, criteria, cb) {
          return db.select(collectionIdentity, criteria, cb);
        },

        /**
         * Look up the name of the primary key field
         * for the collection with the specified identity.
         *
         * @param  {String}   collectionIdentity
         * @return {String}
         */
        $getPK: function (collectionIdentity) {
          if (!collectionIdentity) return;
          return db.getPKField(collectionIdentity);
        }
      }, cb);

    },

    find: function (conn, coll, options, cb) {
      grabConnection(conn).select(coll, options, cb);
    },

    create: function (conn, coll, values, cb) {
      grabConnection(conn).insert(coll, values, cb);
    },

    update: function (conn, coll, options, values, cb) {
      grabConnection(conn).update(coll, options, values, cb);
    },

    destroy: function (conn, coll, options, cb) {
      grabConnection(conn).destroy(coll, options, cb);
    }

  };

  /**
   * Grab the connection object for a connection name
   *
   * @param {String} connectionName
   * @return {Object}
   * @api private
   */

  function grabConnection(connectionName) {
    return connections[connectionName];
  }

  /**
   * Return a function that stalls for one milisecond before
   * calling `cb` with the expected arguments and context.
   * @param {Function} cb
   * @return {Function}
   */
  function AFTERDELAY(cb) {
    return function ( /* ... */ ) {
      var origCtx = this;
      var origArgs = Array.prototype.slice.call(arguments);
      setTimeout(function () {
        cb.apply(origCtx, origArgs);
      }, 1);
    };
  }

  return adapter;
})();
