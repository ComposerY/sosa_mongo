var crypto = require('crypto');

module.exports = function (coll_name, backend_options) {
  backend_options || (backend_options = {});

  if (!backend_options.db) throw new Error('must pass a node-mongodb-native db with backend_options.db');
  var db = backend_options.db;

  function hash (obj) {
    return crypto.createHash('sha1')
      .update(JSON.stringify(obj))
      .digest('base64')
      .replace(/[\+\/=]/g, '')
      .substring(12);
  }

  var coll = db.collection(coll_name);
  
  function toObjId (id) {
    if (backend_options.key_prefix && backend_options.key_prefix.length) {
      return hash(backend_options.key_prefix) + '__' + hash(id);
    }
    return hash(id);
  }

  return {
    load: function (id, opts, cb) {
      opts.fields || (opts.fields = {});
      opts.fields._id = 0;
      coll.findOne({_id: toObjId(id)}, opts, cb);
    },
    save: function (id, obj, opts, cb) {
      if (typeof opts.upsert === 'undefined') opts.upsert = true;
      if (typeof opts.projection === 'undefined') opts.projection = {};
      if (typeof opts.projection._id === 'undefined') opts.projection._id = 0;
      if (typeof opts.returnOriginal === 'undefined') opts.returnOriginal = false;
      coll.findOneAndReplace({_id: toObjId(id)}, obj, opts, function (err, doc) {
        cb(err, doc && doc.value || null);
      });
    },
    destroy: function (id, opts, cb) {
      if (typeof opts.w === 'undefined') opts.w = 1;
      this.load(id, {}, function (err, obj) {
        if (err) return cb(err);
        if (!obj) return cb(null, null);
        coll.deleteOne({_id: toObjId(id)}, opts, function (err) {
          if (err) return cb(err);
          cb(null, obj);
        });
      });
    },
    select: function (opts, cb) {
      if (typeof opts.query === 'undefined') opts.query = {};
      if (backend_options.key_prefix && backend_options.key_prefix.length) {
        // use an indexed regex to restrict query to key prefix.
        opts.query._id = {'$regex': new RegExp('^' + hash(backend_options.key_prefix) + '__')};
      }
      var cursor = coll.find(opts.query);
      if (typeof opts.project === 'undefined') opts.project = {};
      if (typeof opts.project._id === 'undefined') opts.project._id = 0;
      cursor = cursor.project(opts.project);
      if (typeof opts.comment === 'string') cursor = cursor.comment(opts.comment);
      if (typeof opts.hint === 'object') cursor = cursor.hint(opts.hint);
      if (typeof opts.limit === 'number') cursor = cursor.limit(opts.limit);
      if (typeof opts.skip === 'number') cursor = cursor.skip(opts.skip);
      if (typeof opts.sort === 'object') cursor = cursor.sort(opts.sort);
      cursor.toArray(cb);
    }
  };
};
