var crypto = require('crypto');

module.exports = function (coll_name, backend_options) {
  backend_options || (backend_options = {});

  if (!backend_options.db) throw new Error('must pass a node-mongodb-native db with backend_options.db');
  var db = backend_options.db;

  function escapeBase64 (str) {
    return str.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '')
  }

  function hash (id) {
    return escapeBase64(crypto.createHash('sha1').update(id).digest('base64'))
  }

  var coll_path = coll_name;
  if (backend_options.key_prefix && backend_options.key_prefix.length) {
    coll_path += '.' + backend_options.key_prefix.map(hash).join('.')
  }

  var coll = db.collection(coll_path);

  return {
    load: function (id, opts, cb) {
      opts.fields || (opts.fields = {});
      opts.fields._id = 0;
      coll.findOne({_id: id}, opts, cb);
    },
    save: function (id, obj, opts, cb) {
      if (typeof opts.upsert === 'undefined') opts.upsert = true;
      if (typeof opts.projection === 'undefined') opts.projection = {};
      if (typeof opts.projection._id === 'undefined') opts.projection._id = 0;
      if (typeof opts.returnOriginal === 'undefined') opts.returnOriginal = false;
      coll.findOneAndReplace({_id: id}, obj, opts, function (err, doc) {
        cb(err, doc && doc.value || null);
      });
    },
    destroy: function (id, opts, cb) {
      if (typeof opts.w === 'undefined') opts.w = 1;
      this.load(id, {}, function (err, obj) {
        if (err) return cb(err);
        if (!obj) return cb(null, null);
        coll.deleteOne({_id: id}, opts, function (err) {
          if (err) return cb(err);
          cb(null, obj);
        });
      });
    },
    select: function (opts, cb) {
      if (typeof opts.query === 'undefined') opts.query = {};
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
