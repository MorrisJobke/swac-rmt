var pg    = require('pg')
  , async = require('async')
  , uuid  = require('node-uuid')
  , util  = require('util')

var API = function(model, define, callback) {
  this.model       = model
  this.name        = model._type.toLowerCase()
  this.initialized = false
  this.queue       = []
  this.params      = {}
  this.callback    = function(err) {
    if (err) throw err
    if (this.queue.length === 0) {
      if (callback) callback()
    } else {
      (this.queue.shift())()
    }
  }
  var that = this
  this.initialize(function(client) {
    that.db = client
    if (define)
      define.call(that)

    that.updatePRelation(function(err) {
      if (err) throw err
      if (that.queue.length > 0) (that.queue.shift())()
      else that.callback()
    })
  })
}

API.prototype.updatePRelation = function(callback) {
  var that = this
  async.waterfall([
    function(next) {
      that.db.query('SELECT column_name, data_type ' + 
                    'FROM information_schema.columns ' +
                    'WHERE table_name = $1;', ['p_' + that.name],
      function(err, result) {
        if (err) throw err
        next(null, result.rows)
      })
    },
    function(rows, next) {
      var properties = Object.keys(that.model.prototype._validation)
      rows.forEach(function(row) {
        if (row.column_name === 'id' || row.column_name === 'surrogat')
          return

        var idx
        if ((idx = properties.indexOf(row.column_name)) !== -1) {
          properties.splice(idx, 1)
          if (that.getType(row.column_name) === row.data_type)
            return
          //console.log('alter row %s %s %s', that.name, row.column_name, type)
          that.queue.push(function() {
            that.db.query(util.format(
                'ALTER TABLE p_%s DROP COLUMN %s;' +
                'ALTER TABLE p_%s ADD COLUMN %s %s',
                that.name, row.column_name, that.name,
                row.column_name, that.getType(row.column_name)
            ), that.callback.bind(that))
          })
        } else {
          //console.log('drop row %s %s', that.name, row.column_name)
          that.queue.push(function() {
            that.db.query(util.format(
              'ALTER TABLE p_%s DROP COLUMN %s',
              that.name, row.column_name
            ), that.callback.bind(that))
          })
        }
      })

      properties.forEach(function(prop) {
        if (prop === 'id') return
        //console.log('add row %s %s', that.name, prop)
        that.queue.push(function() {
          that.db.query(util.format(
            'ALTER TABLE p_%s ADD COLUMN %s %s',
            that.name, prop, that.getType(prop)
          ), that.callback.bind(that))
        })
      })

      next(null)
    }
  ], callback)
}

API.prototype.getType = function(prop) {
  var validation = this.model.prototype._validation[prop]
  if (!validation.type) return 'character varying'
  switch (validation.type) {
    case 'string':
    default:
      return 'character varying'
  }
}

API.prototype.initialize = function(callback) {
  if (this.initialized === true)
    return
  var that = this
  exports.boostrap(function(client) {
    if (that.initialized === true)
      return callback(client)
    async.parallel([
      // reset
      client.query.bind(client,
        'DELETE FROM ugi WHERE sub = $1',
        [that.model._type]),
      client.query.bind(client,
        'DELETE FROM ag WHERE sup = $1',
        [that.model._type]),
      client.query.bind(client,
        'DELETE FROM pg  WHERE sup = $1',
        [that.model._type]),
      client.query.bind(client,
        'CREATE TABLE IF NOT EXISTS e_' + that.name +
        '(Id uuid NOT NULL PRIMARY KEY)'),
      client.query.bind(client,
        'CREATE TABLE IF NOT EXISTS p_' + that.name +
        '(surrogat UUID NOT NULL UNIQUE, id SERIAL PRIMARY KEY)')
    ], function(err) {
      if (err) throw err
      that.initialized = true
      callback(client)
    })
  })
}

API.prototype.extends = function(label, sup) {
  var that = this
  this.queue.push(function() {
    that.db.query(
      'INSERT INTO psychic_UGI (sup, label, sub) VALUES ($1, $2, $3)',
      [sup._type, label, this.name]
    )
  })
}

API.prototype.composes = function() {
  var subs = Array.prototype.slice.call(arguments)
    , that = this
  this.queue.push(
    subs.map(function(sub) {
      return function() {
        that.db.query(
          'INSERT INTO psychic_AG (sup, sub) VALUES ($1, $2)',
          [that.name, sub._type]
        )
        that.callback()
      }
    })
  )
}

API.prototype.createModel = function(id, data, rev) {
  data.id        = this.extractId(id)
  var instance   = new this.model(data)
  instance._id   = id
  instance._rev  = rev
  instance.isNew = false
  return instance
}

API.prototype.get = function(id, callback) {
  if (!callback) callback = function() {}
  if (!id) return callback(null, null)

  var that = this
  this.db.get(this.adaptId(id), function(err, body) {
    if (err) {
      switch (err.message) {
        case 'missing':
        case 'deleted':
          return callback(null, null)
        default:
          return callback(err, null)
      }
    }

    callback(null, that.createModel(body._id, body, body._rev))
  })
}

API.prototype.list = function(/*view, key, callback*/) {
  var args = Array.prototype.slice.call(arguments)
    , that = this
    , callback = args.pop()
    , view = args.shift() || 'all'
    , key = args.shift() || null
    , params = this.params[view]

  if (key)       params.key = key
  if (!callback) callback = function() {}
  
  var fn
  if (this.views[view])
    fn = this.views[view].bind(this.db, key, process.domain.req)
  else
    fn = this.db.view.bind(this.db, this.model._type, view, params)

  fn(function(err, body) {
    if (err) return callback(err, null)
    if (!body || !body.rows || !Array.isArray(body.rows))
      return callback(null, body || null)
    var rows = []
    body.rows.forEach(function(data) {
      var doc = data.value || data.doc
      rows.push(that.createModel(doc._id, doc, doc._rev))
    })
    callback(null, rows)
  })
}

API.prototype.put = function(instance, callback) {
  if (!callback) callback = function() {}
  var data   = instance.toJSON(true)
  data._rev  = instance._rev
  data._id   = instance._id
  data.$type = instance._type
  delete data.id
  this.db.insert(data, instance._id, function(err, res) {
    if (err) return callback(err, null)
    instance._rev  = res.rev
    instance.isNew = false
    callback(null, instance)
  })
}

API.prototype.post = function(props, callback) {
  if (!callback) callback = function() {}

  var model = props instanceof this.model ? props : new this.model(props)
  props = model.toJSON(true)

  if (props.id) props._id = this.model._type + '/' + props.id
  delete props.id
  props.$type = this.model._type

  var that = this
  this.db.insert(props, props._id, function(err, body) {
    if (err) return callback(err, null)
    if (!model.id) model.id = body.id
    model._id = body.id
    model._rev = body.rev
    model.isNew = false
    callback(null, model)
  })
}


API.prototype.delete = function(instance, callback) {
  if (!callback) callback = function() {}
  this.db.destroy(instance._id, instance._rev, function(err) {
    if (err) return callback(err)
    callback(null)
  })
}

exports.initialize = function(model, opts, define, callback) {
  var api = new API(model, define, callback)

  // Object.defineProperties(model.prototype, {
  //   _id:  { value: null, writable: true },
  //   _rev: { value: null, writable: true } 
  // })

  return api
}

var bootstrapped = false
exports.boostrap = function(callback) {
  pg.connect('tcp://markus@alice/postgres', function(err, client) {
    if (err) throw err
    if (bootstrapped) return callback(client)
    async.parallel([
      client.query.bind(client, "CREATE TABLE IF NOT EXISTS ugi(sup varchar, label varchar, sub varchar, PRIMARY KEY (sup, sub))"),
      client.query.bind(client, "CREATE TABLE IF NOT EXISTS pg(sup varchar, sub varchar, PRIMARY KEY (sup, sub))"),
      client.query.bind(client, "CREATE TABLE IF NOT EXISTS ag(sup varchar, sub varchar, PRIMARY KEY (sup, sub))")
    ], function(err) {
      if (err) throw err
      bootstrapped = true
      callback(client)
    })
  })
}