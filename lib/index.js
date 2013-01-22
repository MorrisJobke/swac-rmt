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
    that.model._properties = Object.keys(that.model.prototype._validation)
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
      var properties = that.model._properties.concat(that.model._composes.map(function(sub) {
        return sub._type.toLowerCase()
      }))

      rows.forEach(function(row) {
        if (row.column_name === 'id' || row.column_name === 'surrogat')
          return

        var idx
        if ((idx = properties.indexOf(row.column_name)) !== -1) {
          properties.splice(idx, 1)
          if (that.getType(row.column_name) === row.data_type)
            return
          // console.log('alter row %s %s %s', that.name, row.column_name, row.data_type)
          that.queue.push(function() {
            that.db.query(util.format(
                'ALTER TABLE p_%s DROP COLUMN %s;' +
                'ALTER TABLE p_%s ADD COLUMN %s %s',
                that.name, row.column_name, that.name,
                row.column_name, that.getType(row.column_name)
            ), that.callback.bind(that))
          })
        } else {
          // console.log('drop row %s %s', that.name, row.column_name)
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
        // console.log('add row %s %s', that.name, prop)
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
  if (this.model._properties.indexOf(prop) === -1) return 'uuid'
  switch (validation.type) {
    case 'number':
    case 'integer':
      return 'integer'
    case 'boolean':
      return 'boolean'
    case 'object':
    case 'null':
    case 'any':
    case 'array':
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
        'DELETE FROM rmt_ugi WHERE sub = $1',
        [that.model._type]),
      client.query.bind(client,
        'DELETE FROM rmt_ag WHERE sup = $1',
        [that.model._type]),
      client.query.bind(client,
        'DELETE FROM rmt_pg  WHERE sup = $1',
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
  applyProperties(sup, this.model)
  this.model._extends.push(sup)
  this.queue.push(function() {
    that.db.query(
      'INSERT INTO rmt_ugi (sup, label, sub) VALUES ($1, $2, $3)',
      [sup._type, label, that.model._type],
      that.callback.bind(that)
    )
  })
}

API.prototype.composes = function() {
  var subs = Array.prototype.slice.call(arguments)
    , that = this
  subs.forEach(function(sub) {
    that.model._definition.property(sub._type.toLowerCase())
    that.model._composes.push(sub)
    that.queue.push(function() {
      that.db.query(
        'INSERT INTO rmt_ag (sup, sub) VALUES ($1, $2)',
        [that.model._type, sub._type],
        that.callback.bind(that)
      )
    })
  })
}

function applyProperties(from, to) {
  from._properties.forEach(function(prop) {
    if (prop === 'id') return
    to._definition.property(prop, from.prototype._validation[prop])
  })
  from._composes.forEach(function(sub) {
    to._definition.property(sub._type.toLowerCase())
  })
  from._extends.forEach(function(from) {
    applyProperties(from, to)
  })
}

API.prototype.createModel = function(data) {
  return (function traverse(m, path) {
    // TODO: SURROGATE!!!111elf
    var model = new m()
    m._composes.forEach(function(sub) {
      model[sub._type.toLowerCase()] = traverse(sub, path + '_' + sub._type.toLowerCase())
    })
    m._extends.forEach(function(sup) {
      model.updateAttributes(traverse(sup, path + '_' + sup._type.toLowerCase()).toJSON())
    })
    m._properties.forEach(function(prop) {
      model[prop] = data[path + '_' + prop]
    })
    model._surrogat = data[path + '_surrogat']
    return model
  })(this.model, this.name)
}

API.prototype.buildSqlStatement = function() {
  var that = this, _composes = [], _extends = []
    , _fields = [] , _tables = [], _joins = []
    , i = 0
  !function traverse(m, path) {
    var alias = 'a' + i++
    _tables.push(m._type.toLowerCase() + ' AS ' + alias)
    m._composes.forEach(function(sub) {
      _composes.push(sub)
      _joins.push('LEFT OUTER JOIN p_' + sub._type.toLowerCase() + ' AS a' + (i) + ' ON(a' + (i) + '.surrogat = ' + alias + '.' + sub._type.toLowerCase() + ')')
      traverse(sub, path + '_' + sub._type.toLowerCase())
    })
    m._extends.forEach(function(sup) {
      _extends.push(sup)
      _joins.push('LEFT OUTER JOIN p_' + sup._type.toLowerCase() + ' AS a' + (i) + ' ON(a' + (i) + '.surrogat = a0.surrogat)')
      traverse(sup, path + '_' + sup._type.toLowerCase())
    })
    m._properties.forEach(function(prop) {
      _fields.push(alias + '.' + prop + ' AS ' + path + '_' + prop)
    })
    _fields.push(alias + '.surrogat AS ' + path + '_surrogat')
  }(that.model, that.name)

  return util.format(
    'SELECT %s\nFROM %s\n%s',
    _fields.join(', '),
    'p_' + that.name + ' AS a0',
    _joins.join('\n')
  )
}

API.prototype.get = function(id, callback) {
  if (!callback) callback = function() {}
  if (!id) return callback(null, null)

  var that = this
  var sql = this.buildSqlStatement()
  sql += '\nWHERE a0.id = ' + id
  var domain = process.domain
  exports.boostrap(function(client) {
    client.query(sql, function(err, result) {
      process.domain = domain
      if (err) return callback(err)
      if (result.rowCount !== 1) return callback(null, null)
      client.end()
      callback(null, that.createModel(result.rows[0]))
    })
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

  var sql = this.buildSqlStatement()
  var domain = process.domain
  exports.boostrap(function(client) {
    client.query(sql, function(err, result) {
      process.domain = domain
      if (err) return callback(err)
      var rows = []
      result.rows.forEach(function(row) {
        rows.push(that.createModel(row))
      })
      client.end()
      callback(null, rows)
    })
  })
}

API.prototype.post = API.prototype.put = function(props, callback) {
  if (!callback) callback = function() {}
  var that = this
    , model = props instanceof this.model ? props : new this.model(props)
    , data = model.toJSON(true)
    , surrogat = props._surrogat || uuid.v4()
    , properties = this.model._properties.concat(this.model._composes.map(function(sub) {
      return sub._type.toLowerCase()
    }))
    , values = []
    , domain = process.domain

  properties.splice(properties.indexOf('id'), 1)
  var i = 0
  for (i = 0; i < properties.length;) values.push('$' + ++i)
  i = 0
  exports.boostrap(function(client) {
    async.parallel(
      that.model._composes.map(function(sub) {
        return function(done) {
          var prop = sub._type.toLowerCase()
            , value = props[prop]
          if (value && value._type !== sub._type)
            throw new Error('invalid property ' + prop)
          if (!value) return done()
          if (value.isNew) {
            value.save(function() {
              data[prop] = value._surrogat
              done()
            })
          } else {
            data[prop] = value._surrogat
            done()
          }
        }
      }),
      function() {
        async.parallel(
          that.model._extends.map(
            function(sub) {
              var m = new sub(props)
              m._surrogat = surrogat
              return m.save.bind(m)
            }
          ),
          client.query.bind(client,
            !data.id
            ? 'INSERT INTO p_' + that.model._type.toLowerCase() + ' ' +
              '(surrogat, id, ' + properties.join(', ') + ') ' +
              'VALUES (\'' + surrogat + '\', DEFAULT, ' + values.join(', ') + ') ' +
              'RETURNING id'
            : 'UPDATE p_' + that.model._type.toLowerCase() + ' ' +
              'SET ' + properties.map(function(prop) {
                return prop + '=' + values[i++]
              }).join(', ') + ' ' +
              'WHERE surrogat=\'' + surrogat + '\'',
            properties.map(function(prop) {
              return data[prop]
            }),
            function(err, body) {
              process.domain = domain
              if (err) return callback(err, null)
              if (!model.id) model.id = body.rows[0].id
              model._surrogat = surrogat
              model.isNew = false
              client.end()
              callback(null, model)
            }
          )
        )
      }
    )
  })
}


API.prototype.delete = function(instance, callback) {
  if (!callback) callback = function() {}
  var that = this
    , domain = process.domain
  exports.boostrap(function(client) {
    async.parallel(
      (function traverse(sub) {
        var sups = sub._extends.map(function(sup) {
          return sup._type.toLowerCase()
        })
        return sups.concat.apply(sups, sub._extends.map(function(sup) {
          return traverse(sup)
        }))
      })(this.model).map(function(type) {
        return client.query.bind(that.db,
          'DELETE FROM p_' + type + ' WHERE surrogat = $1',
          [instance._surrogat])
      }),
      function(err) {
        if (err) throw err
        process.domain = domain
        client.end()
        callback(null)
      }
    )
  })
}

exports.initialize = function(model, opts, define, callback) {
  var api = new API(model, define, callback)

  Object.defineProperties(model.prototype, {
    _surrogat: { value: null, writable: true }
  })

  Object.defineProperties(model, {
    _composes: { value: [] },
    _extends:  { value: [] },
    _properties: { value: [], writable: true }
  })

  return api
}

var bootstrapped = false
exports.boostrap = function(callback) {
  pg.connect(process.env.POSTGRES  || 'tcp://postgres@127.0.0.1/postgres', function(err, client) {
    if (err) throw err
    if (bootstrapped) return callback(client)
    bootstrapped = true
    async.parallel([
      client.query.bind(client, "CREATE TABLE IF NOT EXISTS rmt_ugi(sup varchar, label varchar, sub varchar, PRIMARY KEY (sup, sub))"),
      client.query.bind(client, "CREATE TABLE IF NOT EXISTS rmt_pg(sup varchar, sub varchar, PRIMARY KEY (sup, sub))"),
      client.query.bind(client, "CREATE TABLE IF NOT EXISTS rmt_ag(sup varchar, sub varchar, PRIMARY KEY (sup, sub))")
    ], function(err) {
      if (err) throw err
      callback(client)
    })
  })
}