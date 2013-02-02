var pg    = require('pg')        // postgre library
  , async = require('async')     // async helper methods
  , uuid  = require('node-uuid') // for creating UUIDs
  , util  = require('util')      // helper methods (such as util.format)
  , conn  = process.env.POSTGRES  || 'tcp://postgres@127.0.0.1/postgres'

// the API object, which got created for each model that
// uses RM/T as its db adapter
var API = function(model, define, callback) {
  // the model the adapter is created for
  this.model       = model
  // the database name
  this.name        = model._type.toLowerCase()
  // model already initialized?
  this.initialized = false
  // a queue for handling multiple async calls
  this.queue       = []
  // the callback, which should be called once a
  // queue entry got executed
  this.callback    = function(err) {
    if (err) throw err
    // once the queue is empty, the model definition is complete
    if (this.queue.length === 0) {
      if (callback) callback()
    }
    // otherwise execute the next queue entry
    else {
      (this.queue.shift())()
    }
  }
    
  var that = this
  process.nextTick(function() {
    // save the list of the models properties
    that.model._properties = Object.keys(that.model.prototype._validation)
  
    // call the adapter specific definition
    if (define)
      define.call(that)
    
    // initialize the model's postgre tables
    that.initialize(function(client) {
      // once initialized:
      pg.connect(conn, function(err, client) {
        if (err) throw err
        that.db = client
      
        // update the P-relation
        that.updatePRelation(function(err) {
          if (err) throw err
          // once the p-relations are updated,
          // work off the queue (if not empty)
          if (that.queue.length > 0) (that.queue.shift())()
          else that.callback()
        })
      })
    })
  })
}

// the method which could be called to update
// the P-relation of a model
API.prototype.updatePRelation = function(callback) {
  var that = this
  // Runs an array of functions in series,
  // each passing their results to the next in the array
  async.waterfall([
    function (next) {
      that.db.query(
        'INSERT INTO rmt_pg (sup, sub) VALUES ($1, $2)',
        [that.name, 'p_' + that.name],
        function(err) {
          if (err) throw err
          next(null)
        }
      )
    }, 
    // fetch the current p-relation scheme informations
    function(next) {
      that.db.query('SELECT column_name, data_type ' +
                    'FROM information_schema.columns ' +
                    'WHERE table_name = $1;', ['p_' + that.name],
      function(err, result) {
        if (err) throw err
        next(null, result.rows)
      })
    },
    // update the p-relation
    function(columns, next) {
      // create a list of needed columns, containing the model's properties
      // and a column for each associative entity
      var properties = that.model._properties.concat(that.model._composes.map(function(sub) {
        return sub._type.toLowerCase()
      }))
      
      // iterate over the existent columns
      columns.forEach(function(column) {
        // skip the id and surrogat column
        if (column.column_name === 'id' || column.column_name === 'surrogat')
          return
        
        var idx
        // is the column still needed?
        if ((idx = properties.indexOf(column.column_name)) !== -1) {
          properties.splice(idx, 1) // column processed -> remove
          // if the data type does not change, do nothing
          if (that.getType(column.column_name) === column.data_type)
            return
          // otherwise re-create the column with the new datatype
          that.queue.push(function() {
            that.db.query(util.format(
              'ALTER TABLE p_%s DROP COLUMN %s;' +
              'ALTER TABLE p_%s ADD COLUMN %s %s',
              that.name, column.column_name, that.name,
              column.column_name, that.getType(column.column_name)
            ), that.callback.bind(that))
          })
        }
        // otherwise create the column
        else {
          that.queue.push(function() {
            that.db.query(util.format(
              'ALTER TABLE p_%s DROP COLUMN %s',
              that.name, column.column_name
            ), that.callback.bind(that))
          })
        }
      })
      
      // at this point, the properties array only contains properties
      // that are entirely new for the the model definition
      properties.forEach(function(prop) {
        if (prop === 'id') return
        // add the column
        that.queue.push(function() {
          that.db.query(util.format(
            'ALTER TABLE p_%s ADD COLUMN %s %s',
            that.name, prop, that.getType(prop)
          ), that.callback.bind(that))
        })
      })
      
      // done
      next(null)
    }
  ], callback)
}

// this method transforms a SWAC type into
// the appropriated Postgre type
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

// this method initialized/resets a model's definition
API.prototype.initialize = function(callback) {
  var that = this
  if (that.initialized === true)
    return callback()
  exports.boostrap(function() {
    pg.connect(conn, function(err, client) {
      if (err) throw err
      async.parallel([
        // reset UGI rows
        client.query.bind(client,
          'DELETE FROM rmt_ugi WHERE sub = $1',
          [that.name]),
        // reset AG rows
        client.query.bind(client,
          'DELETE FROM rmt_ag WHERE sup = $1',
          [that.name]),
        // reset Property Graph rows
        client.query.bind(client,
          'DELETE FROM rmt_pg  WHERE sup = $1',
          [that.name]),
        // create e-Relation if not exists
        client.query.bind(client,
          'CREATE TABLE IF NOT EXISTS e_' + that.name +
          '(Id uuid NOT NULL PRIMARY KEY)'),
        // create empty p-Relation if not exists
        client.query.bind(client,
          'CREATE TABLE IF NOT EXISTS p_' + that.name +
          '(surrogat UUID NOT NULL UNIQUE, id SERIAL PRIMARY KEY)')
      ], function(err) {
        if (err) throw err
        // done
        that.initialized = true
        callback()
      })
    })
  })
}

// this method could be used to extend a model through another one (UGI)
API.prototype.extends = function(label, sup) {
  var that = this
  // "adopt" sups properties
  applyProperties(sup, this.model)
  // add to the list of sups
  this.model._extends.push(sup)
  this.queue.push(function() {
    // insert appropriated row into the UGI table
    that.db.query(
      'INSERT INTO rmt_ugi (sup, label, sub) VALUES ($1, $2, $3)',
      [sup._type.toLowerCase(), label, that.model._type.toLowerCase()],
      that.callback.bind(that)
    )
  })
}

// this method could be used to define associative entities (AG)
API.prototype.composes = function() {
  var subs = Array.prototype.slice.call(arguments)
    , that = this
  // iterate the list of provided subs
  subs.forEach(function(sub) {
    // set the property that is later used to access the sub
    that.model._definition.property(sub._type.toLowerCase())
    // add to the list of subs
    that.model._composes.push(sub)
    that.queue.push(function() {
      // insert appropriated row into the AG table
      that.db.query(
        'INSERT INTO rmt_ag (sup, sub) VALUES ($1, $2)',
        [that.model._type.toLowerCase(), sub._type.toLowerCase()],
        that.callback.bind(that)
      )
    })
  })
}

// this helper could be used to adopt properties of a model
// on to another one
function applyProperties(from, to) {
  // apply the "normal" properties
  from._properties.forEach(function(prop) {
    if (prop === 'id') return // skip the id property
    to._definition.property(prop, from.prototype._validation[prop])
  })
  // apply the sub-accessor property
  from._composes.forEach(function(sub) {
    to._definition.property(sub._type.toLowerCase())
  })
  // recursively apply sup's properties
  from._extends.forEach(function(from) {
    applyProperties(from, to)
  })
}

API.prototype.createModel = function(data) {
  // recursively traverse through sups
  return (function traverse(m, path) {
    // create model
    var model = new m()
    // iterate through associative entities
    m._composes.forEach(function(sub) {
      // traverse through associative entity
      // + set property
      model[sub._type.toLowerCase()] = traverse(sub, path + '_' + sub._type.toLowerCase())
    })
    // iterate through sups
    m._extends.forEach(function(sup) {
      // traverse through sup
      // + set property
      model.updateAttributes(traverse(sup, path + '_' + sup._type.toLowerCase()).toJSON())
    })
    // set properties
    m._properties.forEach(function(prop) {
      model[prop] = data[path + '_' + prop]
    })
    // set surrogate
    model._surrogat = data[path + '_surrogat']
    // done!
    return model
  })(this.model, this.name)
}

// build SELECT Query
API.prototype.buildSqlStatement = function() {
  var that = this, _composes = [], _extends = []
    , _fields = [] , _joins = []
    , i = 0
  // recursively traverse through sups
  !function traverse(m, path) {
    var alias = 'a' + i++
    // iterate through associative entities
    m._composes.forEach(function(sub) {
      // append appropriated join
      _composes.push(sub)
      _joins.push('LEFT OUTER JOIN p_' + sub._type.toLowerCase() + ' AS a' + (i) + ' ON(a' + (i) + '.surrogat = ' + alias + '.' + sub._type.toLowerCase() + ')')
      // continue traversing
      traverse(sub, path + '_' + sub._type.toLowerCase())
    })
    // iterate through sups
    m._extends.forEach(function(sup) {
      // append appropriated join
      _extends.push(sup)
      _joins.push('LEFT OUTER JOIN p_' + sup._type.toLowerCase() + ' AS a' + (i) + ' ON(a' + (i) + '.surrogat = a0.surrogat)')
      // continue traversing
      traverse(sup, path + '_' + sup._type.toLowerCase())
    })
    // add fields
    m._properties.forEach(function(prop) {
      _fields.push(alias + '.' + prop + ' AS ' + path + '_' + prop)
    })
    _fields.push(alias + '.surrogat AS ' + path + '_surrogat')
  }(that.model, that.name)
  
  // put statement together
  return util.format(
    'SELECT %s\nFROM %s\n%s',
    _fields.join(', '),
    'p_' + that.name + ' AS a0',
    _joins.join('\n')
  )
}

// GET specific object from the database
API.prototype.get = function(id, callback) {
  if (!callback) callback = function() {}
  if (!id) return callback(null, null)

  var that = this
  // build SELECT query
  var sql = this.buildSqlStatement()
  sql += '\nWHERE a0.id = ' + id
  // pg.connect kils the current request-domain, so save it to be able
  // to restore it afterwards
  var domain = process.domain
  pg.connect(conn, function(err, client) {
    if (err) throw err
    client.query(sql, function(err, result) {
      // restore previous request-domain
      process.domain = domain
      if (err) return callback(err)
      if (result.rowCount !== 1) return callback(null, null)
      // create model from database result
      callback(null, that.createModel(result.rows[0]))
    })
  })
}

// LIST all objects of a specific type from the database
API.prototype.list = function(/*view, key, callback*/) {
  var args = Array.prototype.slice.call(arguments)
    , that = this
    , callback = args.pop()
    , view = args.shift() || 'all'
    , key = args.shift() || null

  if (!callback) callback = function() {}

  // build SELECT query
  var sql = this.buildSqlStatement()
  console.log(sql)
  // pg.connect kils the current request-domain, so save it to be able
  // to restore it afterwards
  var domain = process.domain
  pg.connect(conn, function(err, client) {
    if (err) throw err
    client.query(sql, function(err, result) {
      // restore previous request-domain
      process.domain = domain
      if (err) return callback(err)
      var rows = []
      // create models from database result
      result.rows.forEach(function(row) {
        rows.push(that.createModel(row))
      })
      callback(null, rows)
    })
  })
}

// Create & Update Method
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
    
  // remove `id` from the list of properties
  properties.splice(properties.indexOf('id'), 1)
  // construct SQL placeholder, e.g. $1, $2, $3, ..., $n
  var i = 0
  for (i = 0; i < properties.length;) values.push('$' + ++i)
  i = 0
  
  // execute the following tasks asynchronously
  async.parallel(
    // create/update associative entities (if not already exists)
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
    })
    // create/update sups (if not already exists)
    .concat(that.model._extends.map(function(sub) {
      return function(done) {
        var m = new sub(props)
        m._surrogat = surrogat
        m.save(function() {
          var toMerge = m.toJSON()
          Object.keys(toMerge).forEach(function(key) {
            if (key === 'id') return
            model[key] = toMerge[key]
          })
          done()
        })
      }
    }))
    // Create E-relation
    .concat(function(done) {
      if (data.id) return done()
      pg.connect(conn, function(err, client) {
        client.query(
          'INSERT INTO e_' + that.model._type.toLowerCase() + ' ' +
          '(id) VALUES ($1)', [surrogat], done
        )
      })
    }),
    // finally:
    function(err) {
      if (err) throw err
      pg.connect(conn, function(err, client) {
        if (err) throw err
        // Execute Insert/Update Query
        client.query(
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
            // restore request-domain
            process.domain = domain
            if (err) return callback(err, null)
            if (!model.id) model.id = body.rows[0].id
            // create resulting model
            model._surrogat = surrogat
            model.isNew = false
            callback(null, model)
          }
        )
      })
    }
  )
}

// DELETE method
API.prototype.delete = function(instance, callback) {
  if (!callback) callback = function() {}
  var that = this
    , domain = process.domain
  
  // execute the following tasks asynchronously
  async.parallel(
    // recursively Delete self & sups
    (function traverse(sub) {
      return [sub._type.toLowerCase()].concat(sub._extends.map(function(sup) {
        return traverse(sup)
      }))
    })(this.model).map(function(type) {
      return function(done) {
        pg.connect(conn, function(err, client) {
          if (err) throw err
          // delete e-relation
          client.query(
            'DELETE FROM e_' + type + ' WHERE id = $1',
            [instance._surrogat],
            function(err) {
              if (err) return done(err)
              // delete p-relation
              client.query(
                'DELETE FROM p_' + type + ' WHERE surrogat = $1',
                [instance._surrogat],
                done
              )
            }
          )
        })
      }
    }),
    // finally:
    function(err) {
      if (err) throw err
      // restore domain
      process.domain = domain
      callback(null)
    }
  )
}

// this is the method that got called by SWAC once a
// model uses `rmt` as its adapter
exports.initialize = function(model, opts, define, callback) {
  // add a hidden surrogat property to each model instance
  Object.defineProperties(model.prototype, {
    _surrogat: { value: null, writable: true }
  })
  
  // add some hidden properties to the model
  Object.defineProperties(model, {
    _composes:   { value: [] }, // AG
    _extends:    { value: [] }, // UGI
    _properties: { value: [], writable: true }
  })
  
  // create the API for the provided model
  return new API(model, define, callback)
}

// bootstrap RM/T by creating a UGI, PG und AG table
// (if not already exists)
var bootstrapped = false
exports.boostrap = function(callback) {
  if (bootstrapped) return callback()
  bootstrapped = true
  pg.connect(conn, function(err, client) {
    if (err) throw err
    async.parallel([
      client.query.bind(client, "CREATE TABLE IF NOT EXISTS rmt_ugi(sup varchar, label varchar, sub varchar, PRIMARY KEY (sup, sub))"),
      client.query.bind(client, "CREATE TABLE IF NOT EXISTS rmt_pg(sup varchar, sub varchar, PRIMARY KEY (sup, sub))"),
      client.query.bind(client, "CREATE TABLE IF NOT EXISTS rmt_ag(sup varchar, sub varchar, PRIMARY KEY (sup, sub))")
    ], function(err) {
      if (err) throw err
      callback()
    })
  })
}