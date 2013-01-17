var Arkansas = require('arkansas')
  , should   = require('should')
  , pg       = require('pg')
  , async    = require('async')
  , client

var domain = require('domain')
  , d = domain.create()
d.req = {}

var domainify = function(fn) {
  return function(done) {
    d.run(fn.bind(null, done))
  }
}

describe('Arkansas PostgreSql RM/T Adapter', function() {
  before(function(done) {
    pg.connect('tcp://markus@alice/postgres', function(err, c) {
      if (err) throw err
      client = c
      done()
    })
  })
  // after(function(done) {
  //   nano.db.destroy('arkansas-couchdb-test', done)
  // })
  var Fahrzeug, Auto, Rad, Motor
  describe('Model Definition', function() {
    before(function(done) {
      async.series([
        function(done) {
          Rad = Arkansas.Model.define('Rad', function() {
            this.use(require('../'), { db: 'postgre' })
            this.property('umfang')
          }, done)
        },
        function(done) {
          Motor = Arkansas.Model.define('Motor', function() {
            this.use(require('../'), { db: 'postgre' })
            this.property('leistung')
          }, done)
        },
        function(done) {
          Fahrzeug = Arkansas.Model.define('Fahrzeug', function() {
            this.use(require('../'), { db: 'postgre' }, function() {
              this.composes(Rad, Motor)
            })
            this.property('hersteller')
          }, done)
        },
        function(done) {
          Auto = Arkansas.Model.define('Auto', function() {
            this.use(require('../'), { db: 'postgre' }, function() {
              this.extends('is-a', Fahrzeug)
            })
            this.property('farbe')
          }, done)
        }
      ], done)
    })
    it('should initialize global tables (AG, PG, UGI)', function(done) {
      client.query(
        'SELECT DISTINCT COUNT(table_name) FROM information_schema.tables WHERE table_name LIKE \'rmt\\_%\';',
        function(err, result) {
          if(err) throw err
          result.rows[0].count.should.equal(3)
          done()
        }
      )
    })
    it('should create a P-Relation and a E-Relation if not exists', function(done) {
      client.query(
        'SELECT DISTINCT table_name FROM information_schema.tables ' +
        'WHERE table_name SIMILAR TO \'(p|e)\\_%\';',
        function(err, result) {
          if(err) throw err
          var tables = result.rows.map(function(a){return a.table_name})
          tables.length.should.equal(8)
          for(var i in tables)
            tables[i].should.match(/^(p|e)_(auto|motor|rad|fahrzeug)$/)
          done()
        }
      )
    })
    it('should create all specified columns', function(done) {
      client.query(
        'SELECT DISTINCT table_name, column_name FROM information_schema.columns ' +
        'WHERE table_name SIMILAR TO \'(p|e)\\_%\';',
        function(err, result) {
          if(err) throw err
          var columns = result.rows
          columns.length.should.equal(16)
          for(var i in columns) {
            var table = columns[i]
            if(table.table_name.match(/^e_.*/))
              table.column_name.should.equal('id')
            else
              table.column_name.should.match(/^(id|surrogat|leistung|umfang|hersteller|farbe)$/)
          }
          done()
        }
      )
    })
    it('should create AG entries', function(done) {
      client.query(
        'SELECT * FROM rmt_ag;',
        function(err, result) {
          if(err) throw err
          result.rows.length.should.equal(2)
          for(var i in result.rows) {
            var e = result.rows[i]
            e.sup.should.equal('Fahrzeug')
            e.sub.should.match(/^(Rad|Motor)$/)
          }
          done()
        }
      )
    })
    it.skip('should create PG entries', function(done) {
      client.query(
        'SELECT * FROM rmt_pg;',
        function(err, result) {
          if(err) throw err
          var pgs = result.rows
          pgs.length.should.equal(4)
          for(var i in pgs)
            pgs[i].sub.should.equal('p_' + pgs[i].sup.toLowerCase())
          done()
        }
      )
    })
    it('should create UGI entries', function(done) {
      client.query(
        'SELECT * FROM rmt_ugi;',
        function(err, result) {
          if(err) throw err
          result.rows.length.should.equal(1)
          var e = result.rows[0]
          e.sup.should.equal('Fahrzeug')
          e.label.should.equal('is-a')
          e.sub.should.equal('Auto')
          done()
        }
      )
    })
  })
  describe.skip('CRUD', function() {
    var cur
    it('POST should work', domainify(function(done) {
      model.post({ key: '1', type: 'a' }, function(err, row) {
        should.not.exist(err)
        cur = row
        db.get(row.id, function(err, body) {
          if (err) throw err
          body.key.should.equal(row.key)
          body.type.should.equal(row.type)
          done()
        })
      })
    }))
    it('PUT should work', domainify(function(done) {
      cur.key = 2
      cur.type = 'b'
      model.put(cur.id, cur, function(err, row) {
        should.not.exist(err)
        db.get(row.id, function(err, body) {
          if (err) throw err
          body.key.should.equal(cur.key)
          body.type.should.equal(cur.type)
          done()
        })
      })
    }))
    it('GET should work', domainify(function(done) {
      model.get(cur.id, function(err, body) {
        should.not.exist(err)
        body.id.should.equal(cur.id)
        body.key.should.equal(cur.key)
        body.type.should.equal(cur.type)
        done()
      })
    }))
    it('LIST should work', domainify(function(done) {
      model.post({ key: '1', type: 'a' }, function(err, row) {
        should.not.exist(err)
        model.list(function(err, items) {
          if (err) throw err
          items.should.have.lengthOf(2)
          done()
        })
      })
    }))
  })
  describe.skip('Views', function() {
    it('should be created', function(done) {
      db.get('_design/TestModel', function(err, body) {
        should.not.exist(err)
        body.views.should.have.property('by-key')
        body.views['by-key'].map.should.equal(view.toString())
        done()
      })
    })
    it('should work', domainify(function(done) {
      model.list('by-key', 2, function(err, items) {
        should.not.exist(err)
        items.should.have.lengthOf(1)
        done()
      })
    }))
    it('should work with function type views', domainify(function(done) {
      model.list('by-fn', 42, function(err, res) {
        res.should.have.lengthOf(2)
        res[0].should.eql(42)
        res[1].should.eql(d.req)
        done()
      })
    }))
  })
})
