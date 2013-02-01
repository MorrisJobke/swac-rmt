var swac = require('swac')
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

describe('SWAC PostgreSql RM/T Adapter', function() {
  before(function(done) {
    pg.connect(process.env.POSTGRES  || 'tcp://postgres@127.0.0.1/postgres', function(err, c) {
      if (err) throw err
      client = c
      done()
    })
  })
  after(function(done) {
    async.parallel(
      ['e_auto', 'e_fahrzeug', 'e_motor', 'e_rad', 'p_auto', 'p_fahrzeug', 'p_motor', 'p_rad',
      'rmt_ag', 'rmt_pg', 'rmt_ugi'].map(function(table) {
        return client.query.bind(client, 'DROP TABLE ' + table)
      }),
      done
    )
  })
  var Fahrzeug, Auto, Rad, Motor
  describe('Model Definition', function() {
    before(function(done) {
      async.series([
        function(done) {
          Rad = swac.Model.define('Rad', function() {
            this.use(require('../'), { db: 'postgre' })
            this.property('umfang')
          }, done)
        },
        function(done) {
          Motor = swac.Model.define('Motor', function() {
            this.use(require('../'), { db: 'postgre' })
            this.property('leistung')
          }, done)
        },
        function(done) {
          Fahrzeug = swac.Model.define('Fahrzeug', function() {
            this.use(require('../'), { db: 'postgre' }, function() {
              this.composes(Rad, Motor)
            })
            this.property('hersteller')
          }, done)
        },
        function(done) {
          Auto = swac.Model.define('Auto', function() {
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
          columns.length.should.equal(18)
          for(var i in columns) {
            var table = columns[i]
            if(table.table_name.match(/^e_.*/))
              table.column_name.should.equal('id')
            else
              table.column_name.should.match(/^(id|surrogat|leistung|umfang|hersteller|farbe|rad|motor)$/)
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
  describe('CRUD', function() {
    var cur, rad, motor
    it('POST should work', domainify(function(done) {
      Auto.post({ farbe: 'schwarz', rad: (rad = new Rad({umfang: '13 cm'})), motor: (motor = new Motor({leistung: '130 PS'})), hersteller: 'MM' }, function(err, auto) {
        cur = auto
        should.not.exist(err)
        client.query(
          'SELECT p_auto.farbe, p_rad.umfang, p_motor.leistung, p_fahrzeug.hersteller ' +
          'FROM p_auto JOIN p_fahrzeug ON p_auto.surrogat = p_fahrzeug.surrogat ' +
          'JOIN p_rad ON p_rad.surrogat = p_fahrzeug.rad ' +
          'JOIN p_motor ON p_motor.surrogat = p_fahrzeug.motor ' +
          'WHERE p_auto.id = $1;',
          [auto.id] ,
          function(err, result) {
            if (err) throw err
            result.rows.length.should.equal(1)
            var a = result.rows[0]
            auto.should.have.property('farbe', 'schwarz')
            auto.should.have.property('hersteller', 'MM')
            auto.motor.should.have.property('leistung', '130 PS')
            auto.rad.should.have.property('umfang', '13 cm')
            a.should.have.property('farbe', 'schwarz')
            a.should.have.property('hersteller', 'MM')
            a.should.have.property('leistung', '130 PS')
            a.should.have.property('umfang', '13 cm')
            done()
        })
      })
    }))
    it.skip('POST should create surrogates in e-relation', domainify(function(done) {
      client.query(
        'SELECT e_auto.id AS surrogat ' +
        'FROM e_auto JOIN p_auto ON e_auto.id = p_auto.surrogat ' +
        'WHERE p_auto.id = $1;',
        [cur.id] ,
        function(err, result) {
          should.not.exist(err)
          result.rows.length.should.equal(1)
          result.rows[0].should.have.property('surrogat', 'abc')
          done()
      })
    }))
    it('PUT should work', domainify(function(done) {
      cur.farbe = 'blue'
      Auto.put(cur.id, cur, function(err, row) {
        should.not.exist(err)
        client.query('SELECT * FROM p_auto WHERE id = $1', [cur.id], function(err, result) {
          if (err) throw err
          result.should.have.property('rowCount', 1)
          result.rows[0].should.have.property('farbe', 'blue')
          done()
        })
      })
    }))
    it('GET should work', domainify(function(done) {
      Auto.get(cur.id, function(err, auto) {
        should.not.exist(err)
        auto.should.have.property('id', cur.id)
        auto.should.have.property('farbe', cur.farbe)
        auto.should.have.property('hersteller', cur.hersteller)
        auto.motor.should.have.property('leistung', cur.motor.leistung)
        auto.rad.should.have.property('umfang', cur.rad.umfang)
        auto.should.have.property('_surrogat', cur._surrogat)
        auto.rad.should.have.property('_surrogat', rad._surrogat)
        auto.motor.should.have.property('_surrogat', motor._surrogat)
        done()
      })
    }))
    it('LIST should work', domainify(function(done) {
      Auto.post({ farbe: 'rot' }, function(err, row) {
        should.not.exist(err)
        Auto.list(function(err, autos) {
          should.not.exist(err)
          autos.should.have.lengthOf(2)
          done()
        })
      })
    }))
  })
})
