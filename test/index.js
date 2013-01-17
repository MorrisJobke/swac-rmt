var Arkansas = require('arkansas')
  , should   = require('should')
  , pg       = require('pg')
  , client, model

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
  describe('Model Definition', function() {
    before(function(done) {
      model = Arkansas.Model.define('Fahrzeug', function() {
        this.use(require('../'), { db: 'postgre' })
        this.property('ps')
        this.property('color')
      }, done)
    })
    it.skip('should create a P-Relation if not exists', function(done){
      client.query(
        'SELECT COUNT(*) FROM information_schema.tables WHERE table_name = \'p_user\' OR table_name = \'p_user_stammdaten\';',
        function(err, result) {
          if(err) throw err
          result.rows[0].count.should.equal(2) // tables exists
          client.query(
            'SELECT COUNT(*) FROM ' +
              'information_schema.columns ' +
            'WHERE table_name = \'p_user\' AND (' +
              'column_name = \'ai\' AND  data_type = \'integer\' OR ' +
              'column_name = \'af\' AND  data_type = \'real\' OR ' +
              'column_name = \'ad\' AND  data_type = \'real\' OR ' +
              'column_name = \'ab\' AND  data_type = \'boolean\' OR ' +
              'column_name = \'ac\' AND  data_type = \'character varying\'' +
            ');',
            function(err, result) {
              if(err) throw err
              result.rows[0].count.should.equal(5)
              client.query(
                'SELECT COUNT(*) FROM ' +
                  'information_schema.columns ' +
                'WHERE table_name = \'p_user\';',
                function(err, result) {
                  if(err) throw err
                  result.rows[0].count.should.equal(5 + 2) // + 2 == surrogate + id
                  done()
                }
              )
            }
          )
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
