# [Arkansas](https://github.com/rkusa/Arkansas)' ![](https://dl.dropbox.com/u/6699613/arkansas-logo.png) CouchDB Adapter

## Status [![Build Status](https://secure.travis-ci.org/rkusa/arkansas-couchdb.png)](http://travis-ci.org/rkusa/arkansas-couchdb)

Don't use yet.

## Usage

```js
this.use('couchdb', { db: 'name' }, function() {
  // definition
})
```

### Options

* **db** - the database name the model instances should be saved in

## Definition API

The definitions context provides the following methods:

### .view(name, obj)

**Arguments:**

* **name** - the view's name
* **obj** - an object containing at least a `map` property and optionally a `reduce` property

**Example:**

```js
this.use('couchdb', function() {
  this.view('by-user', {
    map: function(doc) {
      if(doc.$type === 'Todo') emit(doc.user, null)
    }
  })
})
```

### .view(name, fn)

**Arguments:**

* **name** - the view's name
* **fn** - a function which will be executed once the view got called

**Example:**

```js
this.use('couchdb', function() {
  this.view('by-user', function(key, req, callback) {
    if (!key || !req.user) return callback(null, [])
    var user = req.user
    this.view('Todo', 'by-group', {
      keys: user.groups,
      include_docs: true
    }, callback(err, body))
  })
})
```