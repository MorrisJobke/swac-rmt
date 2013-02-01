# [SWAC](https://github.com/rkusa/swac)'s ![](https://dl.dropbox.com/u/6699613/swac-logo.png) PostgreSQL RM/T Adapter (Prototype)

## Usage

```js
this.use('rmt', { db: 'name' }, function() {
  // definition
})
```

### Options

* **db** - the database name the model instances should be saved in

## Definition API

The definitions context provides the following methods:

### .composes(sub)

**Arguments:**

* **sub** - the associative entity

**Example:**

```js
var Engine = swac.Model.define('Engine', function() {
  this.use('rmt', { db: 'postgre' })
  this.property('power')
})

var Vehicle = swac.Model.define('Vehicle', function() {
  this.use('rmt', { db: 'postgre' }, function() {
    this.composes(Engine)
  })
  this.property('manufacturer')
})
```

### .extends(label, sup)

**Arguments:**

* **label** - the relation's label
* **sup** - the super-type

**Example:**

```js
var Car = swac.Model.define('Car', function() {
  this.use('rmt', { db: 'postgre' }, function() {
    this.extends('is-a', Vehilce)
  })
  this.property('color')
})
```