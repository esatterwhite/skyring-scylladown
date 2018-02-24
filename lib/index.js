'use strict'

/**
 *
 * @module @skyring/scylladown
 * @author Eric Satterwhite
 * @requires util
 * @requires debug
 * @requires cassandra-driver
 * @requires abstract-leveldown
 * @requires @skyring/scylladown/lib/iterator
 **/

const {inherits, format} = require('util')
const {Client, types} = require('cassandra-driver')
const {AbstractLevelDOWN} = require('abstract-leveldown')
const ScyllaIterator = require('./iterator')
const debug = require('debug')('skyring:scylladown')

const ERR_NOT_FOUND = 'ENOENT'
const kQuery = Symbol('queries')
const q_opts = { prepare: true }
const CREATE_KEYSPACE = `

CREATE KEYSPACE
IF NOT EXISTS %s
WITH REPLICATION = {
  'class': 'SimpleStrategy'
, 'replication_factor': %d
}
`
const CREATE_TABLE  = `
CREATE TABLE IF NOT EXISTS %s.%s (
  id text PRIMARY KEY
, value TEXT
)
`

module.exports = ScyllaDown

/**
 * ScyllaDB Leveldown backend for levelup
 * @class ScyllaDown
 * @extends AbstractLevelDOWN
 * @alias module:@skyring/scylladown
 * @params {String} location The name of a the database table
 * the db instance is responsible for
 **/
function ScyllaDown(location) {

  if (!(this instanceof ScyllaDown)) return new ScyllaDown(location)

  AbstractLevelDOWN.call(this, location)
  this.keyspace = null
  this.client = null
  this.table = location
  this[kQuery] = {
    insert: null
  , update: null
  , get: null
  , del: null
  }
}

inherits(ScyllaDown, AbstractLevelDOWN)

ScyllaDown.prototype._open = function _open(opts, cb) {
  const {
    contactPoints = ['127.0.0.1']
  , keyspace = 'skyring'
  , replicas = 1
  } = opts

  debug('contact points: ', contactPoints)
  debug('keyspace', keyspace)
  debug('replicas', replicas)

  this.client = new Client({
    contactPoints: contactPoints
  })

  this.keyspace = keyspace

  this[kQuery] = {
    get: `
      SELECT value FROM ${this.table}
      WHERE id = ?
    `
  , put: `
      UPDATE ${this.table}
      SET value = ?
      WHERE id = ?
    `
  , del: `
      DELETE FROM ${this.table}
      WHERE id = ?
    `
  , insert: `
      INSERT INTO ${this.table} (
        id, value
      ) VALUES (?, ?)
    `
  }

  this.client.connect((err) => {
    if (err) return cb(err)
    this._keyspace(replicas, (err) => {
      if (err) return cb(err)
      this.client.keyspace = keyspace
      return this._table((err) => {
        if (err) return cb(err)
        return cb(null, this)
      })
    })
  })
}

ScyllaDown.prototype._get = function _get(key, options, cb) {
  const query = this[kQuery].get
  this.client.execute(query, [key], q_opts, (err, res) => {
    if (err) return cb(err)
    if (!res.rows.length) {
      const error = new Error('Key Not Found')
      error.code = ERR_NOT_FOUND
      return cb(error)
    }
    return cb(null, res.rows[0].value)
  })
}

ScyllaDown.prototype._put = function _put(key, value, options, cb) {
  if (options.insert) return this._insert(key, value, options, cb)

  const query = this[kQuery].put
  this.client.execute(query, [value, key], q_opts, cb)
}

ScyllaDown.prototype._insert = function _insert(key, value, options, cb) {
  const query = this[kQuery].insert
  const values = [
    key
  , value
  ]
  debug('insert', query, values)
  this.client.execute(query, values, q_opts, cb)
}

ScyllaDown.prototype._del = function _del(key, options, cb) {
  const query = this[kQuery].del
  this.client.execute(del, [key], q_opts, cb)
}

ScyllaDown.prototype._batch = function _batch(arr, options, cb) {
  const ops = arr.map((op) => {
    switch(op.type) {
      case 'del':
        return {
          query: this[kQuery].del
        , params: [op.key]
        }
      case 'put':
        return {
          query: this[kQuery].del
        , params: [op.value, op.key]
        }
    }
  })

  this.client.batch(ops, cb)
}

ScyllaDown.prototype._iterator = function _iterator(options) {
  return new ScyllaIterator(this, options)
}

ScyllaDown.prototype._keyspace = function _keyspace(replicas = 1, cb) {
  const query = format(CREATE_KEYSPACE, this.keyspace, replicas)
  debug('creating keyspace', this.keyspace, query)
  this.client.execute(query, cb)
}

ScyllaDown.prototype._table = function _table(cb) {
  const query = format(CREATE_TABLE, this.keyspace, this.table)
  debug('creating data table', this.table)
  this.client.execute(query, cb)
}
