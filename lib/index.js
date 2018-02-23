'use strict'

const {Client, types} = require('cassandra-driver')
const {AbstractLevelDOWN} = require('abstract-leveldown')
const debug = require('debug')('skyring:scylladown')

const UUID = types.Uuid

const kGET = Symbol('get')
const kPUT = Symbol('put')
const kDEL = Symbol('delete')
const kINSERT = Symbol('insert')

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
  id UUID PRIMARY KEY
, created TIMESTAMP
, payload TEXT
)
`
process.on('unhanledRejection', (err) => {
  process.nextTick(() => {
    throw err
  })
})

module.exports = class ScyllaDown extends AbstractLevelDOWN {
  constructor(location) {
    super(location)
    this.keyspace = null
    this.client = null
    this.table = location
    this.queries = {
      insert: null
    , update: null
    , get: null
    , del: null
    }
  }

  _open(opts, cb) {
    debug('open')
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
    this._queries()

    this.client
      .connect()
      .then(() => {
        return this._keyspace(replicas)
      })
      .then(() => {
        this.client.keyspace = keyspace
        return this._table(keyspace)
      })
      .then(() => {
        cb(null, this)
      })
      .catch(cb)
  }

  _get(key, options, cb) {
    const query = this.queries[kGET]
    this.client.execute(query, [UUID.fromString(key)], cb)
  }

  _put(key, value, options, cb) {
    if (options.insert) return this._insert(key, value, options, cb)

    const query = this.queries[kPUT]
    this.client.execute(query, [value, UUID.fromString(key)], cb)
  }

  _insert(key, value, options, cb) {
    const query = this.queries[kINSERT]
    const values = [
      UUID.fromString(key)
    , new Date()
    , JSON.stringify(value)
    ]
    debug('insert', query, values)
    this.client.execute(query, values, cb)
  }

  _del(key, options, cb) {
    const query = this.queries[kDEL]
    this.client.execute(del, [UUID.fromString(key)], cb)
  }

  _keyspace(replicas = 1) {
    const query = util.format(CREATE_KEYSPACE, this.keyspace, replicas)
    debug('creating keyspace', this.keyspace, query)
    return this.client.execute(query)
  }

  _table() {
    const query = util.format(CREATE_TABLE, this.keyspace, this.table)
    debug('creating data table', this.table)
    return this.client.execute(query)
  }

  _queries() {
    this.queries[kGET] = `
      SELECT * FROM ${this.table}
      WHERE id = ?
    `

    this.queries[kPUT] = `
      UPDATE ${this.table}
      SET payload = ?
      WHERE id = ? IF EXISTS
    `

    this.queries[kDEL] = `
      DELETE FROM ${this.table}
      WHERE id = ? IF EXISTS
    `

    this.queries[kINSERT] = `
      INSERT INTO ${this.table} (
        id, created, payload
      ) VALUES (?, ?, ?)
    `
  }
}
