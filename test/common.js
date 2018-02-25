'use strict'

const crypto = require('crypto')
const now = process.hrtime().join()
let db = 0
module.exports = {
  location
, lastLocation
, setup
, tearDown
}

function location() {
  return crypto.createHash('md5').update(now).update(`${++db}`).digest('hex').replace(/[0-9]/g ,'x')
}

function lastLocation() {
  return crypto.createHash('md5').update(now).update(`${db}`).digest('hex').replace(/[0-9]/g, 'x')
}

function setup(t) {
  t.pass('setup complete')
  t.end()
}

function tearDown(t) {
  setup(t)
}

function collectEntries(iterator, callback) {
  const data = []
  const next = (...args) => {
    iterator.next((err, key, value) => {
      if (err) return callback(err)
      if (!args.length) {
        return iterator.end(function (err) {
          callback(err, data)
        })
      }
      data.push({ key: key, value: value })
      setImmediate(next)
    })
  }
  next()
}
