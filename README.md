# skyring-scylladown

> [`ScyllaDB`] backend store for [`levelup`] leveraging [`abstract-leveldown`]

[![level badge][level-badge]](https://github.com/level/awesome)
[![npm](https://img.shields.io/npm/v/@skyring-scylladown.svg?style=flat-square)](https://github.com/esatterwhite/skyring-scylladown)
[![npm](https://img.shields.io/npm/l/@skyring/scylladown.svg?style=flat-square)](https://github.com/esatterwhite/skyring-scylladown/blob/master/LICENSE)

## Example

```javascript
const levelup = require('levelup')
const scylladown = require('@skyring/scylladown')

const opts = {
  contactPoints: ['192.0.0.1:9042', '192.0.0.2:9042', '192.0.0.3:9042']
, keyspace: 'customkeyspace'
}

const db = levelup(scylladown('table_name'), opts)
```

[`ScyllaDB`]: https://github.com/Level/abstract-leveldown
[`abstract-leveldown`]: https://github.com/Level/levelup
[level-badge]: http://leveldb.org/img/badge.svg
[`levelup`]: https://github.com/Level/levelup
