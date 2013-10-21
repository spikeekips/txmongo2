# (Another) MongoDB driver for Python Twisted

This driver is the derived work from [@oubiwann](https://github.com/oubiwann)'s [txmongo](https://github.com/oubiwann/txmongo) driver, and added some missing features like the support `replicaSet`, which is basically available in [pymongo](https://github.com/mongodb/mongo-python-driver).

The basic usage is almost same with the @oubiwann's `txmongo`, but I added new connection class for `replicaSet`, `AutoDetectConnection`, this will detect whether your mongodb node(s) is/are replica set or not and automatically trying to create the connections to the primary and all secondary.

> In the replicaSet connection, zero(0)-priority node will not be connected.

## Usage

### connect to single mongodb instance

```python
from twisted.internet import defer

import txmongo2

@defer.inlineCallbacks
def cb_connected (connection, ) :
    assert isinstance(connection, txmongo2._ConnectionPool, )
    assert isinstance(connection['test_db'], txmongo2.Database, )
    assert isinstance(connection['test_db']['test_collection'], txmongo2.Collection, )

    yield connection['test_db']['test_collection'].insert(dict(b='test value', ), )
    _count = yield connection['test_db']['test_collection'].count()
    print _count

    reactor.stop()

    return

txmongo2.Connection('localhost', 27017, ).addCallback(cb_connected, )

reactor.run()

```


### connect to replicaset mongodb nodes

```python
from twisted.internet import defer

import txmongo2
from txmongo2.database import Database
from txmongo2.collection import Collection

@defer.inlineCallbacks
def cb_connected (connection, ) :
    assert isinstance(connection, txmongo2._ConnectionPool, )
    assert isinstance(connection['test_db'], txmongo2.Database, )
    assert isinstance(connection['test_db']['test_collection'], txmongo2.Collection, )
    assert len(connection._pool) == 1
    assert isinstance(connection._pool[0], txmongo2.ReplicaSetConnection, )

    yield connection['test_db']['test_collection'].insert(dict(b='test value', ), )
    _count = yield connection['test_db']['test_collection'].count()
    print _count

    return

txmongo2.Connection('localhost', 27017, pool_size=1, ).addCallback(cb_connected, )

reactor.run()
```


