# coding: utf-8

"""
Connection
    ConnectionStandalon
        (class) ConnectionStandalonFactory
        (class) ConnectionStandalonProtocol
    ConnectionReplset
        (class) ConnectionReplsetFactory
        (class) ConnectionReplsetProtocol

        (property) primary
        (property) secondaries

connect (uri='mongodb://host[:port, 27017]?options', )
    . connect to 'host:port'
        . check is replset or single
            . replset
                - reconnect to server using `ConnectionReplset`
            . single
                - reconnect to server using `ConnectionStandalon`

        . monitor all connections

senario

. if one node disconnected in any reason
    . remove
"""

import logging
import copy
import random

from pymongo.uri_parser import parse_uri
from pymongo import errors
from pymongo.read_preferences import ReadPreference

from twisted.internet import reactor, defer
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.python import log, failure

from .factory import (
        AutoDetectConnectionFactory,
        SingleConnectionFactory,
        ReplicaSetConnectionFactory,
    )
from .protocol import Query
from .database import Database

STATE_PRIMARY = 1
STATE_SECONDARY = 2


class BaseConnection (object, ) :
    factory = None
    uri = None
    uri_nodelist = None

    class NoMoreNodeToConnect (Exception, ) : pass

    @classmethod
    def send_is_master (cls, proto, ) :
        _query = Query(collection='admin.$cmd', query={'ismaster': 1, }, )
        _d = proto.send_QUERY(_query, )

        return _d

    @classmethod
    def send_replset_get_status (cls, proto, ) :
        _query = Query(collection='admin.$cmd', query={'replSetGetStatus': 1, }, )
        return proto.send_QUERY(_query, )

    def __init__ (self, uri, ) :
        if type(uri) in (str, unicode, ) :
            uri = parse_uri('mongodb://%s' % uri, )

        self.uri = uri
        self.uri_nodelist = self.uri.get('nodelist')[:]

    def get_factory_instance (self, uri, ) :
        return self.factory(uri, )

    def connect (self, ) :
        log.msg('trying to connect to `%s`.' % self.uri.get('nodelist'), )

        return self._connect().addErrback(self._eb, )

    def _connect (self, ) :
        return self.do_connect().addCallbacks(self._cb_connected, self._eb_connected, )

    def do_connect (self, nodelist=None, ) :
        if nodelist is None :
            nodelist = self.uri_nodelist

        try :
            _host, _port = nodelist.pop(0, )
        except IndexError :
            raise self.NoMoreNodeToConnect

        _uri = copy.copy(self.uri, )
        _uri['nodelist'] = [(_host, _port, ), ]
        _factory = self.get_factory_instance(_uri, )
        return TCP4ClientEndpoint(
                reactor, _host, int(_port),
                ).connect(_factory, ).addCallback(
                        lambda proto : proto.connectionReady(),
                    )

    def _eb_connected (self, f, ) :
        try :
            return self._connect()
        except self.NoMoreNodeToConnect :
            raise errors.ConnectionFailure

    def _eb (self, f, proto=None, ) :
        log.msg(f.printDetailedTraceback(), logLevel=logging.ERROR, )
        if proto and proto.transport :
            proto.transport.loseConnection()

        f.raiseException()

    def _cb_connected (self, proto, ) :
        raise NotImplemented


class AutoDetectConnection (BaseConnection, ) :
    factory = AutoDetectConnectionFactory

    def _cb_connected (self, proto, ) :
        _d = BaseConnection.send_is_master(proto, )
        _d.addCallback(self._cb_verify_ismaster, proto, )
        _d.addErrback(self._eb, proto, )

        return _d

    def _cb_verify_ismaster (self, r, proto, ) :
        if len(r.documents) != 1 :
            raise errors.OperationFailure('Invalid document length.')

        _config = r.documents[0].decode()
        log.msg('[debug,%s] read data, \'%s\'' % (proto.addr, _config, ), )

        if 'hosts' in _config and 'setName' in _config : # replicaset
            log.msg('[debug,%s] found replicaset for `%s`' % (
                    proto.addr, _config.get('setName'), ), )
            _connection = ReplicaSetConnection(
                    parse_uri('mongodb://%s' % _config.get('me'), ),
                )
        else :
            log.msg('[debug,%s] found Single mode.' % (proto.addr, ), )
            _connection = SingleConnection(self.uri.copy(), )

        log.msg('[debug,%s] disconnecting for further process.' % proto.addr, )
        proto.transport.loseConnection()

        # reconnect
        return _connection.connect()


class RealConnection (BaseConnection, ) :
    connections = dict()

    def __init__ (self, uri, ) :
        BaseConnection.__init__(self, uri, )
        self.connections = dict()

    def disconnect (self, ) :
        for i in self.connections.keys() :
            self.remove_connection(i, )

        return

    def _cb_connected (self, proto, ) :
        return self

    def __getitem__ (self, name, ) :
        return Database(self, name, )

    def getprotocol (self, _type='read', ) :
        raise NotImplemented

    def add_connection (self, proto, config=None, ) :
        if config is not None :
            proto.config = config

        self.connections[proto.addr] = proto
        return

    def remove_connection (self, name, ) :
        if name not in self.connections :
            return False

        _proto = self.connections.get(name, )
        if _proto.transport :
            _proto.transport.loseConnection()

        del self.connections[name]
        return True


class SingleConnection (RealConnection, ) :
    factory = SingleConnectionFactory

    def _cb_connected (self, proto, ) :
        self.add_connection(proto, config=dict(), )
        return RealConnection._cb_connected(self, proto, )

    def getprotocol (self, _type='read', ) :
        if not self.connections:
            raise errors.OperationFailure('failed to get protocol.', )

        return self.connections.values()[0]


class ReplicaSetConnection (RealConnection, ) :
    READ_PREFERENCES_FOR_READ = (
            ReadPreference.SECONDARY,
            ReadPreference.SECONDARY_ONLY,
            ReadPreference.SECONDARY_PREFERRED,
        )

    factory = ReplicaSetConnectionFactory
    hosts = list()

    connections = dict()

    def get_factory_instance (self, uri, ) :
        return self.factory(self, uri, )

    def connect_new (self, config, ) :
        _uri = parse_uri('mongodb://%s' % config.get('name'), )
        return self.do_connect([_uri.get('nodelist')[0], ], ).addCallback(
                lambda proto : self.add_connection(proto, config=config, ),
            )

    def _cb_connected (self, proto, ) :
        _d = BaseConnection.send_is_master(proto, )
        _d.addCallback(self._cb_connected_member_status, proto, )
        _d.addErrback(self._eb, proto, )

        return _d

    def _cb_connected_member_status (self, r, proto, ) :
        if len(r.documents) != 1 :
            raise errors.OperationFailure('Invalid document length.')

        _config = r.documents[0].decode()
        self.hosts = _config.get('hosts')
        if not self.hosts :
            raise errors.ConnectionFailure

        _d = BaseConnection.send_replset_get_status(proto, )
        _d.addCallback(self._cb_get_config, proto, )
        _d.addErrback(self._eb, proto, )
        _d.addBoth(self._connection_done, )

        return _d

    def _cb_get_config (self, r, proto, ) :
        if len(r.documents) != 1 :
            raise errors.OperationFailure('Invalid document length.')

        if proto.addr not in self.hosts :
            proto.transport.loseConnection()

        _config = r.documents[0].decode()

        log.msg('[debug,%s] read data, \'%s\'' % (proto.addr, _config, ), )
        if _config.get('ok') != 1.0 :
            log.msg('invalid result, \'%s\'' % _config, logLevel=logging.ERROR, )
            raise errors.OperationFailure('invalid result, \'%s\'' % _config, )

        _dl = list()
        for i in _config.get('members') :
            if i.get('name') not in self.hosts :
                continue

            if i.get('state') not in (1, 2, ) :
                continue

            if i.get('self') :
                self.add_connection(proto, config=i, )
                continue

            _dl.append(self.connect_new(i, ), )

        if _dl :
            return defer.DeferredList(_dl, ).addCallback(self._connect_nodes, )

        return None

    def _connect_nodes (self, r, ) :
        return

    def _connection_done (self, r, ) :
        # start monitor
        reactor.callLater(0.01, ReplicaSetConnectionMonitor(self, ).start, )

        return self

    def _filter_protocol (self, state=None, ) :
        _r = filter(lambda proto : proto.config.get('state') == state, self.connections.values(), )
        if len(_r) < 1 :
            raise errors.OperationFailure('connections not found for %s' % state, )

        return _r

    def _get_protocol (self, state=None, ) :
        if state is None :
            state = STATE_PRIMARY

        _r = self._filter_protocol(state, )
        if state in (STATE_PRIMARY, ) :
            return _r[0]

        if len(_r) < 2 :
            return _r[0]

        return _r[random.choice(range(len(_r)))]

    def getprotocol (self, _type='read', ) :
        if not self.connections :
            raise errors.OperationFailure('connections not found.', )

        if _type != 'read' :
            _proto = self._get_protocol(STATE_PRIMARY, )
            log.msg('[debug] get primary for not read, %s.' % _proto, )
            return _proto

        _rf = self.uri.get('options', dict(), ).get('read_preferences', ReadPreference.SECONDARY_PREFERRED, )
        if _rf not in self.READ_PREFERENCES_FOR_READ :
            _proto = self._get_protocol(STATE_PRIMARY, )
            log.msg('[debug] get primary, %s (%s).' % (_proto, _rf, ), )
            return _proto

        if _rf in (ReadPreference.SECONDARY, ReadPreference.SECONDARY_ONLY, ) :
            _proto = self._get_protocol(STATE_SECONDARY, )
            log.msg('[debug] get secondary, %s (%s).' % (_proto, _rf, ), )
            return _proto

        if _rf in (ReadPreference.SECONDARY_PREFERRED, ReadPreference.NEAREST, ) :
            try :
                _proto = self._get_protocol(STATE_SECONDARY, )
                log.msg('[debug] get secondary, %s (%s).' % (_proto, _rf, ), )
                return _proto
            except errors.OperationFailure :
                _proto = self._get_protocol(STATE_PRIMARY, )
                log.msg('[debug] get secondary, but no secondary, %s (%s).' % (_proto, _rf, ), )
                return _proto

        _proto = self._get_protocol(STATE_SECONDARY, )
        log.msg('[debug] get secondary, %s (%s).' % (_proto, _rf, ), )

        return _proto


class ReplicaSetConnectionMonitor (object, ) :
    interval = 0.4
    #interval = 5

    hosts = list()

    def __init__ (self, connection, ) :
        self._connection = connection

    def start (self, ) :
        log.msg('[debug] start monitor.',)
        return self._monitor()

    def _monitor (self, ) :
        return defer.maybeDeferred(self._select_connection, ).addBoth(self._cb_start, )

    def _cb_start (self, r, ) :
        if isinstance(r, failure.Failure, ) :
            log.msg(r.printDetailedTraceback(), logLevel=logging.ERROR, )

        reactor.callLater(self.interval, self._monitor, )
        return

    def _select_connection (self, ) :
        if not self._connection.connections :
            raise errors.OperationFailure('no connection found.', )

        try :
            _primary = self._connection._get_protocol(STATE_PRIMARY, )
        except errors.OperationFailure :
            _primary = None

        if _primary :
            return self.configure(_primary, )

        for i in self._connection.connections.values() :
            return self.configure(i, )

    def configure (self, proto, ) :
        _d = BaseConnection.send_is_master(proto, )
        _d.addCallback(self._cb_get_member_status, proto, )

        return _d

    def _cb_get_member_status (self, r, proto, ) :
        if len(r.documents) != 1 :
            raise errors.OperationFailure('Invalid document length.')

        _config = r.documents[0].decode()
        self.hosts = _config.get('hosts')
        if not self.hosts :
            raise errors.ConnectionFailure

        _d = BaseConnection.send_replset_get_status(proto, )
        _d.addCallback(self._filter_member, proto, )

        return _d

    def _filter_member (self, r, proto, ) :
        if len(r.documents) != 1 :
            raise errors.OperationFailure('Invalid document length.')

        _config = r.documents[0].decode()
        if _config.get('ok') != 1.0 :
            log.msg('invalid result, \'%s\'' % _config, logLevel=logging.ERROR, )
            raise errors.OperationFailure('invalid result, \'%s\'' % _config, )

        # filter new node
        _dl = list()
        for i in _config.get('members') :
            _new_node = self._check_node(i, )
            if _new_node :
                log.msg('[debug] found new node, `%s`.' % i.get('name', ), )
                _dl.append(self._connection.connect_new(_new_node, ), )

        def _eb (r, ) :
            for _b, _r in r :
                if isinstance(_r, failure.Failure, ) :
                    log.msg(_r.printDetailedTraceback(), logLevel=logging.ERROR, )

            return

        if _dl :
            return defer.DeferredList(_dl, ).addBoth(_eb, )

        return

    def _check_node (self, config, ) :
        if config.get('name') not in self.hosts :
            if config.get('name') in self._connection.connections : # disconnect it
                self._connection.remove_connection(config.get('name'))

            return None

        if config.get('name') in self._connection.connections :
            if config.get('state') in (1, 2, ) :
                self._connection.connections[config.get('name')].config = config
            else :
                log.msg('[debug] > but node, `%s` is not proper state, `%s`, so disconnecting it.' % (
                        config.get('name'), config.get('state'),
                    ), )
                self._connection.remove_connection(config.get('name'))

            return None

        # if new node
        return config


class _ConnectionPool (object, ) :
    _index = 0
    _pool = None
    _pool_size = None
    _cls = AutoDetectConnection
    uri = None

    def __init__ (self, uri=None, pool_size=1, cls=None, ) :
        assert isinstance(pool_size, int)
        assert pool_size >= 1

        uri = uri if uri else 'mongodb://127.0.0.1:27017'
        if type(uri) in (str, unicode, ) :
            if not uri.startswith('mongodb://') :
                uri = 'mongodb://%s' % uri

            uri = parse_uri(uri, )

        self.uri = uri
        self._cls = cls if cls else AutoDetectConnection
        self._pool_size = pool_size
        self._pool = list()

    def connect (self, ) :
        def _cb_connection_done (connection, ) :
            self._pool.append(connection, )
            return

        def _cb_connections_done (r, ) :
            log.msg('filled the %d-sized pool.' % self._pool_size, )
            return self

        _dl = list()
        for i in range(self._pool_size) :
            _dl.append(self._cls(self.uri, ).connect().addCallback(
                    _cb_connection_done,
                ), )

        return defer.DeferredList(_dl, ).addCallback(_cb_connections_done, )

    def __getitem__ (self, name, ) :
        return Database(self, name)

    def __getattr__ (self, name, ) :
        return self[name]

    def disconnect (self, ) :
        for connection in self._pool:
            connection.disconnect()
            del self._pool[self._pool.index(connection, )]

        return

    def getprotocol (self, _type='read', ) :
        _retry = 0
        _c = self._get_connection()
        while not _c.connections :
            if _retry > self._pool_size :
                break

            _c = self._get_connection()
            _retry += 1

        _p = _c.getprotocol(_type)

        return _p

    def _get_connection (self, ) :
        if self._index > self._pool_size :
            self._index = 0

        _r = self._pool
        _c = _r[self._index % len(_r)]
        log.msg('choose the connection from pool, `%s`.' % _c, )

        self._index += 1

        return _c


def MongoConnection (host, port, pool_size=1, cls=None, ) :
    return _ConnectionPool(
            'mongodb://%s:%d' % (host, port, ),
            pool_size=pool_size,
            cls=cls,
        ).connect()


def MongoConnectionPool (host, port, pool_size=5, cls=None, ) :
    return MongoConnection(host, port, pool_size=pool_size, cls=cls, )


Connection = MongoConnection


ConnectionPool = MongoConnectionPool


