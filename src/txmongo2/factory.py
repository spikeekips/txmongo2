# coding: utf-8

from twisted.internet import reactor
from twisted.internet.protocol import ClientFactory
from twisted.python import log

from .protocol import MongoProtocol


class BaseConnectionFactory (ClientFactory, ) :
    protocol = MongoProtocol

    def __init__ (self, uri, ) :
        self.uri = uri

    def buildProtocol (self, addr, ) :
        return ClientFactory.buildProtocol(self, addr, )

    def clientConnectionMade (self, connector, ) :
        log.msg('[debug,%s] connection made.' % connector.addr, )
        return

    def clientConnectionLost (self, connector, reason, ) :
        log.msg('[debug,%s] connection lost.' % connector.addr, )
        return


class AutoDetectConnectionFactory (BaseConnectionFactory, ) :
    pass


class SingleConnectionFactory (BaseConnectionFactory, ) :
    pass


class ReplicaSetConnectionFactory (BaseConnectionFactory, ) :
    def __init__ (self, connection, uri, ) :
        self._connection = connection
        BaseConnectionFactory.__init__(self, uri, )

    def clientConnectionMade (self, connector, ) :
        BaseConnectionFactory.clientConnectionMade(self, connector, )

    def clientConnectionLost (self, connector, reason, ) :
        BaseConnectionFactory.clientConnectionLost(self, connector, reason, )

        if self._connection.remove_connection(connector.addr, ) :
            log.msg('[debug,%s] removed from connection list.' % connector.addr, )

        return


