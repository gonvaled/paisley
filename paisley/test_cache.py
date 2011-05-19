# -*- test-case-name: paisley.test_cache -*-

# Copyright (c) 2007-2008
# See LICENSE for details.

"""
Test for couchdb client.
"""

try:
    import json
except:
    import simplejson as json

import sys

import cgi

from twisted.internet import defer

from twisted.trial.unittest import TestCase
from twisted.internet.defer import Deferred
from twisted.internet import reactor
from twisted.web import resource, server

import paisley

from paisley import client, test_util

class MemoryCacheTestCase(test_util.CouchDBTestCase):
    def setUp(self):
        test_util.CouchDBTestCase.setUp(self)

        self.cache = client.MemoryCache()
        self.db = client.CouchDB('localhost', self.wrapper.port,
            cache=self.cache)

        d = defer.Deferred()
        d.addCallback(lambda _: self.db.createDB('test'))
        d.addCallback(lambda _: self.db.saveDoc('test', {
            'key': 'value'
        }))
        d.addCallback(lambda r: setattr(self, 'first', r['id']))
        d.addCallback(lambda _: self.db.saveDoc('test', {
            'lock': 'chain'
        }))
        d.addCallback(lambda r: setattr(self, 'second', r['id']))
        d.callback(None)
        return d

    def tearDown(self):
        test_util.CouchDBTestCase.tearDown(self)

    def testCached(self):
        
        d = defer.Deferred()

        d.addCallback(lambda _: self.db.openDoc('test', self.first))
        def openCb(result):
            self.assertEquals(result['key'], 'value')
        d.addCallback(openCb)
        d.addCallback(lambda _: self.assertEquals(self.cache.lookups, 1))
        d.addCallback(lambda _: self.assertEquals(self.cache.hits, 0))
        d.addCallback(lambda _: self.assertEquals(self.cache.cached, 1))

        d.addCallback(lambda _: self.db.openDoc('test', self.first))
        def openCb(result):
            self.assertEquals(result['key'], 'value')
        d.addCallback(openCb)
        d.addCallback(lambda _: self.assertEquals(self.cache.lookups, 2))
        d.addCallback(lambda _: self.assertEquals(self.cache.hits, 1))
        d.addCallback(lambda _: self.assertEquals(self.cache.cached, 1))

        d.addCallback(lambda _: self.db.openDoc('test', self.second))
        def openCb(result):
            self.assertEquals(result['lock'], 'chain')
        d.addCallback(openCb)
        d.addCallback(lambda _: self.assertEquals(self.cache.lookups, 3))
        d.addCallback(lambda _: self.assertEquals(self.cache.hits, 1))
        d.addCallback(lambda _: self.assertEquals(self.cache.cached, 2))

        d.callback(None)
        return d
