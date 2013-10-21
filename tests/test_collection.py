# -*- coding: utf-8 -*-

# Copyright 2012 Renzo S.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test the collection module.
Based on pymongo driver's test_collection.py
"""

from bson.son import SON
from pymongo import errors 

from twisted.internet import defer
from twisted.trial import unittest

import txmongo2

from txmongo2 import filter
from txmongo2.collection import Collection

mongo_host="localhost"
mongo_port=27017


class TestCollection(unittest.TestCase):
    
    timeout = 5

    @defer.inlineCallbacks
    def setUp(self):
        self.conn = yield txmongo2.MongoConnection(mongo_host, mongo_port)
        self.db = self.conn.mydb
        self.coll = self.db.mycol

    @defer.inlineCallbacks
    def tearDown(self):
        ret = yield self.coll.drop()
        ret = yield self.conn.disconnect()

    @defer.inlineCallbacks
    def test_collection(self):
        self.assertRaises(TypeError, Collection, self.db, 5)

        def make_col(base, name):
            return base[name]

        self.assertRaises(errors.InvalidName, make_col, self.db, "")
        self.assertRaises(errors.InvalidName, make_col, self.db, "te$t")
        self.assertRaises(errors.InvalidName, make_col, self.db, ".test")
        self.assertRaises(errors.InvalidName, make_col, self.db, "test.")
        self.assertRaises(errors.InvalidName, make_col, self.db, "tes..t")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "te$t")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, ".test")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "test.")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "tes..t")
        self.assertRaises(errors.InvalidName, make_col, self.db.test, "tes\x00t")

        self.assert_(isinstance(self.db.test, Collection))
        self.assertEqual(self.db.test, Collection(self.db, "test"))
        self.assertEqual(self.db.test.mike, self.db["test.mike"])
        self.assertEqual(self.db.test["mike"], self.db["test.mike"])

        yield self.db.drop_collection('test')
        collection_names = yield self.db.collection_names()
        self.assertFalse('test' in collection_names)

    @defer.inlineCallbacks
    def test_create_index(self):
        db = self.db
        coll = self.coll

        self.assertRaises(TypeError, coll.create_index, 5)
        self.assertRaises(TypeError, coll.create_index, {"hello": 1})

        yield coll.insert({'c': 1}) # make sure collection exists.

        yield coll.drop_indexes()
        count = yield db.system.indexes.count({"ns": u"mydb.mycol"})
        self.assertEqual(count, 1)

        result1 = yield coll.create_index(filter.sort(filter.ASCENDING("hello")))
        result2 = yield coll.create_index(filter.sort(filter.ASCENDING("hello") + \
                                          filter.DESCENDING("world")))

        count = yield db.system.indexes.count({"ns": u"mydb.mycol"}) 
        self.assertEqual(count, 3)

        yield coll.drop_indexes()
        ix = yield coll.create_index(filter.sort(filter.ASCENDING("hello") + \
                                   filter.DESCENDING("world")), name="hello_world")
        self.assertEquals(ix, "hello_world")

        yield coll.drop_indexes()
        count = yield db.system.indexes.count({"ns": u"mydb.mycol"}) 
        self.assertEqual(count, 1)
        
        yield coll.create_index(filter.sort(filter.ASCENDING("hello")))
        indices = yield db.system.indexes.find({"ns": u"mydb.mycol"}) 
        self.assert_(u"hello_1" in [a["name"] for a in indices])

        yield coll.drop_indexes()
        count = yield db.system.indexes.count({"ns": u"mydb.mycol"}) 
        self.assertEqual(count, 1)

        ix = yield coll.create_index(filter.sort(filter.ASCENDING("hello") + \
                                   filter.DESCENDING("world")))
        self.assertEquals(ix, "hello_1_world_-1")
    
    @defer.inlineCallbacks
    def test_create_index_nodup(self):
        coll = self.coll

        ret = yield coll.drop()
        ret = yield coll.insert({'b': 1})
        ret = yield coll.insert({'b': 1})

        ix = coll.create_index(filter.sort(filter.ASCENDING("b")), unique=True)
        yield self.assertFailure(ix, errors.DuplicateKeyError)


    @defer.inlineCallbacks
    def test_ensure_index(self):
        db = self.db
        coll = self.coll
        
        yield coll.ensure_index(filter.sort(filter.ASCENDING("hello")))
        indices = yield db.system.indexes.find({"ns": u"mydb.mycol"}) 
        self.assert_(u"hello_1" in [a["name"] for a in indices])

        yield coll.drop_indexes()

    @defer.inlineCallbacks
    def test_index_info(self):
        db = self.db

        yield db.test.drop_indexes()
        yield db.test.remove({})

        db.test.save({})  # create collection
        ix_info = yield db.test.index_information()
        self.assertEqual(len(ix_info), 1)

        self.assert_("_id_" in ix_info)

        yield db.test.create_index(filter.sort(filter.ASCENDING("hello")))
        ix_info = yield db.test.index_information()
        self.assertEqual(len(ix_info), 2)
        
        self.assertEqual(ix_info["hello_1"], [("hello", 1)])

        yield db.test.create_index(filter.sort(filter.DESCENDING("hello") + filter.ASCENDING("world")), unique=True)
        ix_info = yield db.test.index_information()

        self.assertEqual(ix_info["hello_1"], [("hello", 1)])
        self.assertEqual(len(ix_info), 3)
        self.assertEqual([("world", 1), ("hello", -1)], ix_info["hello_-1_world_1"])
        # Unique key will not show until index_information is updated with changes introduced in version 1.7
        #self.assertEqual(True, ix_info["hello_-1_world_1"]["unique"])

        yield db.test.drop_indexes()
        yield db.test.remove({})
        

    @defer.inlineCallbacks
    def test_index_geo2d(self):
        db = self.db
        coll = self.coll 
        yield coll.drop_indexes()
        geo_ix = yield coll.create_index(filter.sort(filter.GEO2D("loc")))

        self.assertEqual('loc_2d', geo_ix)

        index_info = yield coll.index_information()
        self.assertEqual([('loc', '2d')], index_info['loc_2d'])

    @defer.inlineCallbacks
    def test_index_haystack(self):
        db = self.db
        coll = self.coll
        yield coll.drop_indexes()

        _id = yield coll.insert({
            "pos": {"long": 34.2, "lat": 33.3},
            "type": "restaurant"
        })
        yield coll.insert({
            "pos": {"long": 34.2, "lat": 37.3}, "type": "restaurant"
        })
        yield coll.insert({
            "pos": {"long": 59.1, "lat": 87.2}, "type": "office"
        })

        yield coll.create_index(filter.sort(filter.GEOHAYSTACK("pos") + filter.ASCENDING("type")), **{'bucket_size': 1})

        # TODO: A db.command method has not been implemented yet.
        # Sending command directly
        command = SON([
            ("geoSearch", "mycol"),
            ("near", [33, 33]),
            ("maxDistance", 6),
            ("search", {"type": "restaurant"}),
            ("limit", 30),
        ])
           
        results = yield db["$cmd"].find_one(command)
        self.assertEqual(2, len(results['results']))
        self.assertEqual({
            "_id": _id,
            "pos": {"long": 34.2, "lat": 33.3},
            "type": "restaurant"
        }, results["results"][0])


