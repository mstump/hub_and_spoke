#!/usr/bin/env python

KEYSPACE_DEF = """CREATE KEYSPACE %s WITH REPLICATION={'class' : 'SimpleStrategy', 'replication_factor' : 1};"""

TABLE_DEF_MAIN = """
CREATE TABLE %s.serial_numbers (
  serial_number text,
  item_number text,
  name text,
  sequence int,
  qualifier text,
  site_id int,
  value text,
  PRIMARY KEY (serial_number, item_number, name, sequence)
);
"""

TABLE_DEF_QUEUE = """
CREATE TABLE %s.queue (
  site_id int,
  shard int,
  seq timeuuid,
  message text,
  PRIMARY KEY ((site_id, shard), seq)
);
"""

SHARDS = 4

KEYSPACES_MAIN = ["csco", "cm_1", "cm_2"]

EXAMPLE_MESSAGE_1 = """
{
    "type" : "Message",
    "message_type" : "serial_numbers",
    "attributes" : {
        "serial_number" : "264592425",
        "item_number" : "4031761",
        "name" : "HARDWAREVERSION",
        "sequence" : 1,
        "site_id" : 242,
        "value" : "1.0"
    }
}
"""

EXAMPLE_MESSAGE_2 = """
{
    "type" : "Message",
    "message_type" : "serial_numbers",
    "attributes" : {
        "serial_number" : "264592425",
        "item_number" : "4031761",
        "name" : "LEGEND_LABEL",
        "sequence" : 1,
        "site_id" : 242,
        "value" : "EPC3925"
    }
}
"""

PORT_NUMBER = 8080

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from cassandra import AlreadyExists
from cassandra.cluster import Cluster
from cassandra.io.libevreactor import LibevConnection
from cassandra.query import ValueSequence
import json
import posixpath
import random
import urllib
import urlparse
import uuid

class Message:
    message_type = None
    attributes = {}

    def __init__(self, t, a):
        self.message_type = t
        self.attributes = a

    def __str__(self):
        return repr(self.to_dict())

    def __repr__(self):
        return str(self)

    def to_dict(self):
        return {u"type"         : u"Message",        \
                u"message_type" : self.message_type, \
                u"attributes"   : self.attributes}


class MessageEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Message):
            return obj.to_dict()
        else:
            return obj

def message_decoder(obj):
    if type(obj) == type({}) \
       and obj.has_key('type') \
       and obj['type'] == 'Message':
        assert 'message_type' in obj
        assert 'attributes' in obj
        assert type(obj["message_type"]) == type(u"")
        assert type(obj["attributes"]) == type({})

        for k,v in obj["attributes"].items():
            assert type(k) == type(u"")
            assert (type(v) == type(u"") or type(v) == type(0.0) or type(v) == type(1))

        return Message(obj["message_type"], obj["attributes"])
    else:
        return obj


def cassandra_connect(seed, keyspace=None):
    cluster = Cluster()
    cluster.connection_class = LibevConnection
    return cluster.connect(keyspace)


def initialize_cluster(session):
    for i in KEYSPACES_MAIN:
        try:
            session.execute(KEYSPACE_DEF % i)
        except AlreadyExists:
            pass

        try:
            session.execute(TABLE_DEF_MAIN % i)
        except AlreadyExists:
            pass

        try:
            session.execute(TABLE_DEF_QUEUE % i)
        except AlreadyExists:
            pass


def persist_message(session, message):
    statement = "INSERT INTO %s(%s) %s;" % (message.message_type, ", ".join(message.attributes.keys()), "VALUES %s")
    session.execute(statement, parameters=[ValueSequence(message.attributes.values())])


def enqueue_message(session, shard, queues, message):
    for q in queues:
        seq = uuid.uuid1()
        statement = "INSERT INTO queue(site_id, shard, seq, message) VALUES(%s, %s, %s, %s);"
        session.execute(statement, [q, shard, seq, json.dumps(message, cls=MessageEncoder)])


def dispatch_message(message):
    return [242]


def shard_generator(shards):
    i = 0
    while True:
        if i > shards:
            i = 0
        yield i
        i += 1


def delete_queue_message(session, queue, shard, seq):
    statement = "DELETE FROM queue WHERE site_id=%s AND shard=%s AND seq=%s;"
    return session.execute(statement, [queue, shard, seq])


def get_queue_messages(session, queue, max_shards, limit=1):
    epoch_uuid = uuid.UUID("00000000-0000-1000-ba92-28cfe9215919")
    statement = "SELECT site_id, shard, seq, message FROM queue WHERE site_id=%s AND shard=%s AND seq > %s ORDER BY seq LIMIT 1;"

    markers = {}
    shards = range(0, max_shards)
    random.shuffle(shards)

    for shard in shards:
        markers[shard] = epoch_uuid

    turns = float(limit) / max_shards
    results = []

    while True:

        for shard, marker in markers.items():
            if len(results) >= limit:
                break

            result = session.execute(statement, [queue, shard, marker])
            if len(result) == 0:
                del markers[shard]
            else:
                results.append((shard, str(result[0][2]), json.loads(result[0][3], object_hook=message_decoder)))
                markers[shard] = result[0][2]

        if len(results) >= limit or len(markers) == 0:
            break

    return sorted(results, key=lambda x: x[1])


def process_inbound_message(session, shards, blob):
    msg = json.loads(blob, object_hook=message_decoder)
    persist_message(session, msg)
    enqueue_message(session, next(shards), dispatch_message(msg), msg)


def http_get_queue_messages(session, path, query):
    # expects /queue/queue_name?limit=10
    assert len(path) == 2
    queue_name = int(path[1])
    limit = query.get("limit", 10)
    output = get_queue_messages(session, queue_name, SHARDS, limit)
    return 200, output


class CscoHTTPServer(HTTPServer):

    def __init__(self, session, *args, **kw):
        HTTPServer.__init__(self, *args, **kw)
        self.context = { "cassandra" : session }

class CscoHandler(BaseHTTPRequestHandler):

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def do_GET(self):
        session = self.server.context["cassandra"]

        url = urlparse.urlparse(self.path)
        path = [x.lower() for x in filter(lambda x: x != "", posixpath.normpath(urllib.unquote(url.path)).split('/'))]
        query = dict(urlparse.parse_qsl(url.query))

        code = 500
        output = ''

        if path[0] == "queue":
            code, output = http_get_queue_messages(session, path, query)

        elif path[0] == "message":
            code, output = http_get_messages(session, path, query)

        else:
            code = 404

        self.send_response(code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(output, cls=MessageEncoder))
        self.wfile.close()


def setup_broker(cassandra_session):
    try:
        server = CscoHTTPServer(cassandra_session, ('localhost', 8080), CscoHandler)
        print('Started http server')
        server.serve_forever()

    except KeyboardInterrupt:
        print('^C received, shutting down server')
        server.socket.close()



if __name__ == "__main__":
    shards = shard_generator(SHARDS)
    initialize_cluster(cassandra_connect("localhost"))
    csco_session = cassandra_connect("localhost", "csco")

    process_inbound_message(csco_session, shards, EXAMPLE_MESSAGE_1)
    process_inbound_message(csco_session, shards, EXAMPLE_MESSAGE_2)

    setup_broker(csco_session)



    # shard, seq, msg_d_1 = get_queue_message(csco_session, 242, SHARDS)
    # delete_queue_message(csco_session, 242, shard, seq)
