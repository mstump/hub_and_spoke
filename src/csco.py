#!/usr/bin/env python
#
#
# Copyright 2013, DataStax
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# This software is not "Supported Software" and, is not supported by DataStax under any software subscription or other agreement.
# See the License for the specific language governing permissions and limitations under the License.

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

TABLE_DEF_NODES = """CREATE TABLE %s.nodes (site_id int PRIMARY KEY, host text);"""

SHARDS = 4

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

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from multiprocessing import Process
from optparse import OptionParser
import cgi
import json
import mimetypes
import multiprocessing
import posixpath
import random
import string
import time
import traceback
import urllib
import urllib2
import urlparse
import uuid

BOUNDARY_CHARS = string.digits + string.ascii_letters

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


def shard_generator(shards):
    i = 0
    while True:
        if i > shards:
            i = 0
        yield i
        i += 1


def cassandra_connect(seed, keyspace=None):
    from cassandra.cluster import Cluster
    from cassandra.io.libevreactor import LibevConnection

    cluster = Cluster()
    cluster.connection_class = LibevConnection
    return cluster, cluster.connect(keyspace)


def initialize_cluster(session, keyspace):
    from cassandra import AlreadyExists

    try:
        session.execute(KEYSPACE_DEF % keyspace)
    except AlreadyExists:
        pass

    try:
        session.execute(TABLE_DEF_MAIN % keyspace)
    except AlreadyExists:
        pass

    try:
        session.execute(TABLE_DEF_QUEUE % keyspace)
    except AlreadyExists:
        pass

    try:
        session.execute(TABLE_DEF_NODES % keyspace)
    except AlreadyExists:
        pass


def persist_message(session, message):
    from cassandra.query import ValueSequence
    statement = "INSERT INTO %s(%s) %s;" % (message.message_type, ", ".join(message.attributes.keys()), "VALUES %s")
    session.execute(statement, parameters=[ValueSequence(message.attributes.values())])


def enqueue_message(session, shard, queues, message):
    for q in queues:
        seq = uuid.uuid1()
        statement = "INSERT INTO queue(site_id, shard, seq, message) VALUES(%s, %s, %s, %s);"
        session.execute(statement, [q, shard, seq, json.dumps(message, cls=MessageEncoder)])


def get_nodes(session):
    statement = "SELECT site_id, host FROM nodes;"
    return session.execute(statement)


def register_node(session, site_id, host, ttl=12*60):
    statement = "INSERT INTO nodes(site_id, host) VALUES(%s, %s) USING TTL %s;"
    return session.execute(statement, parameters=[site_id, host, ttl])


def delete_queue_message(session, queue, message):
    shard, seq = message.split(":")
    statement = "DELETE FROM queue WHERE site_id=%s AND shard=%s AND seq=%s;"
    return session.execute(statement, [queue, int(shard), uuid.UUID(seq)])


def get_queue_messages(session, queue, max_shards, limit=100):
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
                results.append(("%s:%s" % (shard, str(result[0][2])), json.loads(result[0][3], object_hook=message_decoder)))
                markers[shard] = result[0][2]

        if len(results) >= limit or len(markers) == 0:
            break

    return sorted(results, key=lambda x: x[1])


def http_register_node(session, path, postvars):
    assert len(path) == 2
    if not postvars.has_key("host"):
        return 400, "message parameter is required"

    site_id = int(path[1])
    hosts = postvars["host"]
    for host in hosts:
        register_node(session, site_id, host)

    return 200, ""


def http_get_queue_messages(session, path, query):
    # expects /queue/queue_name?limit=10
    assert len(path) >= 2
    queue_name = int(path[1])
    limit = query.get("limit", 10)
    output = get_queue_messages(session, queue_name, SHARDS, limit)
    return 200, output


def http_delete_queue_messages(session, path, postvars):
    # expects /queue/queue_name/delete with form data message param
    assert len(path) == 3
    if not postvars.has_key("message"):
        return 400, "message parameter is required"

    assert type(postvars["message"]) == type([])
    queue_name = int(path[1])

    output = []
    for message in postvars["message"]:
        try:
            delete_queue_message(session, queue_name, message)
            output.append([message, None])
        except Exception, e:
            output.append([message, str(e)])

    return 200, output

def http_post_message(session, shards, message_dispatch, postvars):
    if not postvars.has_key("message"):
        return 400, "message parameter is required"

    assert type(postvars["message"]) == type([])
    persist_only = postvars.has_key("persist_only")

    messages = []
    try:
        messages = [json.loads(x, object_hook=message_decoder) for x in postvars["message"]]
    except Exception, e:
        return 400, str(e)

    for msg in messages:
        persist_message(session, msg)
        if not persist_only:
            enqueue_message(session, next(shards), message_dispatch(msg), msg)

    return 200, ""


class CscoHTTPServer(HTTPServer):

    def __init__(self, session, shards, message_dispatch, *args, **kw):
        HTTPServer.__init__(self, *args, **kw)
        self.context = {"cassandra"        : session, \
                        "shards"           : shards,     \
                        "message_dispatch" : message_dispatch}

class CscoHandler(BaseHTTPRequestHandler):

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()


    def do_POST(self):
        session = self.server.context["cassandra"]
        shards = self.server.context["shards"]
        message_dispatch = self.server.context["message_dispatch"]


        url = urlparse.urlparse(self.path)
        path = [x.lower() for x in filter(lambda x: x != "", posixpath.normpath(urllib.unquote(url.path)).split('/'))]

        code = 500
        output = ''

        if len(path) == 3 and path[0] == "queue" and path[2] == "delete":
            ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
            if ctype == 'multipart/form-data':
                postvars = cgi.parse_multipart(self.rfile, pdict)
                code, output = http_delete_queue_messages(session, path, postvars)
            else:
                code = 400
                output = "only accepts multipart encoded form data"

        elif len(path) == 2 and path[0] == "nodes":
            ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
            if ctype == 'multipart/form-data':
                postvars = cgi.parse_multipart(self.rfile, pdict)
                code, output = http_register_node(session, path, postvars)
            else:
                code = 400
                output = "only accepts multipart encoded form data"

        elif len(path) == 1 and path[0] == "message":
            ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
            if ctype == 'multipart/form-data':
                postvars = cgi.parse_multipart(self.rfile, pdict)
                code, output = http_post_message(session, shards, message_dispatch, postvars)
            else:
                code = 400
                output = "only accepts multipart encoded form data"

        else:
            code = 404

        self.send_response(code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(output, cls=MessageEncoder))
        self.wfile.close()

    def do_GET(self):
        session = self.server.context["cassandra"]

        url = urlparse.urlparse(self.path)
        path = [x.lower() for x in filter(lambda x: x != "", posixpath.normpath(urllib.unquote(url.path)).split('/'))]
        query = dict(urlparse.parse_qsl(url.query))

        code = 500
        output = ''

        if path[0] == "queue":
            code, output = http_get_queue_messages(session, path, query)

        # elif path[0] == "message":
        #     code, output = http_get_messages(session, path, query)

        else:
            code = 404

        self.send_response(code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(output, cls=MessageEncoder))
        self.wfile.close()

def encode_multipart(fields, boundary=None):
    def escape_quote(s):
        return s.replace('"', '\\"')

    if boundary is None:
        boundary = ''.join(random.choice(BOUNDARY_CHARS) for i in range(30))
    lines = []

    for name, value in fields:
        lines.extend((
            '--{0}'.format(boundary),
            'Content-Disposition: form-data; name="{0}"'.format(escape_quote(name)),
            '',
            str(value),
        ))

    lines.extend((
        '--{0}--'.format(boundary),
        '',
    ))
    body = '\r\n'.join(lines)

    headers = {
        'Content-Type': 'multipart/form-data; boundary={0}'.format(boundary),
        'Content-Length': str(len(body)),
    }

    return (body, headers)


def client_post_message(host, messages, persist_only=False):
    url = "http://%s/message" % host
    fields = [["message", json.dumps(msg, cls=MessageEncoder)] for msg in messages]
    if persist_only:
        fields.append(["persist_only", True])

    data, headers = encode_multipart(fields)
    request = urllib2.Request(url, data=data, headers=headers)
    f = urllib2.urlopen(request)
    return f.getcode()


def client_delete_message(host, queue, messages):
    url = "http://%s/queue/%s/delete" % (host, queue)
    fields = [["message", msg] for msg in messages]
    data, headers = encode_multipart(fields)
    request = urllib2.Request(url, data=data, headers=headers)
    f = urllib2.urlopen(request)
    return f.getcode()


def client_register_node(host, site_id, listen_address):
    url = "http://%s/nodes/%s" % (host, site_id)
    fields = [["host", listen_address]]
    data, headers = encode_multipart(fields)
    request = urllib2.Request(url, data=data, headers=headers)
    f = urllib2.urlopen(request)
    return f.getcode()


def client_get_queue(host, queue):
    url = "http://%s/queue/%s/limit=10" % (host, queue)
    request = urllib2.Request(url)
    f = urllib2.urlopen(request)
    if f.getcode() == 200:
        return json.loads(f.read(), object_hook=message_decoder)
    else:
        return []


def manage_queues(cassandra_server, keyspace, my_id, message_dispatch, interval=10):
    shards = shard_generator(SHARDS)

    while True:
        print "checking queues for messages"
        cluster = None

        try:
            cluster, session = cassandra_connect(cassandra_server, keyspace)
            print "connected to cassandra"

            for site_id, node in get_nodes(session):
                print "checking remote queue %s on host %s for data" % (my_id, node)
                messages = client_get_queue(node, my_id)
                print "found %s messages in remote queue %s on host %s" % (len(messages), my_id, node)
                for msgid, message in messages:
                    persist_message(session, message)
                    dispatch_queues = [x for x in message_dispatch(message) if x != site_id]
                    print "dispatching %s to queues %s" % (msgid, dispatch_queues)
                    enqueue_message(session, next(shards), dispatch_queues, message)

                if len(messages) > 0:
                    print "deleting messages %s from remote queue %s on %s" % ([x[0] for x in messages], my_id, node)
                    print "received HTTP %s from %s" % (client_delete_message(node, my_id, [x[0] for x in messages]), node)

                print "checking local queue %s for messages destined for %s" % (site_id, node)
                messages = get_queue_messages(session, site_id, SHARDS)
                print "found %s messages in local queue %s for %s" % (len(messages), site_id, node)

                if len(messages) > 0:
                    print "posting messages %s to %s" % ([x[0] for x in messages], node)
                    print "received HTTP %s from %s" % (client_post_message(node, [x[-1] for x in messages], True), node)

                for message, _ in messages:
                    print "deleting message %s from local queue %s" % (message, site_id)
                    delete_queue_message(session, site_id, message)

        except Exception, e:
            traceback.print_exc()

        finally:
            try:
                if cluster:
                    cluster.shutdown()
            except Exception, e:
                traceback.print_exc()

        time.sleep(interval)


def periodically_register_node(host, site_id, listen_address, interval=5*60):
    while True:
        try:
            print client_register_node(host, site_id, listen_address)

        except Exception, e:
            traceback.print_exc()

        time.sleep(interval)


def central_dispatcher(queues):
    return (lambda _: queues)


def leaf_node_dispatcher(message):
    return [0]


def setup_central_broker(cassandra_server, keyspace, listen_hostname, listen_port, dispatcher):
    p = Process(target=manage_queues, args=(cassandra_server, keyspace, 0, dispatcher, ))
    p.start()

    cluster = None
    try:
        shards = shard_generator(SHARDS)
        cluster, session = cassandra_connect(cassandra_server, keyspace)

        server = CscoHTTPServer(session, shards, dispatcher, ('0.0.0.0', listen_port), CscoHandler)
        print('Started http server')
        server.serve_forever()

    except KeyboardInterrupt:
        print('^C received, shutting down server')
        server.socket.close()

    finally:
        try:
            if cluster:
                cluster.shutdown()
        except Exception, e:
            traceback.print_exc()


def setup_leaf_node(cassandra_server, keyspace, listen_hostname, listen_port, site_id, central_server):
    p = Process(target=periodically_register_node, args=(central_server, site_id, "%s:%s" % (listen_hostname, listen_port), ))
    p.start()

    cluster = None
    try:
        shards = shard_generator(SHARDS)
        cluster, session = cassandra_connect("localhost", keyspace)

        server = CscoHTTPServer(session, shards, leaf_node_dispatcher, ('0.0.0.0', listen_port), CscoHandler)
        print('Started http server')
        server.serve_forever()

    except KeyboardInterrupt:
        print('^C received, shutting down server')
        server.socket.close()

    finally:
        try:
            if cluster:
                cluster.shutdown()
        except Exception, e:
            traceback.print_exc()



if __name__ == "__main__":
    parser = OptionParser()

    parser.add_option("-k", "--keyspace",
                      action="store", dest="keyspace", type="string", metavar="KEYSPACE",
                      help="keyspace to use")

    parser.add_option("-l", "--leaf",
                      action="store", dest="leaf", type="string", metavar="ID",
                      help="act as leaf node with node ID")

    parser.add_option("-c", "--central",
                      action="store_true", dest="central", default=False,
                      help="act as central broker with ID")

    parser.add_option("-x", "--post",
                      action="store", dest="post", type="string", metavar="HOST",
                      help="post sample messages to host")

    parser.add_option("-i", "--initialize",
                      action="store_true", dest="initialize", default=False,
                      help="initialize the keyspace and column families")

    parser.add_option("-b", "--broker",
                      action="store", type="string", dest="central_server",
                      help="hostname and port of the central server")

    parser.add_option("-n", "--hostname",
                      action="store", type="string", dest="hostname", default="localhost",
                      help="the hostname that this server listens at")

    parser.add_option("-p", "--port",
                      action="store", dest="port", type="int", metavar="port", default=8080,
                      help="the hostname that this server listens at")

    parser.add_option("-s", "--seed",
                      action="store", type="string", dest="seed", default="localhost",
                      help="the hostname of the cassandra cluster seed")

    parser.add_option("-d", "--dispatch",
                      action="append", type="int", default=[],
                      help="central broker should dispatch messages to queues")


    (options, args) = parser.parse_args()

    keyspace = options.keyspace
    central = options.central
    leaf = options.leaf
    central_server = options.central_server
    dispatch = options.dispatch
    post = options.post
    hostname = options.hostname
    seed = options.seed
    port = options.port
    initialize = options.initialize

    if post:
        print "posting sample messages to %s" % post
        messages = [json.loads(x, object_hook=message_decoder) for x in [EXAMPLE_MESSAGE_1, EXAMPLE_MESSAGE_2]]
        print client_post_message(post, messages)
        exit(0)

    if not keyspace:
        print "no keyspace specified"
        parser.print_help()
        exit(1)

    if initialize:
        print "initializing keyspace and column families"
        cluster, session = cassandra_connect(seed)
        initialize_cluster(session, keyspace)
        cluster.shutdown()
        exit(0)

    if leaf:
        if not central_server:
            print "leaf node neads to know which server to use as the central broker"
            exit(1)

        print "starting leaf node"
        setup_leaf_node(seed, keyspace, hostname, port, leaf, central_server)

    elif central:
        print "starting central node"
        setup_central_broker(seed, keyspace, hostname, port, central_dispatcher(dispatch))

    else:
        parser.print_help()
        exit(1)
