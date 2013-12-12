## Overview

There are two types of nodes, a leaf node and a central broker. A leaf node would be a manufacturing site, and a central broker would be cisco HQ.

When a leaf node boots up it will attempt to register itself every 5 minutes with the central broker specified by the ```-b``` commandline option. The leaf node specifies which queue it is interested in monitoring and at what IP/port it can be reached. If a leaf node doesn't check in within 15 minutes it will be removed from the registration.

Every 10 seconds the central broker will iterate through nodes with valid registration and attempt to check for messages that need to be pulled from each remote site. After each message is fetched it is persisted to the local keyspace, and checked to see if it needs to be replicated to a different leaf node. The leaf node dispatching is controlled by the ```-d``` command line option.

After fetching messages from the remote site the central broker will check if it has any messages for the leaf node. If messages are in the queue it will post these messages to the remote node, and they will be persisted.

If a message is posted to the central broker then this process is reversed. The central broker will persist the message to the local keyspace, and then dispatch the message to queues for the corresponding remote nodes.

Every 10 seconds the central broker will check if it has messages for remote nodes with valid registrations and will post those messages to the remote site. The remote site will then persist the message to it's local keyspace.

## Available options
```
$ ./src/csco.py -h
Usage: csco.py [options]

Options:
  -h, --help            show this help message and exit
  -k KEYSPACE, --keyspace=KEYSPACE
                        keyspace to use
  -l ID, --leaf=ID      act as leaf node with node ID
  -c, --central         act as central broker with ID
  -x HOST, --post=HOST  post sample messages to host
  -i, --initialize      initialize the keyspace and column families
  -b CENTRAL_SERVER, --broker=CENTRAL_SERVER
                        hostname and port of the central server
  -n HOSTNAME, --hostname=HOSTNAME
                        the hostname that this server listens at
  -p port, --port=port  the hostname that this server listens at
  -s SEED, --seed=SEED  the hostname of the cassandra cluster seed
  -d DISPATCH, --dispatch=DISPATCH
                        central broker should dispatch messages to queues
```

## Example usage
These steps assume cassandra is running on localhost, if this is not true please specify a seed node using the ```-s``` commandline option.

1. Initialize the keyspace for the central broker ```python csco.py -i -k csco```
1. Initialize the keyspace for the leaf node ```python csco.py -i -k cm_1```
1. Start the central broker ```python csco.py -c -k csco -p 8080 -d 242```
1. Start the leaf node ```python csco.py -l 242 -k cm_1 -p 8181 -b localhost:8080```
1. Post a message to the central broker or the leaf node ```python csco.py -x localhost:8080```
