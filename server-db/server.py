#!/usr/bin/env python
import os
import sys
base_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(base_path)

import json
import time
import datetime
import subprocess
import hashlib
import random
import zlib

from socket import AF_INET
from multiprocessing import cpu_count
from copy import copy
from hashlib import md5
from twisted.protocols import basic
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet import task
from elasticsearch import Elasticsearch
from elasticsearch import helpers as ESHelpers

# Set the working timezone to GMT
os.environ['TZ'] = 'Europe/London'
time.tzset()

DEBUG = False
if DEBUG:
  from twisted.python import log
  log.startLogging(sys.stdout)


class Timer(object):
  def __init__(self, verbose=False):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end    = time.time()
    self.secs   = self.end - self.start
    self.msecs  = self.secs * 1000  # millisecs
    if self.verbose:
      print 'Elapsed time: %f ms' % self.msecs


def singleton(cls):
  instances = {}
  def getinstance():
    if cls not in instances:
      instances[cls] = cls()
    return instances[cls]
  return getinstance


@singleton
class DB():
  _cursor = None
  def cursor(self):
    if not self._cursor:
      self._cursor = Elasticsearch(
        [
          'es0', 
          #'es1'
        ]
      )
    return self._cursor


@singleton
class WriteRecord():
  tables    = {}
  records   = {}
  offset    = 1

  def received(self, _buffer):
    def write(cb, _buffer, offset):
      data = json.loads(zlib.decompress(''.join(_buffer), 16+zlib.MAX_WBITS))
      writer = WriteRecord()
      for location_hash in data:
        records = data[location_hash]
        location = records[0]['@location']
        with Timer() as t:
          self.store(location_hash, location, records)
        print ">>> Written (%d) %s in %s ms" % (offset, len(records), t.msecs)

    d = defer.Deferred()
    d.addCallback(write, _buffer, self.offset)
    d.callback('write')
    self.offset += 1

  def store(self, location, location_hash, records):
    """
      Write the location to the locations index for quick referencing
    """
    if location not in self.tables:
      self.tables[location] = {'schema': []}

    DB().cursor().index(
      index='locations',
      doc_type='location_string',
      id=location,
      body={
        '@hash': location_hash
      },
      ignore=400
    )

    """
      Write all possible fields that have been recorded for quick reference
    """
    def _write_column_name(result, location_hash, column_name):
      DB().cursor().index(
        index= '%s_schema' % location_hash,
        doc_type='location_schema',
        id=column_name,
        body={
          '@data': column_name
        },
        ignore=400
      )

    for record in records:
      for column_name in record:
        if column_name in self.tables[location]['schema']:
          continue

        self.tables[location]['schema'].append(column_name)
        d = defer.Deferred()
        d.addCallback(_write_column_name, location_hash, column_name)
        d.callback('write')

    """
      Bulk write all records
    """
    db = DB().cursor()
    with Timer(True) as t:
      ESHelpers.bulk(
        db, 
        records, 
        index=location_hash,
        doc_type="received"
      )


class ReceiverProtocol(basic.LineReceiver):
  __buffer = []

  def __init__(self):
    self.MAX_LENGTH = 10073741824

  def rawDataReceived(self, data):
    #print ">>> Received ..."
    self.__buffer.append(data)

  def connectionMade(self):
    print ">>> Connection made (%d)" % WriteRecord().offset
    self.setRawMode()

  def connectionLost(self, reason):
    #print ">>> Connection lost"
    WriteRecord().received(copy(self.__buffer))
    self.__buffer = []


class ReceiverFactory(protocol.ServerFactory):
  def buildProtocol(self, addr):
    return ReceiverProtocol()

"""
  Main controller for this application
"""
class Daemon():
  def __init__(self):
    pass

  def master(self, fd):
    """
      Reactor master instance configure
    """
    # Create a new listening port and several other processes to help out.
    port = reactor.listenTCP(9889, ReceiverFactory())
    print "<<< HTTP Listening on port 9889"

    fds = {}

    for i in range(cpu_count()-1):
      fds[i] = i
    fds[port.fileno()] = port.fileno()

    for i in range(cpu_count()):
      reactor.spawnProcess(
        None, 
        sys.executable, 
        [
          sys.executable, 
          __file__, 
          str(port.fileno())
        ],
        childFDs=fds,
        env=os.environ
      )

  def child(self, fd):
    """
      Reactor child instance configure
    """
    # Attach to the existing port instance                                                                         
    port = reactor.adoptStreamPort(
      fd, 
      AF_INET, 
      ReceiverFactory()
    )

  def start(self):
    """
      Start the reactor
    """
    reactor.run()


def main(fd=None):
  daemon = Daemon()
  if fd is None:
    daemon.master(fd)
  else:
    daemon.child(fd)

  daemon.start()


if __name__ == '__main__':
  if len(sys.argv) == 1:
    main()
  else:
    main(int(sys.argv[1]))