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
import StringIO
import gzip
import socket

from multiprocessing import cpu_count
from copy import copy
from hashlib import md5
from twisted.protocols import basic
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet import defer
from twisted.internet import task

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
class Flush():
  offset = 1
  def start(self):
    """
      Start the flusher
    """
    def flush():
      try:
        writer = WriteRecord()
        data = copy(writer.records)
        writer.records = {}
      except:
        return None

      if not data == {}:
        Flush().do(data)
      task.deferLater(reactor, 1.0, flush)
    task.deferLater(reactor, 1.0, flush)

  def do(self, data):
    if data == {}:
      return None

    with Timer() as t:
      #print "Uncompressed %s kB" % (int(sys.getsizeof(json.dumps(data))) / 1024)

      out = StringIO.StringIO()
      with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(json.dumps(data))

      print "Compressed %s bytes" % int(sys.getsizeof(out.getvalue()))

      print ">>> Executing: %d" % self.offset
      with Timer(True) as t:
        try:
          connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
          connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
          connection.connect_ex(('es0', 9889))
          connection.sendall(out.getvalue())
          connection.shutdown(1)
          connection.close()
        except:
          print ">>> Exception"
    self.offset += 1


@singleton
class WriteRecord():
  records  = {}

  def store(self, key, data):
    data = json.loads(data)

    """
      Validate the data
    """
    if 'location' not in data:
      return None

    try:
      location       = copy(data['location'].encode('utf-8'))
      location_hash  = md5(location).hexdigest()[0:7]
      del data['location']
    except:
      return None

    """
      Format the record and then stash it for bulk writing
    """
    if location_hash not in self.records:
      self.records[location_hash] = []

    self.records[location_hash].append({
      '@timestamp': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+0000"),
      '@source_key': key,
      '@location': location,
      '@data': data
    })


"""
  Parses incoming data
"""
class LineParser(basic.LineReceiver):
  clients = []

  def __init__(self):
    self.MAX_LENGTH = 10073741824

  def connectionMade(self):
    self.clients.append(self)

  def connectionLost(self, reason):
    self.clients.remove(self)

  def lineReceived(self, line):
    command, key = line.split(' ', 1)

    if command == "write":
      if self._is_valid_api_key(key) is not True:
        print "!WARNING! Incorrect API key"
        client.abortConnection() # sever the connection
        return None

      command, key, data = line.split(' ', 2)
      WriteRecord().store(key, data)

  def _is_valid_api_key(self, key):
    """
      Todo: Check if API key is valid
    """
    return True


"""
  Socket factory. Receives and builds connections.
"""
class ConnectionFactory():
  protocol = LineParser

  def doStart(self):
    pass

  def startedConnecting(self, connectorInstance):
    pass

  def buildProtocol(self, address):
    return self.protocol()

  def clientConnectionLost(self, connection, reason):
    pass

  def clientConnectionFailed(self, connection, reason):
    pass

  def doStop(self):
    pass


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
    port = reactor.listenTCP(9888, ConnectionFactory())
    print "<<< Listening on port 9888"

    fds = {}

    for i in range(cpu_count()-1):
      fds[i] = i

    for i in range(0):
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
      socket.AF_INET, 
      ConnectionFactory()
    )

  def start(self):
    reactor.run()


def main(fd=None):
  daemon = Daemon()
  if fd is None:
    daemon.master(fd)
  else:
    daemon.child(fd)

  Flush().start()
  daemon.start()


if __name__ == '__main__':
  if len(sys.argv) == 1:
    main()
  else:
    main(int(sys.argv[1]))