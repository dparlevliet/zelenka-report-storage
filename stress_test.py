#!/usr/bin/env python
import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

import time
import json
import subprocess
from socket import AF_INET
from multiprocessing import cpu_count
from twisted.protocols import basic
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet import task

DEBUG = True
if DEBUG:
  from twisted.python import log
  log.startLogging(sys.stdout)


DESTINATION_SERVER_IP = "localhost"
DESTINATION_SERVER_PORT = 9888


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
      print 'elapsed time: %f ms' % self.msecs


class ClientLineReceiver(basic.LineReceiver):
  """
  """
  def connectionMade(self):
    print ">>> Connection made"
    with Timer(True) as t:
      for i in range(4000):
        self.sendLine('write abcd1234 %s' % json.dumps({
          "location":"cpu",
          "data": {
            "total": {
              "usage": 85,
              "nice": 0,
              "system": 50,
              "idle": 25,
              "iowait": 2,
              "irq": 0,
              "softirq": 0,
              "steal": 1,
              "guest": 35,
              "guest_nice": 0
            },
            "cores": {
              0: {
                "usage": 85,
                "nice": 0,
                "system": 50,
                "idle": 25,
                "iowait": 2,
                "irq": 0,
                "softirq": 0,
                "steal": 1,
                "guest": 35,
                "guest_nice": 0
              },
              1: {
                "usage": 85,
                "nice": 0,
                "system": 50,
                "idle": 25,
                "iowait": 2,
                "irq": 0,
                "softirq": 0,
                "steal": 1,
                "guest": 35,
                "guest_nice": 0
              },
              2: {
                "usage": 85,
                "nice": 0,
                "system": 50,
                "idle": 25,
                "iowait": 2,
                "irq": 0,
                "softirq": 0,
                "steal": 1,
                "guest": 35,
                "guest_nice": 0
              },
              3: {
                "usage": 85,
                "nice": 0,
                "system": 50,
                "idle": 25,
                "iowait": 2,
                "irq": 0,
                "softirq": 0,
                "steal": 1,
                "guest": 35,
                "guest_nice": 0
              },
              4: {
                "usage": 85,
                "nice": 0,
                "system": 50,
                "idle": 25,
                "iowait": 2,
                "irq": 0,
                "softirq": 0,
                "steal": 1,
                "guest": 35,
                "guest_nice": 0
              },
              5: {
                "usage": 85,
                "nice": 0,
                "system": 50,
                "idle": 25,
                "iowait": 2,
                "irq": 0,
                "softirq": 0,
                "steal": 1,
                "guest": 35,
                "guest_nice": 0
              },
              6: {
                "usage": 85,
                "nice": 0,
                "system": 50,
                "idle": 25,
                "iowait": 2,
                "irq": 0,
                "softirq": 0,
                "steal": 1,
                "guest": 35,
                "guest_nice": 0
              },
              7: {
                "usage": 85,
                "nice": 0,
                "system": 50,
                "idle": 25,
                "iowait": 2,
                "irq": 0,
                "softirq": 0,
                "steal": 1,
                "guest": 35,
                "guest_nice": 0
              },
            }
          }
        }))
      time.sleep(1)

    self.transport.loseConnection()

  def connectionLost(self, reason):
    print ">>> Connection lost"
    reactor.stop()

  def lineReceived(self, line):
    print line
    pass


class ClientConnectionFactory(protocol.ClientFactory):
  protocol = ClientLineReceiver



def main():
  factory = ClientConnectionFactory()
  reactor.connectTCP(
    DESTINATION_SERVER_IP, 
    DESTINATION_SERVER_PORT, 
    factory
  )
  reactor.run()


if __name__ == '__main__':
  if len(sys.argv) > 1:
    main()
  else:
    for i in range(24000):
      print ">>> ", "/usr/bin/python %s run" % os.path.realpath(__file__)
      subprocess.Popen(
        "/usr/bin/python %s run" % os.path.realpath(__file__), 
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
      )
      time.sleep(1.0)
