
import socket

connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
connection.connect(('es0', 9889))
#connection.sendall(out.getvalue())
connection.shutdown(1)
connection.close()
print ">>> Closed"