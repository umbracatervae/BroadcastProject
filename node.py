from socket import *
import sys
from thread import *
import time
import select


if len(sys.argv) != 4:
    print "Correct usage: python node.py <nodeID> <num_nodes>"
    exit()
nodeID = -1
numNodes = -1
readLogFile = ""
try:
	nodeID = int(sys.argv[1])
	numNodes = int(sys.argv[2])
	readLogFile = sys.argv[3]

except ValueError:
	print "[ERROR] Could not parse nodeID and/or numNodes"
	exit()
if numNodes < nodeID:
	print "[ERROR] Specified nodeID out of range"
	exit()

# we can have them sit on the same port... I just did it seperately here
serverPort = 55000 + nodeID 			# things are received here

serverSocket = socket(AF_INET,SOCK_STREAM)
serverSocket.bind(('', serverPort))
serverSocket.listen(1)

print "Node %d is listening on port %d" % (nodeID, serverPort)

# Store me as client socket to check for I/O event. Read 16.1.2 of https://docs.python.org/2/library/select.html
# The value returned by calling poll again is my node client port address.
poll = select.poll()

# Me as client and other as server . Key is my node cliet port address.
clientSockets = {}

# Me as client and other as server. Connection start from me. Key is the other's server port address.
connectedServer = {}

# Me as server and other as client. Connection start from others. Key is the other's client port address
connectedClients = {}



def receive_fn(connectionSocket, addr):
	while 1:
		sentence = connectionSocket.recv(1024)
		if sentence:
			print "\nReceived %s from '%d'" % (sentence, addr)
		else:
			connectedClients.pop(addr, None)
			print "\nClient from '{0}'' disconnected\n".format(addr)
			exit()

def send_fn():
	readFile = open(readLogFile,"r")
	reowr = raw_input("About to send read log...\n")
	for line in readFile:
		targetID = 1
		while targetID <= numNodes:
			targetAddr = 55000 + targetID

			# If the server has never been connected to
			if not connectedServer.has_key(targetAddr):
				# Build a new client socket
				clientSocket = socket(AF_INET, SOCK_STREAM)

				# store the clientSocket port in order to be able to find the socket by address later. The key is client address
				clientSockets[clientSocket.fileno()] = clientSocket

				# register the socket to be able to detect I/O event of those sockets in the future
				poll.register(clientSocket)

				clientSocket.connect(('', targetAddr))

				# In case of connecting multiple times. The key is target server's address
				connectedServer[targetAddr] = clientSocket
			else:
				clientSocket = connectedServer[targetAddr]

			modifiedline = "(" + str(targetID) + ", " + line.strip() + ")\n"

			clientSocket.send(modifiedline.encode("utf-8"))
			print "\nSent to node %d: %s\n" % (targetID, modifiedline)
			targetID += 1

# Start wait for input to connect to other servers
start_new_thread(send_fn,())

try:
	# Meanwhile, we also listen for connections from other servers
	i = 0
	connectFlag = False
	while True:
		connectionSocket, addr = serverSocket.accept()
		print "\nAccepted connection from '{0}'\n".format(addr[1])
		connectedClients[addr[1]] = connectionSocket
		start_new_thread(receive_fn,(connectionSocket, addr[1]))
		if len(connectedClients) == numNodes-1 and not connectFlag:
			print "\n-----ALL CONNECTIONS MADE-----\n"
			connectFlag = True
except Exception as err:
	print err

	# Close all connections upon any exception to avoid weird bugs
	serverSocket.close()

	for clientSocket in clientSockets.values():
		clientSocket.close()

	for connectedClient in connectedClients.values():
		connectedClient.close()
