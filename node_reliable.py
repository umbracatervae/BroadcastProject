from socket import *
import sys
from thread import *
import time
from time import sleep
import select
import threading
import random
import re

lock = threading.Lock()
lock2 = threading.Lock()
if len(sys.argv) != 6:
    print "Correct usage: python node.py <nodeID> <num_nodes> <readLogFile> <writeLogFile> <mode (1=reliable, 2=fifo, 3=causal)>"
    exit()
nodeID = -1
numNodes = -1
readLogFile = ""
writeLogFile = ""
TERM_MSG = "<END>"
TERM_DICT = {}
INTRO_MSG = "INTRO"
USE_FIFO = False
USE_CAUSAL = False

try:
	nodeID = int(sys.argv[1])
	numNodes = int(sys.argv[2])
	readLogFile = sys.argv[3]
	writeLogFile = sys.argv[4]
	if(sys.argv[5] is '2'):
		USE_FIFO = True
	if(sys.argv[5] is '3'):
		USE_FIFO = True
		USE_CAUSAL = True

except ValueError:
	print "[ERROR] Could not parse nodeID and/or numNodes"
	exit()
if numNodes < nodeID:
	print "[ERROR] Specified nodeID out of range"
	exit()

for i in range(1,numNodes+1):
	TERM_DICT[i] = False


# we can have them sit on the same port... I just did it seperately here
serverPort = 55000 + nodeID 			# things are received here

serverSocket = socket(AF_INET,SOCK_STREAM)
serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
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

# List of which communication channels are down, used for faulty test case
# Channel from me (client) to other node (server)
# Indices refer to indices of clientSockets
brokenChannels = list()

# the list of undelivered nodes required by FIFO broadcast
fifoBuffer = list()


# the next expected sequence number required by FIFO broadcast
fifoNextSeq = [0]*numNodes

outF = open(writeLogFile, "w")
r_seq_num = 0
r_delivered = {}

prevDelivered = []

def isNodeComplete():
	global TERM_DICT
	for i in range(1,numNodes+1):
		if not TERM_DICT[i]:
			return False
	return True

def send_with_delay(msg, tag, targetID): #run this in a thread to simulate a slow communication channel
    randTime = random.random() * 3 #between 0 and 3 seconds
    sleep(randTime)
    send_to_one(msg, tag, targetID)


def send_to_one(msg, tag, targetID):
    if brokenChannels[targetID]:
        print("Cant send to %i, broken channel", targetID)
    if targetID > numNodes:
        print "ERROR: Destination does not exist"
    else:
        targetAddr = 55000 + targetID
        print "targetAddr: " + str(targetAddr)
        # If the server has never been connected to
        if not connectedServer.has_key(targetAddr):
                # Build a new client sockets
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

        modifiedMsg = str(tag) + ", " + msg.strip() + "\n"

        clientSocket.send(modifiedMsg.encode("utf-8"))
        print "\nSent to node %d: %s\n" % (targetID, modifiedMsg)

def send_to_all(msg, tag):
	targetID = 1
	while targetID <= numNodes:
		targetAddr = 55000 + targetID
		#print "targetAddr: " + str(targetAddr)
		# If the server has never been connected to
		if not connectedServer.has_key(targetAddr):
			# Build a new client sockets
			clientSocket = socket(AF_INET, SOCK_STREAM)
			clientSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

			# store the clientSocket port in order to be able to find the socket by address later. The key is client address
			clientSockets[clientSocket.fileno()] = clientSocket

			# register the socket to be able to detect I/O event of those sockets in the future
			poll.register(clientSocket)


			try:
				clientSocket.connect(('', targetAddr))
			except:
				return False

			# In case of connecting multiple times. The key is target server's address
			connectedServer[targetAddr] = clientSocket
		else:
			clientSocket = connectedServer[targetAddr]

		modifiedMsg = str(tag) + ", " + msg.strip() + "\n"

		clientSocket.send(modifiedMsg.encode("utf-8"))
		print "\nSent to node %d: %s\n" % (targetID, modifiedMsg)
		targetID += 1
	return True

def r_broadcast(msg):
	global r_seq_num
	tag = str(nodeID) + '-' + str(r_seq_num)
	r_seq_num += 1
	send_to_all(msg, tag)


def c_broadcast(msg):
	global prevDelivered
	sendMsg = "{"
	for pd in prevDelivered:
		sendMsg += str(pd) + ";"
	sendMsg = sendMsg[0:-1] + "}"
	sendMsg += ", " + msg
	r_broadcast(sendMsg)
	lock2.acquire()
	prevDelivered = []
	lock2.release()


def recv_handler(msg, tag):
	lock.acquire()
	if not r_delivered.has_key(tag):
		if int(tag.split('-')[0]) != nodeID:
			send_to_all(msg, tag)
                if USE_FIFO:
                    f_deliver(msg,tag)
                else:
                    r_deliver(msg, tag)
	lock.release()


def f_deliver(msg, tag):
    addr = int(tag.split('-')[0])
    seq = int(tag.split('-')[1])
    if(seq == fifoNextSeq[addr-1]): #the recieved message is is the next expected one which can be delivered
       fifoNextSeq[addr-1]+=1
       r_deliver(msg,tag)
       foundMatch = True
       while foundMatch:
           foundMatch = False
           for m in fifoBuffer: #fifo deliver the undelivered messages
                m_addr = int(m[1].split('-')[0])
                m_seq = int(m[1].split('-')[1])
                if m_addr == addr and m_seq == fifoNextSeq[addr-1]:
                    r_deliver(m[0],m[1])
                    foundMatch = True
                    break
    else: #the recieved message cannot be delivered because previous sequence numbers have not been delivered
        fifoBuffer.append([msg,tag])




def r_deliver(msg, tag):
	r_delivered[tag] = msg
	outF.write(tag)
	outF.write(',')
	outF.write(msg)
	outF.write("\n")

def receive_fn(connectionSocket, addr):
	global TERM_DICT
	while 1:
		messagesRAW = connectionSocket.recv(2048)
		if messagesRAW:
			#print "\nReceived %s from '%d'" % (messagesRAW, addr)
			for msg in messagesRAW.split("\n"):
				if msg:
					sObj = re.search(r'(.*?),(.*)',msg)
					if sObj:
						msgTag = sObj.group(1).strip()
						msgMsg = sObj.group(2).strip()
						print "RECEIVED: TAG: (%s) | MSG: (%s)" %(msgTag,msgMsg)
						if msgMsg == TERM_MSG:
							print "TERM SIGNAL RECEIVED"
							TERM_DICT[int(msgTag)] = True
							exit()
						elif msgMsg == INTRO_MSG:
							print "CONNECTION FROM NODE %s RECEIVED" %(msgTag)					
						else:
							recv_handler(msgMsg, msgTag)
		else:
			connectedClients.pop(addr, None)
			print "\nClient from '{0}'' disconnected\n".format(addr)
			exit()

def overall_broadcast_fn():
	readFile = open(readLogFile,"r")
	flagLine = readFile.readline()
	flags = flagLine.split()
	for f in flags:
		if f is '0':
			brokenChannels.append(False)
		else:
			brokenChannels.append(True)
	print("The broken channels are:")
	print(brokenChannels)
	#print "\n-----WAITING FOR OTHER NODES TO BE ONLINE-----\n"
	#sleep(10)
	for line in readFile:
		r_broadcast(line)
	###########
	#termination message time!
	sleep(10)
	send_to_all(TERM_MSG,str(nodeID))

def introduct_fn():
	success = False
	while not success:
		sucess = send_to_all(INTRO_MSG,str(nodeID))
		sleep(10)
		print "\n-----RETRYING TRANSMISSIONS-----\n"	
	

start_new_thread(introduct_fn,())

# Start wait for input to connect to other servers
#start_new_thread(overall_broadcast_fn,())

try:
	# Meanwhile, we also listen for connections from other servers
	while len(connectedClients) < numNodes:
		connectionSocket, addr = serverSocket.accept()
		print "\nAccepted connection from '{0}'\n".format(addr[1])
		connectedClients[addr[1]] = connectionSocket
		start_new_thread(receive_fn,(connectionSocket, addr[1]))
	print "-----ALL CONNECTIONS HAVE BEEN MADE-----"
	# Start wait for input to connect to other servers
	start_new_thread(overall_broadcast_fn,())
	while not isNodeComplete():
		i=0

finally:
	#print r_delivered
	# Close all connections upon any exception to avoid weird bugs
	serverSocket.close()
	outF.close()

	for clientSocket in clientSockets.values():
		clientSocket.close()

	for connectedClient in connectedClients.values():
		connectedClient.close()
