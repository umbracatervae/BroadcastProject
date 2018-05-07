####################################
#File: node_causal.py
#Authors: Ang Deng <adeng3@gatech.edu>
#		  Akshay Nagendra <akshaynag@gatech.edu>
#		  Paul Yates <paul.maxyat@gatech.edu>
#
#Description: Solution for the broadcasting algorithm proposed assignment for ECE 6102 SPRING 2018 Assignment 4
###################################
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
lock3 = threading.Lock()
if len(sys.argv) != 5:
    print "Correct usage: python node.py <nodeID> <num_nodes> <mode (1=reliable, 2=fifo, 3=causal)> <delay (0=no delay, 1=delay)>"
    exit()
nodeID = -1
numNodes = -1
readLogFile = "ReadLog_"
writeLogFile = ""
TERM_MSG = "<END>"
TERM_DICT = {}
INTRO_MSG = "<HAI>"
USE_FIFO = False
USE_CAUSAL = False
terminated = False
DELAY_MODE = False
killClock = False
systemTick = 0

try:
	nodeID = int(sys.argv[1])
	numNodes = int(sys.argv[2])
	if(sys.argv[3] is '2'):
		USE_FIFO = True
	if(sys.argv[3] is '3'):
		USE_FIFO = True
		USE_CAUSAL = True
	if(sys.argv[4] is '1'):
		DELAY_MODE = True


except ValueError:
	print "[ERROR] Could not parse nodeID and/or numNodes"
	exit()
if numNodes < nodeID:
	print "[ERROR] Specified nodeID out of range"
	exit()

for i in range(1,numNodes+1):
	TERM_DICT[i] = False


readLogFile = "ReadLog_" + str(nodeID)
readLogFile += ".log"

writeLogFile = "writeLogFile"
if USE_CAUSAL:
	writeLogFile += "_CAUSAL" + str(nodeID) 
elif USE_FIFO:
	writeLogFile += "_FIFO" + str(nodeID)
else:
	writeLogFile += str(nodeID)

writeLogFile += ".log"

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
transFile = open("Trans" + str(nodeID) +".log","w+")
transFile.write("TRANSMISSION LOG\n")
transFile.close()
r_seq_num = 0
r_delivered = {}

prevDelivered = []
c_delivered = {}

def systemTicker():
	while not killClock:
		global systemTick
		randTime = random.random() * 3
		sleep(randTime)
		systemTick += 1	
		#print "\n-----SYSTEM CLK: %d-----\n" %(systemTick)	

def setupBrokenChannels():
	brokenFile = open(readLogFile,"r")
	flagLine = brokenFile.readline()
	flags = flagLine.split()
	for f in flags:
		if f is '0':
			brokenChannels.append(False)
		else:
			brokenChannels.append(True)
	print("The broken channels are:")
	print(brokenChannels)	

def isNodeComplete():
	global TERM_DICT
	for i in range(1,numNodes+1):
		if not TERM_DICT[i]:
			return False
	return True

def determineNumTerm():
	global TERM_DICT
	count = 0
	for i in range(1,numNodes+1):
		if TERM_DICT[i]:
			count += 1
	return count

def send_with_delay(msg, tag, targetID): #run this in a thread to simulate a slow communication channel
    randTime = random.random() * 3 #between 0 and 3 seconds
    sleep(randTime)
    send_to_one(msg, tag, targetID)

def send_to_one(msg, tag, targetID):
	urgent = False
	if msg is INTRO_MSG or msg is TERM_MSG:
		urgent = True
	if brokenChannels[targetID-1] and not urgent:
		print "Cant send to %d due to broken channel" %(targetID)
		return False
	if targetID > numNodes:
		print "ERROR: Destination does not exist"
		return False
	else:
		targetAddr = 55000 + targetID
        # If the server has never been connected to
		if not connectedServer.has_key(targetAddr):
                # Build a new client sockets
				clientSocket = socket(AF_INET, SOCK_STREAM)

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

		modifiedMsg = "(" + str(tag) + ", " + msg.strip() + ")\n"

		clientSocket.send(modifiedMsg.encode("utf-8"))
		print "\nSent to node %d: %s\n" % (targetID, modifiedMsg)
		return True

def send_to_all(msg, tag):
	targetID = 1
	while targetID <= numNodes:
		if DELAY_MODE:
			send_with_delay(msg,tag,targetID)
			#start_new_thread(send_with_delay,(msg,tag,targetID))
		else:
			send_to_one(msg,tag,targetID)	
		targetID += 1
	return True

def r_broadcast(msg):
	global r_seq_num
	tag = str(nodeID) + "-" + str(r_seq_num)
	r_seq_num += 1
	send_to_all(msg, tag)


def c_broadcast(msg):
	global prevDelivered
	lock2.acquire()
	sendMsg = "{"
	transFile = open("Trans" + str(nodeID) +".log","a")
	transFile.write("NODE ")
	transFile.write(str(nodeID))
	transFile.write(" IS SENDING MSG WITH TAG ")
	transFile.write(str(nodeID) + "-" + str(r_seq_num))
	transFile.write(" WITH PREVDELIV: ")
	
	for pd in prevDelivered:
		sendMsg += "(" + str(pd[0]) + "," + str(pd[1])  + ");"
		transFile.write("[")
		transFile.write(str(pd[1]))
		transFile.write("],")
		
	if prevDelivered:
		sendMsg = sendMsg[0:-1] + "}"
	else:
		transFile.write("NONE")
		sendMsg = "{}"
	transFile.write("\n")
	transFile.close()
	sendMsg += ", " + msg
	r_broadcast(sendMsg)
	prevDelivered = []
	lock2.release()



def recv_handler(msg, tag):
	lock.acquire()
	if not r_delivered.has_key(tag):
		if int(tag.split('-')[0]) != nodeID:
			send_to_all(msg, tag)
                if USE_FIFO:
                    r_deliver_handler(msg,tag)
                else:
                    sendToApplication(msg, tag)
	lock.release()


def r_deliver_handler(msg, tag):
    addr = int(tag.split('-')[0])
    seq = int(tag.split('-')[1])
    if(seq == fifoNextSeq[addr-1]): #the recieved message is is the next expected one which can be delivered
       fifoNextSeq[addr-1]+=1
       if not USE_CAUSAL:
           sendToApplication(msg,tag)
       else:
           f_deliver_handler(msg,tag)
       foundMatch = True
       while foundMatch:
           foundMatch = False
           for m in fifoBuffer: #fifo deliver the undelivered messages
                m_addr = int(m[1].split('-')[0])
                m_seq = int(m[1].split('-')[1])
                if m_addr == addr and m_seq == fifoNextSeq[addr-1]:
                    if not USE_CAUSAL:
                        sendToApplication(m[0],m[1])
                    else:
                        f_deliver_handler(msg,tag)
                    foundMatch = True
                    break
    else: #the recieved message cannot be delivered because previous sequence numbers have not been delivered
        fifoBuffer.append([msg,tag])

def f_deliver_handler(msg,tag):
	lock2.acquire()	
	msgList = []
	s = re.search(r'\{(.*)\},(.*)',msg)
	if s:
		messages = s.group(1).split(';')
		for m in messages:
			s2 = re.search(r'\((.*?),(.*)\)',m)
			if s2:
				msgTag = s2.group(2).strip()
				msgMsg = s2.group(1).strip()
				msgList.append((msgMsg,msgTag))
		currentMsg = s.group(2).strip()
		msgList.append((currentMsg,tag))

		#print "MESSAGE LIST: %s" %(msgList)
		#lock.acquire()
		for m in msgList:
			if not c_delivered.has_key(m[1]):
				#this means it has not been delivered yet
				c_delivered[m[1]] = m[0]
				sendToApplication(m[0],m[1])
				prevDelivered.append(m)		
		#lock.release()
	else:
		print "[ERROR] UNSUPPORTED MESSAGE RECEIVED"
	lock2.release()	
	


def sendToApplication(msg, tag):
	r_delivered[tag] = msg
	outF.write("NODE CLK: ")
	outF.write(str(systemTick))
	outF.write("\tTAG: ")
	outF.write(tag)
	outF.write('\t')
	outF.write(msg)
	outF.write("\n")

def receive_fn(connectionSocket, addr):
	global TERM_DICT
	while True:
		messagesRAW = connectionSocket.recv(2048)
		if messagesRAW:
			messages = messagesRAW.split("\n")
			for msg in messages:
				if msg:
					sObj = re.search(r'\((.*?),(.*)\)',msg)
					if sObj:
						msgTag = sObj.group(1).strip()
						msgMsg = sObj.group(2).strip()
						print "RECEIVED: TAG: (%s) | MSG: (%s)" %(msgTag,msgMsg)
						if msgMsg == TERM_MSG:
							print "TERM SIGNAL RECEIVED"
							TERM_DICT[int(msgTag)] = True
							print "NUMBER OF NODES THAT HAVE SENT TERM SIGNAL: %s" %(determineNumTerm())
							#exit()
						elif msgMsg == INTRO_MSG:
							print "RANDOM PACKET FROM NODE %s RECEIVED" %(msgTag)					
						else:
							recv_handler(msgMsg, msgTag)
					else:
						print "UNALLOWED PACKET: %s" %(msg)
		else:
			connectedClients.pop(addr, None)
			print "\nClient from '{0}'' disconnected\n".format(addr)
			exit()

def overall_broadcast_fn():
	readFile = open(readLogFile,"r")
	flagLine = readFile.readline()
	#print "\n-----WAITING FOR OTHER NODES TO BE ONLINE-----\n"
	#sleep(10)
	for line in readFile:
		sObj = re.search(r'^Timestamp:(\d+)\s+(.*)$',line)
		if sObj:
			reqTimeTick = int(sObj.group(1))
			rawMsg = sObj.group(2)
			while systemTick < reqTimeTick:
				sleep(1)
			if not USE_CAUSAL:
				r_broadcast(rawMsg)
			else:
				c_broadcast(rawMsg)
		else:
			print "[ERROR] Unsupported Data, Skipping line: %s" %(line)
	###########
	#termination message time!
	sleep(10)
	for i in range(1,numNodes+1):
		send_to_one(TERM_MSG,str(nodeID),i)

def random_packet_fn():
	while True:
		if terminated:
			break
		for i in range(1,numNodes+1):	
			send_to_one(INTRO_MSG,str(nodeID),i)
		sleep(10)



setupBrokenChannels()
start_new_thread(random_packet_fn,())

# Start wait for input to connect to other servers
#start_new_thread(overall_broadcast_fn,())

try:
	# Meanwhile, we also listen for connections from other servers
	while len(connectedClients) < numNodes:
		connectionSocket, addr = serverSocket.accept()
		print "\nAccepted connection from '{0}'\n".format(addr[1])
		connectedClients[addr[1]] = connectionSocket
		start_new_thread(receive_fn,(connectionSocket, addr[1]))
	#print "-----ALL CONNECTIONS HAVE BEEN MADE-----"
	# Start wait for input to connect to other servers
	start_new_thread(overall_broadcast_fn,())

	#start the system clock
	start_new_thread(systemTicker,())	

	while not isNodeComplete():
		i=0
	terminated = True
	while True:
		print "------------------------------------------------------------------------------------------"
		print "-----YOU MAY CTRL+C THIS TERMINAL IF ALL OTHER NODES ARE DISPLAYING ONLY THIS MESSAGE-----"
		print "------------------------------------------------------------------------------------------"
		sleep(10)

except:
	print "\n-----NODE %d TERMINATED-----\n" %(nodeID)

finally:
	print "\n-----PERFORMING CLEANUP-----\n"
	killClock = True
	# Close all connections upon any exception to avoid weird bugs
	serverSocket.close()
	outF.close()

	for clientSocket in clientSockets.values():
		clientSocket.close()

	for connectedClient in connectedClients.values():
		connectedClient.close()
