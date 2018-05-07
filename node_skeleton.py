####################################
#File: node_skeleton.py
#Authors: Ang Deng <adeng3@gatech.edu>
#		  Akshay Nagendra <akshaynag@gatech.edu>
#		  Paul Yates <paul.maxyat@gatech.edu>
#		  <INSERT YOUR NAME HERE WITH EMAIL ADDRESS>
#
#Description: Skeleton Code for broadcasting algorithm proposed assignment for ECE 6102 SPRING 2018 Assignment 4

##################### IMPORTANT NOTE #############################
#Only modify areas marked with TODO; DO NOT MODIFY ANY OTHER CODE#
#INSTRUCTORS: Search by INSTRUCTOR NOTE to see important info    #
##################################################################


from socket import *
import sys
from thread import *
import time
from time import sleep
import select
import threading
import random
import re

if len(sys.argv) != 5:
    print "Correct usage: python node_skeleton.py <nodeID> <num_nodes> <mode (1=reliable, 2=fifo, 3=causal)> <delay (0=no delay, 1=delay)>"
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
	writeLogFile += "_RB" + str(nodeID)

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

outF = open(writeLogFile, "w")
#######################################################################################
#																					  #
#							TODO: ADD ANY GLOBALS HERE								  #
#																					  #






#																					  #
#######################################################################################

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

#######################################################################################
#																					  #
#							TODO: ADD ANY HELPER FUNCTIONS HERE 					  #
#																					  #






#																					  #
#######################################################################################


def r_broadcast(msg):
	#DESCRIPTION: Broadcast function for a Reliable Broadcast
	#TODO: ADD CODE HERE TO MAKE THIS WORK	
	return None

def f_broadcast(msg):
	#DESCRIPTION: Broadcast function for a FIFO Broadcast
	#TODO: ADD CODE HERE TO MAKE THIS WORK
	return None

def c_broadcast(msg):
	#DESCRIPTION: Broadcast function for a Causal Broadcast
	#TODO: ADD CODE HERE TO MAKE THIS WORK	
	return None	

def recv_handler(msg, tag):
	#TODO: ADD CODE HERE TO PROPERLY RECEIVE MESSAGES AS PER THE DELIVERY PROTOCOLS COVERED IN CLASS	
	return None

#INSTRUCTOR NOTE: THIS FUNCTION PROTOTYPE COULD BE PROVIDED OR REMOVED DEPENDING ON DIFFICULTY 
def r_deliver_handler(msg, tag):
   	#DESCRIPTION: Deliver function to handle delivery from R-Layer to the layer above
	#TODO: ADD CODE HERE TO PROPERLY R_DELIVER MESSAGES
	return None 

#INSTRUCTOR NOTE: THIS FUNCTION PROTOTYPE COULD BE PROVIDED OR REMOVED DEPENDING ON DIFFICULTY 
def f_deliver_handler(msg,tag):
	#DESCRIPTION: Deliver function to handle delivery from F-Layer to the layer above
	#TODO: ADD CODE HERE TO PROPERLY F_DELIVER MESSAGES	
	return None
	
def sendToApplication(msg, tag):
	#DESCRIPTION: Function to print out the message that has been delivered to the application layer (i.e. the highest layer)
	#TODO: Write to the writelog (hint: outF is the file handler for the output file)
	#CHECK THE ASSIGNMENT DESCRIPTION FOR CORRECT FORMATTING
	return None

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
						if msgMsg == TERM_MSG:
							print "TERM SIGNAL RECEIVED"
							TERM_DICT[int(msgTag)] = True
							print "NUMBER OF NODES THAT HAVE SENT TERM SIGNAL: %s" %(determineNumTerm())
						elif msgMsg == INTRO_MSG:
							print "RANDOM PACKET FROM NODE %s RECEIVED" %(msgTag)					
						else:
							recv_handler(msgMsg, msgTag)
					else:
						print "UNALLOWED PACKET: %s" %(msg)
		else:
			try:
				connectedClients.pop(addr, None)
			except:
				print "-----NODE CONNECTIONS REMOVED-----"
				connectedClients = {}
			print "\nClient from '{0}'' disconnected\n".format(addr)
			exit()

def overall_broadcast_fn():
	#DESCRIPTION: Function to handle the broadcast for the appropriate mode
	readFile = open(readLogFile,"r")
	flagLine = readFile.readline()
	for line in readFile:
	##################################	
	#TODO: ADD CODE HERE TO SEND MESSAGES AT CURRENT LOCAL SYSTEM TIME (HINT: systemTick is the local node tick)

	#HINT: USE r_broadcast(), c_broadcast(), and f_broadcast() from above here!
		print "I SHOULD PROBABLY DO SOMETHING HERE"	

	##################################

	#NOTE: DO NOT ALTER CODE FROM THIS POINT ONWARD!
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

try:
	# Meanwhile, we also listen for connections from other servers
	while len(connectedClients) < numNodes:
		connectionSocket, addr = serverSocket.accept()
		print "\nAccepted connection from '{0}'\n".format(addr[1])
		connectedClients[addr[1]] = connectionSocket
		start_new_thread(receive_fn,(connectionSocket, addr[1]))
	

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
