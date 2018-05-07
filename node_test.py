from socket import *
import sys
import multiprocessing
import time
import threading


if len(sys.argv) != 2:
    print "Correct usage: script, node ID"
    exit()

nodeID = int(sys.argv[1])

# we can have them sit on the same port... I just did it seperately here
serverPort = 55000 + (nodeID * 2) 			# things are received here
clientPort = 55000 + (nodeID * 2) + 1 		# things are sent from here
serverSocket = socket(AF_INET,SOCK_STREAM)
clientSocket = socket(AF_INET, SOCK_STREAM)
serverSocket.bind(('', serverPort))
clientSocket.bind(('', clientPort))
serverSocket.listen(1)
print "Node %d is listening on port %d" % (nodeID, serverPort)


def receive_fn():
	while 1:
		connectionSocket, addr = serverSocket.accept()
		sentence = connectionSocket.recv(1024)
		fromNode = (addr[1] - 55001) / 2
		print "\nReceived From node %d: %s" % (fromNode, sentence)
		print "Input target node: "
		connectionSocket.close()

def send_fn():
	while 1:
		targetID = int(raw_input("Input target node: "))
		clientSocket.connect(('', 55000 + (targetID * 2)))
		sentence = raw_input("Input lowercase sentence: ")
                if not sentence:
                    print "Exiting send function"
                    clientSocket.close()
                    break;
		clientSocket.send(sentence.encode("utf-8"))
		print "Sent to node %d: %s" % (targetID, sentence)
		clientSocket.close()

# threading.Thread(target=receive_fn).start()
threading.Thread(target=send_fn).start()

while 1:
        connectionSocket, addr = serverSocket.accept()
        sentence = connectionSocket.recv(1024)
        fromNode = (addr[1] - 55001) / 2
        print "\nReceived From node %d: %s" % (fromNode, sentence)
        print "Input target node: "
        connectionSocket.close()
#rec = multiprocessing.Process(target=receive_fn)
#sen = multiprocessing.Process(target=send_fn)
#rec.daemon = True
#sen.daemon = True
#rec.start()
#sen.start()
#print "sleep for 10 sec"
#print "done sleeping"


