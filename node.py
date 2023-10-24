import datetime
import os
import json
import sys
import socket
import heapq
from threading import Thread, Lock
import time
import errno
from collections import defaultdict
import csv
import pdb

local_node = ''
nodes_connected = {}
times = 0
connectedNodeNum_Lock = Lock()
times_lock = Lock()
receivedRepo = []  
receivedRepo_Lock = Lock()
connectedNodeNum = 0
connectedNodeNum_LOCK = Lock()
socketDict = {}
socketDict_Lock = Lock()
bytesNum = 0
bytesNum_Lock = Lock()
head_Lock = Lock()
sort_Lock = Lock()
num_bytes_recv = 0
num_bytes_recv_lock = Lock()
sleep_time = 0.05


class message:
    def __init__(self, ID, senderID, transaction, sequence, deliverable):
        self.ID = ID                    #origin time + node 'time.node'
        self.senderID = senderID        #node of message sender 
        self.transaction = transaction  #transaction information
        self.sequence = sequence        #new time + node 'time.node'
        self.deliverable = deliverable  #0-not deliverable, 1-deliverable
        

class Process:
    def __init__(self):
        self.queue = [] #priority queue
        self.items = 0  #num of items
        self.accounts = {}  #accounts information
        self.received = {}  #dict of received information

    def new_message(self, message):
        self.received[message.ID] = []
        self.received[message.ID].append(message.senderID)
        self.queue.append(message)
        self.items += 1
        self.sort_queue(message) #sort message in the queue
        
    def sort_queue(self, message):
        if self.items == 0:
            return -1
        else:
            idx = -1
            for i in range(self.items):
                if message.ID == self.queue[i].ID:
                    idx = i
            if idx == -1:
                print("message not in queue")
                return -1
            #update
            self.queue[idx].deliverable = message.deliverable   #update deliverable
            message_time, message_node = message.sequence.split(".")
            store_time, stored_node = self.queue[idx].sequence.split(".")
            if (message_time > store_time) | ((message_time == store_time) & (message_node>stored_node)):
                self.queue[idx].sequence = message.sequence
            #sort
            for i in range(self.items):
                for j in range(self.items-i-1):
                    j_time, j_node = self.queue[j].sequence.split(".")
                    j1_time, j1_node = self.queue[j+1].sequence.split(".")
                    if (j1_time < j_time) | ((j1_time == j_time) & (j1_node<j_node)):
                        temp = self.queue[j]
                        self.queue[j] = self.queue[j+1]
                        self.queue[j+1] = temp

    def remove_message(self, message):
        if (message.ID not in self.received.keys()) | (self.items == 0):
            return -1
        else:
            idx = -1
            for i in range(self.items):
                if message.ID == self.queue[i].ID:
                    idx = i
            if idx == -1:
                print("message not in queue")
                return -1
            del self.queue[idx]
            del self.received[message.ID]
            self.items -= 1
            return 0
    
    def remove_crash(self, nodeID):
        idx = nodeID[-1]
        crashed_list = []
        if self.items == 0:
            return -1
        for i in range(self.items):
            temp = self.queue[i].ID.split(".")[1]
            if idx == temp:
                crashed_list.append(i)
        crashed_list.reverse()
        for i in crashed_list:
            del self.received[self.queue[i].ID]
            del self.queue[i]
            self.items -= 1
        return 0

    def update_received(self, message):
        if message.ID not in self.received.keys():
            return -1
        else:
            self.received[message.ID].append(message.senderID)
            return 0

    def update_balance(self, message):
        action = message.transaction.split()[0]
        if action == "DEPOSIT":
            account = message.transaction.split()[1]
            money = message.transaction.split()[2]
            if account not in self.accounts.keys():
                self.accounts[account] = 0
            self.accounts[account] += int(money)
        elif action == "TRANSFER":
            sender = message.transaction.split()[1]
            receiver = message.transaction.split()[3]
            money = message.transaction.split()[4]
            if sender not in self.accounts.keys(): 
                print("no sender exists")
                return -1
            elif (self.accounts[sender] < int(money)):
                print("not enough money")
                return -1
            else:
                if receiver not in self.accounts.keys():
                    self.accounts[receiver] = 0
                self.accounts[sender] -= int(money)
                self.accounts[receiver] += int(money)
        else:
            print("invalid action")
            return -1
        
        log = "BALANCES "
        for account in self.accounts.keys():
            log = log + str(account) + ":" + str(self.accounts[account]) + " "
        
        print(log)
    

def deliver_message(message):
    global p
    global nodes_connected
    global local_node
    global times
    global socketDict
    global sleep_time
    global head_Lock
    global sort_Lock
    global times_lock
    status = p.update_received(message)
    if status == -1:
        #add new message 
        p.new_message(message)
        #print("add new message")
        #if local, b-multicast to other
        if message.senderID == local_node:
            #print('local event')
            multicast(message)
            #print('finish local event')
            time.sleep(sleep_time)
            # pass

        #if received from others 
        else:
            #proposed a new sequence number
            times_lock.acquire()
            times += 1
            times_lock.release()
            message.sequence = str(times)+"."+local_node
            reply_node = message.senderID
            message.senderID = local_node
            sort_Lock.acquire()
            p.sort_queue(message)
            sort_Lock.release()
            #print('reply proposed message')
            unicast(reply_node, message)
            time.sleep(sleep_time)
    else:
        #print('update message')
        p.received[message.ID].append(message.senderID)
        sort_Lock.acquire()
        p.sort_queue(message)
        sort_Lock.release()
        if message.senderID != message.ID.split(".")[1]:   #receive proposed sequence
            n_status = -1
            for i in socketDict.keys():      #if all proposed has been received
                ii = i[-1]
                if ii not in p.received[message.ID]:
                    n_status = 0
                    break
            if n_status == -1:  #all proposed have been received 
                idx = -1
                for i in range(p.items):
                    if message.ID == p.queue[i].ID:
                        idx = i
                if idx == -1:
                    #print("message not in queue")
                    return -1
                final_message = p.queue[idx]
                final_message.deliverable = 1
                final_message.senderID = message.ID.split(".")[1]
                sort_Lock.acquire()
                p.sort_queue(final_message)
                sort_Lock.release()
                #print('received all reply')
                multicast(final_message)
                #print("final message: ",final_message.ID)
        #else: receive final agreement do nothing?
        #all need push queue head
        #time.sleep(sleep_time)
        head_Lock.acquire()
        queue_head()
        head_Lock.release()
        time.sleep(sleep_time)
    
                
def queue_head():
    global p
    global socketDict
    while True:
        if p.items == 0:
            break
        head_message = p.queue[0]

        n_status = -1
        for i in socketDict.keys():      #if all proposed has been received
            ii = i[-1]
            if ii not in p.received[head_message.ID]:
                n_status = 0
                break
        if n_status == -1:  #all proposed have been received 
            final_message = p.queue[0]
            final_message.deliverable = 1
            final_message.senderID = final_message.ID.split(".")[1]
            if p.queue[0].deliverable == 0:
                multicast(final_message)
            sort_Lock.acquire()
            p.sort_queue(final_message)
            sort_Lock.release()
            #print('received all reply')
            head_message = p.queue[0]
            

        if head_message.deliverable == 1:
            #print('update balance')
            #print('ID:', head_message.ID)
            #print('sequence:', head_message.sequence)
            #print('total item', p.items)
            p.update_balance(head_message)
            p.remove_message(head_message)
        else:        
            break

def receive(localSocket):
    global sleep_time
    while True:
        new_socket, _ = localSocket.accept()
        Thread(target=recvNewSocket, args=(new_socket,)).start()
        time.sleep(sleep_time)

def recvNewSocket(new_socket):
    global times
    global sleep_time
    global num_bytes_recv
    global num_bytes_recv_lock
    #global msg_tracker
    while True:
        raw_data = new_socket.recv(1024).decode('utf-8')
        #print("handle_receive while")
        if len(raw_data) == 0:
            continue
        else:
            num_bytes_recv_lock.acquire()
            num_bytes_recv += len(raw_data)
            num_bytes_recv_lock.release()
            while True:
                if raw_data[-1] != '}':  # } is the last char of the message
                    raw_data += new_socket.recv(1024).decode("utf-8")
                else:
                    break
        data_list = raw_data.split("}")
        #print("handle_receive data")
        for data in data_list:
            if len(data) <= 1:
                break
            data = data + "}"
            raw_dict = json.loads(data)

            sender = raw_dict["senderID"]
            content = raw_dict["transaction"]
            ID = raw_dict["ID"]
            sequence = raw_dict["sequence"]
            deliverable = raw_dict["deliverable"]
            cur_message = message(ID=ID, senderID=sender, transaction=content, sequence=sequence, deliverable=deliverable)

            #print("deliver_message")
            deliver_message(cur_message)



def recvMessage():
    global times
    global localIP
    global receivedRepo
    global sleep_time
    global times_lock
    while True:
        #print("try-1")
        for content in sys.stdin:
            #print("1")
            #print("content:" + content)
            #pdb.set_trace()
            if len(content) == 0:
                print("continue")
                continue
            #print('try-2',content)
            # init the message struct
            content.replace('\n', '')
            times_lock.acquire()
            #received_repo_lock.acquire()
            times += 1
            times_lock.release()
            ID = str(times) + '.' + local_node  # form: 'node1+1'
            #print("ID: {}".format(ID))
            #times += 1
            msg = message(ID=ID, senderID=local_node, transaction=content, sequence=ID, deliverable=0)
            #print("msg: {}".format(msg))
            receivedRepo.append((ID, msg.sequence.split('.')[0], msg.sequence.split('.')[1], msg.senderID))
            #receivedRepo_Lock.release()
            
            #print("enter deliver")
            deliver_message(msg)
            #time.sleep(sleep_time)


def multicast(msg):
    global connectedNodeNum
    global socketDict
    global bytesNum
    global p
    global sort_Lock 
    global socketDict_Lock
    crashedNodeList = []   
    json_message = {}
    json_message['ID'] = msg.ID
    json_message['senderID'] = msg.senderID
    json_message['transaction'] = msg.transaction
    json_message['sequence'] = msg.sequence
    json_message['deliverable'] = msg.deliverable
    #print("create json")
    json_message1 = json.dumps(json_message).encode('utf-8')
    #print("dump json")
    #print(socketDict.keys())
    for node_i, socket_i in socketDict.items():
        #print("node_i, socket_i: {} {}".format(node_i, socket_i))
        success = 0
        #print("hello hello")
        for _ in range(10):     # send 10 times
            #print("try {} times".format(_))
            try:
                num = socket_i.send(json_message1)
                bytesNum_Lock.acquire()
                bytesNum += num
                bytesNum_Lock.release()
                success = 1  
                break
            except:  
                continue
        #print("success", success)
        if not success:
            #print("not success")
            socket_i.close()
            crashedNodeList.append(node_i)
            connectedNodeNum -= 1
    
    for nodeName in crashedNodeList:
        del socketDict[nodeName]
        #print('run into error')
        #print('item', p.items)
        #print('dict', socketDict.keys())
        #sort_Lock.acquire() 
        p.remove_crash(nodeName)
        #sort_Lock.release()
        #print("finish error")

    #print("before release")
    #socketDict_Lock.release()
    #connectedNodeNum_Lock.release()

def unicast(reply_node, message):
    global socketDict
    #print(socketDict.keys())
    reply_node = "node"+reply_node
    if reply_node not in socketDict.keys():
        return -1
    cur_socket = socketDict[reply_node]
    json_message = {}
    json_message['ID'] = message.ID
    json_message['senderID'] = message.senderID
    json_message['transaction'] = message.transaction
    json_message['sequence'] = message.sequence
    json_message['deliverable'] = message.deliverable
    json_message1 = json.dumps(json_message).encode('utf-8')
    cur_socket.send(json_message1)

def buildConnect(fileName):
    global connectedNodeNum
    global sleep_time
    nodes = decodeConfig(fileName)
    for i in range(len(nodes)):
        curThread = Thread(target=newSingleConnect, args=(nodes[i][0], nodes[i][1], nodes[i][2]))
        curThread.start()
    time.sleep(sleep_time)
    while True:
        if connectedNodeNum == len(nodes):
            return True

def decodeConfig(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()
        nodes = []
        for i in range(1, len(lines)):
            line = lines[i].split(' ')
            line[-1] = int(line[-1])
            nodes.append(line)
    return nodes


def newSingleConnect(local_node, nodeIP, portNum):
    global connectedNodeNum
    global socketDict
    global connectedNodeNum_Lock
    global socketDict_Lock
    while True:
        curSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if 0 == curSocket.connect_ex((nodeIP, portNum)):
            connectedNodeNum_Lock.acquire()
            connectedNodeNum += 1
            connectedNodeNum_Lock.release()
            socketDict_Lock.acquire()
            socketDict[local_node] = curSocket
            socketDict_Lock.release()
            return True
        else:
            continue

def setSocket(IP, port):
    localSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    localSocket.bind((IP, port))
    localSocket.listen(10)
    return localSocket


def main():
    global localIP
    global local_node
    global p 
    global sleep_time

    p = Process()
    if len(sys.argv) != 4:
        print("invalid input")
        return False

    localIP = "127.0.0.1"
    local_node = sys.argv[1]
    local_node = local_node[-1]
    portNum = int(sys.argv[2])
    fileName = sys.argv[3]
    socketMain = setSocket(localIP, portNum)
    buildConnect(fileName)  # build connect with all other nodes in config file
    time.sleep(sleep_time)
    Thread(target=receive, args=(socketMain,)).start()

    while True:
        try:
            recvMessage()
        except:
            continue

if __name__ == '__main__':
    main()



    
