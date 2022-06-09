import psycopg2
import sys
import socket
import time
from _thread import *
import threading
from collections import defaultdict
import json

print_lock = threading.Lock()

global neighbors
neighbors = []

global dead
dead = False

my_port = None

cur = None

global conn
conn = None

client = 55555

global writes
global transactions
writes = {}
transactions = defaultdict(list)

global readonlies
readonlies = set()

global snapshot
snapshot = defaultdict(list)

def connect_to_server(db_port, password):
	global conn
	conn = psycopg2.connect(database="postgres", user="postgres", password=password, host="127.0.0.1", port=db_port)
	print("Database Connected....")
	return conn.cursor()

def listen(c):
	global leader
	global neighbors
	global dead
	global conn
	global writes
	global transactions
	global readonlies
	global snapshot
	while True:
		data = c.recv(1024)
		if not data:
			print_lock.release()
			break
			
		data = data.decode("utf-8")
		
		if data.startswith("connect to:"):
			neighbors = json.loads(data.split(":")[-1])
			
			for neighbor in neighbors:
				if neighbor[1] == "L":
					start_new_thread(heartbeat, (int(neighbor[0]),))
			print(neighbors)

		elif data.startswith("COMMIT"):
			transaction_id = data[-3:]
			
			readonlies.discard(transaction_id)
			
			for data_item, timestamp in transactions[transaction_id]:
				if data_item in writes and writes[data_item] > timestamp:
					send_msg(client, "ABORT")
					break
			
			conn.commit()
			if leader:
				for neighbor in neighbors:
					send_msg(neighbor[0], data)
			send_msg(client, "COMMITTED")
			
		elif data.startswith("HEARTBEATCHECK"):
			other_port = int(data.split(":")[-1])
			send_msg(other_port, "DEAD" if dead else "ALIVE")
			
		elif data.startswith("DIE"):
			dead = True
			
		elif data.startswith("READONLY"):
			readonlies.add(data.split(";")[-1])
			
		elif data.startswith("DEAD"):
			for neighbor in neighbors:
				if neighbor[1] == "L":
					neighbor[1] = "W"
				else:
					if int(neighbor[0]) < my_port:
						neighbor[1] = "L"
					else:
						leader = True
			print(neighbors)
			
		elif data.startswith("ALIVE"):
			pass
			
		elif data.startswith("LOGS"):
			logs = json.loads(data[5:])
			transactions = logs[0]
			writes = logs[1]
			
			
		elif leader is True:
			transaction_id = data[-3:]
			
			data_item = data.split(" ")[1]
			curr_time = time.time()
			transactions[transaction_id].append((data_item, curr_time))
			
			for neighbor in neighbors:
				send_msg(neighbor[0], data)
			cur.execute(data[:-4])
			
			if data.startswith("SELECT") and transaction_id not in readonlies:
				row = cur.fetchone()
				while row is not None:
					send_msg(client, str(row))
					snapshot[data_item].append(row)
					row = cur.fetchone()
					
			elif data.startswith("SELECT") and transaction_id in readonlies and data_item in snapshot:
				for row in snapshot[data_item]
					send_msg(client, str(row))
			else:
				writes[data_item] = curr_time
				
			for neighbor in neighbors:
				send_msg(neighbor[0], "LOGS " + json.dumps((transactions, writes)))
				
			if len(readonlies) == 0:
				snapshot.clear()
		else:
			cur.execute(data[:-4])

	c.close()
	

def heartbeat(port):
	global leader
	while True:
		time.sleep(5)
		if leader:
			break
		send_msg(port, "HEARTBEATCHECK:" + str(my_port))
		
	
	
def send_msg(port, msg):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(("localhost", int(port)))
	sock.sendall(bytes(msg, encoding='utf8'))
	sock.close()


if __name__ == "__main__":
	global leader
	cur = connect_to_server(sys.argv[1], sys.argv[3])
	
	leader = sys.argv[4] == "L"
	print("Is leader:", leader)
	port = int(sys.argv[2])
	my_port = port
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	host = "" #socket.gethostname()
	s.bind((host, port))
	print("socket binded to port", port)
	s.listen(4)
	print("socket is listening")
	
	while True:
		c, addr = s.accept()
		print_lock.acquire()
		#print('Connected to :', addr[0], ':', addr[1])
 
		start_new_thread(listen, (c,))
	s.close()
