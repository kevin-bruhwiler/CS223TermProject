import psycopg2
import sys
import socket
import time
from _thread import *
import threading
import json

print_lock = threading.Lock()

global neighbors
neighbors = []

global dead
dead = False

my_port = None

cur = None

conn = None

def connect_to_server(db_port, password):
	conn = psycopg2.connect(database="postgres", user="postgres", password=password, host="192.168.56.1", port=db_port)
	print("Database Connected....")
	return conn.cursor()

def listen(c):
	global leader
	global neighbors
	global dead
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
			conn.commit()
			
		elif data.startswith("HEARTBEATCHECK"):
			other_port = int(data.split(":")[-1])
			send_msg(other_port, "DEAD" if dead else "ALIVE")
			
		elif data.startswith("DIE"):
			dead = True
			
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
			
		elif leader is True:
			for neighbor in neighbors:
				send_msg(neighbor[0], data)
			cur.execute(data)

		else:
			cur.execute(data)

	c.close()
	

def heartbeat(port):
	global leader
	print(type(port), port)
	while True:
		time.sleep(5)
		if leader:
			break
		send_msg(port, "HEARTBEATCHECK:" + str(my_port))
		
	
	
def send_msg(port, msg):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(("localhost", port))
	sock.sendall(bytes(msg, encoding='utf8'))
	sock.close()


if __name__ == "__main__":
	global leader
	cur = connect_to_server(sys.argv[1], sys.argv[3])
	
	leader = sys.argv[4] == "L"
	print("Is leader:", leader)
	#host = ""
	port = int(sys.argv[2])
	my_port = port
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	host = socket.gethostname()
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
