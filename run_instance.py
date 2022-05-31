import psycopg2
import sys
import socket
import time
from _thread import *
import threading
import json

print_lock = threading.Lock()

neighbors = []

dead = False

leader = False

my_port = None

cur = None

conn = None

def connect_to_server(db_port, password):
	conn = psycopg2.connect(database="postgres", user="postgres", password=password, host="127.0.0.1", port="5432")
	print("Database Connected....")
	return conn.cursor()

def listen(c):
	while True:
		data = c.recv(1024)
		if not data:
			print('Bye')
			print_lock.release()
			break
			
		data = data.decode("utf-8")
		
		if data.startswith("connect to:"):
			neighbors = json.loads(data.split(":")[-1])
			print(neighbors)

		elif leader is True:
			for neighbor in neighbors:
				send_msg(neighbor, data)
			cur.execute(data)

		elif data.startswith("COMMIT"):
			conn.commit()
			
		elif data.startswith("HEARTBEATCHECK"):
			other_port = data.split(":")[-1]
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

		else:
			cur.execute(data)

	c.close()
	

def heartbeat(port):
	while True:
		time.sleep(5)
		if any([l[1] == "L" for l in neighbors]):
			break
		send_msg(port, "HEARTBEATCHECK:" + str(my_port))
		
	
	
def send_msg(port, msg):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(("localhost", port))
	sock.sendall(bytes(msg, encoding='utf8'))
	sock.close()


if __name__ == "__main__":
	cur = connect_to_server(sys.argv[1], sys.argv[3])
	
	leader = sys.argv[4] == "L"
	print("Is leader:", leader)
	host = ""
	port = int(sys.argv[2])
	my_port = port
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((host, port))
	print("socket binded to port", port)
	s.listen(4)
	print("socket is listening")
	
	while True:
		c, addr = s.accept()
		print_lock.acquire()
		print('Connected to :', addr[0], ':', addr[1])
 
		start_new_thread(listen, (c,))
	s.close()
