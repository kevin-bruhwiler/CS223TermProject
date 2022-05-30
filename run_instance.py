import psycopg2
import sys
import socket
from _thread import *
import threading
import json

print_lock = threading.Lock()

neighbors = []

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

		else:
			cur.execute(data)

	c.close()
	
	
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
