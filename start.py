import subprocess
import sys
import time
import socket
import json
from _thread import *
import threading

print_lock = threading.Lock()

write_buffer = {}
my_port = 55555

def listen():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	host = ''
	s.bind((host, my_port))
	s.listen(4)
	
	while True:
		c, addr = s.accept()
		print_lock.acquire()
		
		while True:
			data = c.recv(1024)
			if not data:
				print_lock.release()
				break

			data = data.decode("utf-8")
			if not data.startswith("COMMITTED"):
				print(data)


def start_servers(db_port, server_port, password, leader):
	subprocess.Popen(["python", "run_instance.py", db_port, server_port, password, leader])
	

def send_msg(port, msg):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(("localhost", int(port)))
	sock.sendall(bytes(msg, encoding='utf8'))
	sock.close()
	
	
if __name__ == "__main__":
	db_ports = ["5432", "5433", "5434"]
	server_ports = ["12345", "12346",  "12347"]
	leaders = ["L", "W", "W"]
	password = sys.argv[1]
	for i, (db_port, server_port, leader) in enumerate(zip(db_ports, server_ports, leaders)):
		start_servers(db_port, server_port, password, leader)
		time.sleep(2)
		print(server_port)
		print("connect to:" + json.dumps(list(zip(server_ports[:i] + server_ports[i+1:], leaders[:i] + leaders[i+1:]))))

		send_msg(server_port, "connect to:" + json.dumps(list(zip(server_ports[:i] + server_ports[i+1:], leaders[:i] + leaders[i+1:]))))

	uinput = input()
	
	start_new_thread(listen, ())
	
	#example input: "SELECT vendor_id, vendor_name FROM vendors ORDER BY vendor_name;048"
	#transaction number after SQL statement
	while uinput != "exit":

		#execute reads immediately
		if uinput.startswith("SELECT") or uinput.startswith("CREATE"):
			transaction_id = uinput[-3:]
			send_msg(server_ports[0], uinput)
		#buffer writes
		elif uinput.startswith("UPDATE") or uinput.startswith("INSERT"):
			transaction_id = uinput[-3:]
			if transaction_id in write_buffer:
				write_buffer[transaction_id].append(uinput)
			else:
				write_buffer[transaction_id] = list([uinput])
		#commit when commit message comes
		elif uinput.startswith("COMMIT"):
			transaction_id = uinput[-3:]
			statements = write_buffer[transaction_id]
			for statement in statements:
				send_msg(server_ports[0], statement)
			send_msg(server_ports[0], uinput)
		
		elif uinput.startswith("KILL_LEADER"):
			send_msg(server_ports[0], "DIE")
			server_ports.pop(0)
			
		uinput = input()

	exit()
