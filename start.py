import subprocess
import sys
import time
import socket
import json

write_buffer = {}

def start_servers(db_port, server_port, password, leader):
	subprocess.Popen(["python", "run_instance.py", db_port, server_port, password, leader])
	

def send_msg(port, msg):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect(("localhost", port))
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
		send_msg(int(server_port), "connect to:" + json.dumps(list(zip(server_ports[:i] + server_ports[i+1:], leaders[:i] + leaders[i+1:]))))

	uinput = input()
	#example input: "SELECT vendor_id, vendor_name FROM vendors ORDER BY vendor_name;048"
	#transaction number after SQL statement
	while uinput != "exit":
		#execute reads immediately
		if uinput.startswith("SELECT"):
			send_msg(server_ports[0], uinput[:-4])
		#buffer writes
		elif uinput.startswith("UPDATE") or uinput.startswith("INSERT"):
			transaction_id = uinput[-3:]
			if transaction_id in write_buffer:
				write_buffer[transaction_id].append(uinput[:-4])
			else:
				write_buffer[transaction_id] = list([uinput[:-4]])
		#commit when commit message comes
		elif uinput.startswith("COMMIT"):
			transaction_id = uinput[-3:]
			statements = write_buffer[transaction_id]
			for statement in statements:
				send_msg(server_ports[0], statement)
			send_msg(server_ports[0], "COMMIT")
		
		elif uinput.startswith("KILL_LEADER"):
			send_msg(int(server_ports[0]), "DIE")
			server_ports.pop(0)
			
			
		uinput = input()

	exit()
