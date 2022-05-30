import subprocess
import sys
import time
import socket
import json

write_buffer = []

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

	input = input()
	#example input: "SELECT vendor_id, vendor_name FROM vendors ORDER BY vendor_name;1"
	#transaction number after SQL statement
	while input != "exit":
		#execute reads immediately
		if input.startswith("SELECT"):
			send_msg(server_ports[0], input[:-2])
		#buffer writes
		elif input.startswith("UPDATE") or input.startswith("INSERT"):
			write_buffer.append(input[:-2])
		#commit when commit message comes
		elif input.startswith("COMMIT"):
			send_msg(server_ports[0], write_buffer)
		input = input()

	exit()