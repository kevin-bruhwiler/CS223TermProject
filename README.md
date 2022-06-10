# CS223TermProject

The code can be initialized using the start.py script and a single argument, with the command:

python start.py <postgres_user_password>

This project was developed in python 3.9, but any version of python 3 should work.

Additionally, it depends on the python psycopg2, which does not come with python installations by defualt.



Once all nodes have been initialized by start.py a command line interface is exposed, which allows the client to submit SQL command followed by a transaction ID. 
For example, “CREATE TABLE test (x int, y int);123” will initialize a transaction with ID 123 (all IDs must be a 3 digit number) and “COMMIT;123” will commit that transaction and create a new table. 
Similarly, “INSERT INTO test (x, y) VALUES (1, 2);124” will create a new transaction with ID 124 with an instruction to write a new row of values (1, 2) and “COMMIT;124” will cause the leader and all follower nodes the write that transaction (and any others with ID 124) into their databases.