import psycopg2

def connect():

    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("dbname=suppliers user=postgres password=G3n3r5lK3n0b!")

        # create a cursor
        cur = conn.cursor()

        # # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def create_tables():

    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE vendors (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE parts (
                part_id SERIAL PRIMARY KEY,
                part_name VARCHAR(255) NOT NULL
                )
        """,
        """
        CREATE TABLE part_drawings (
                part_id INTEGER PRIMARY KEY,
                file_extension VARCHAR(5) NOT NULL,
                drawing_data BYTEA NOT NULL,
                FOREIGN KEY (part_id)
                REFERENCES parts (part_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE vendor_parts (
                vendor_id INTEGER NOT NULL,
                part_id INTEGER NOT NULL,
                PRIMARY KEY (vendor_id , part_id),
                FOREIGN KEY (vendor_id)
                    REFERENCES vendors (vendor_id)
                    ON UPDATE CASCADE ON DELETE CASCADE,
                FOREIGN KEY (part_id)
                    REFERENCES parts (part_id)
                    ON UPDATE CASCADE ON DELETE CASCADE
        )
        """)

    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("dbname=suppliers user=postgres password=G3n3r5lK3n0b!")

        # create a cursor
        cur = conn.cursor()

        # execute commands
        for command in commands:
            cur.execute(command)

        # close the communication with the PostgreSQL
        cur.close()

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def insert_vendor(vendor_name):
    """ insert a new vendor into the vendors table """
    sql = """INSERT INTO vendors(vendor_name)
             VALUES(%s) RETURNING vendor_id;"""
    conn = None
    vendor_id = None
    try:
        # read database configuration
        #params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect("dbname=suppliers user=postgres password=G3n3r5lK3n0b!")
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (vendor_name,))
        # get the generated id back
        vendor_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return vendor_id

def insert_vendor_list(vendor_list):
    """ insert multiple vendors into the vendors table  """
    sql = "INSERT INTO vendors(vendor_name) VALUES(%s)"
    conn = None
    try:
        # read database configuration
        #params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect("dbname=suppliers user=postgres password=G3n3r5lK3n0b!")
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,vendor_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#!/usr/bin/python

def update_vendor(vendor_id, vendor_name):
    """ update vendor name based on the vendor id """
    sql = """ UPDATE vendors
                SET vendor_name = %s
                WHERE vendor_id = %s"""
    conn = None
    updated_rows = 0
    try:
        # read database configuration
        #params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect("dbname=suppliers user=postgres password=G3n3r5lK3n0b!")
        # create a new cursor
        cur = conn.cursor()
        # execute the UPDATE  statement
        cur.execute(sql, (vendor_name, vendor_id))
        # get the number of updated rows
        updated_rows = cur.rowcount
        # Commit the changes to the database
        conn.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return updated_rows

def add_part(part_name, vendor_list):
    # statement for inserting a new row into the parts table
    insert_part = "INSERT INTO parts(part_name) VALUES(%s) RETURNING part_id;"
    # statement for inserting a new row into the vendor_parts table
    assign_vendor = "INSERT INTO vendor_parts(vendor_id,part_id) VALUES(%s,%s)"

    conn = None
    try:
        #params = config()
        conn = psycopg2.connect("dbname=suppliers user=postgres password=G3n3r5lK3n0b!")
        cur = conn.cursor()
        # insert a new part
        cur.execute(insert_part, (part_name,))
        # get the part id
        part_id = cur.fetchone()[0]
        # assign parts provided by vendors
        for vendor_id in vendor_list:
            cur.execute(assign_vendor, (vendor_id, part_id))

        # commit changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_vendors():
    """ query data from the vendors table """
    select = "SELECT vendor_id, vendor_name FROM vendors ORDER BY vendor_name"
    conn = None
    try:
        #params = config()
        conn = psycopg2.connect("dbname=suppliers user=postgres password=G3n3r5lK3n0b!")
        cur = conn.cursor()
        cur.execute(select)
        print("The number of parts: ", cur.rowcount)
        row = cur.fetchone()

        while row is not None:
            print(row)
            row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def create_db():
    # connection establishment
    conn = psycopg2.connect(
        database="postgres",
        user='postgres',
        password='G3n3r5lK3n0b!',
        host='192.168.56.1',
        port='5433'
    )

    conn.autocommit = True

    # Creating a cursor object
    cursor = conn.cursor()

    # query to create a database
    sql = ''' CREATE database products ''';

    # executing above query
    cursor.execute(sql)
    print("Database has been created successfully !!");

    # Closing the connection
    conn.close()

if __name__ == '__main__':
    create_db()
    #connect()
    #create_tables()
    # insert one vendor
    #insert_vendor("3M Co.")
    # insert multiple vendors
    # insert_vendor_list([
    #     ('AKM Semiconductor Inc.',),
    #     ('Asahi Glass Co Ltd.',),
    #     ('Daikin Industries Ltd.',),
    #     ('Dynacast International Inc.',),
    #     ('Foster Electric Co. Ltd.',),
    #     ('Murata Manufacturing Co. Ltd.',)
    # ])
    # Update vendor id 1
    #update_vendor(1, "3M Corp")
    # add_part('SIM Tray', (1, 2))
    # add_part('Speaker', (3, 4))
    # add_part('Vibrator', (5, 6))
    # add_part('Antenna', (6, 7))
    # add_part('Home Button', (1, 5))
    # add_part('LTE Modem', (1, 5))
    #get_vendors()