from cassandra.cluster import Cluster

if __name__ == "__main__":
	cluster = Cluster(['localhost'],port=9042)
	
	#Connecting to the cluster 
	session = cluster.connect()
	print("-----------------------------------------------------------------------------------------------------")
	print(f"Connected to Cassandra cluster: {session}")

	#Creating a new keyspace
	keyspace = 'employee_management4'
	session.execute(f"create keyspace IF NOT EXISTS {keyspace} with replication={{'class': 'SimpleStrategy', 'replication_factor' : 1}};")
	print("-----------------------------------------------------------------------------------------------------")
	print(f"New keyspace: {keyspace} created successfully")


	#Setting the session keyspace
	session.set_keyspace(keyspace)
	print("-----------------------------------------------------------------------------------------------------")
	print(f"Setting the session to keyspace: {keyspace}")

	#Creating a table
	created_table_query = '''
            CREATE TABLE employee (
            email text,
            name text,
            department text,
            experience double,
            active boolean,	
            skills set<text>,
            PRIMARY KEY (email)
            );
		 '''
	session.execute(created_table_query)
	print("-----------------------------------------------------------------------------------------------------")
	print("New Table users created successfully")

	#Insert Records into a table
	record1 = '''
                INSERT INTO employee (email, name, department, experience, active, skills)
                VALUES ('yaswanth@gmail.com', 'yash', 'Devops', 5, true, {'Python', 'flask'});
			  '''
	record2 = '''
                INSERT INTO employee (email, name, department, experience, active, skills)
                VALUES ('rahul@gmail.com', 'Rahul', 'Devops', 6, true, {'Python', 'flask', 'aws'});
			  '''
	record3 = '''
                INSERT INTO employee (email, name, department, experience, active, skills)
                VALUES ('sarvesh@gmail.com', 'Sarvesh', 'Devops', 13, true, {'Python', 'flask', 'aws'});
			  '''		
	session.execute(record1)
	session.execute(record2)
	session.execute(record3)
	print("-----------------------------------------------------------------------------------------------------")
	print("Inserted new records")

	#Fetch all the records from a table
	print("-----------------------------------------------------------------------------------------------------")
	print("Fetching all the records present in [users] table")
	rows = session.execute('SELECT * FROM employee')
	for row in rows:
		#Accessing each data element from row
		print(row.email,row.name,row.department,row.experience,row.active,row.skills)
	print("-----------------------------------------------------------------------------------------------------")
	
	#Filtering records/Prepared statement
	print("-----------------------------------------------------------------------------------------------------")
	print("Fetching all the records from user table using Bhubaneswar as birth_city")
	user_lookup_stmt = session.prepare("SELECT * FROM employee WHERE email=? ALLOW FILTERING")
	rows = session.execute(user_lookup_stmt, ['yaswanth@gmail.com'])
	for row in rows:
		#Accessing each data element from row
		print(row.email,row.name,row.department,row.experience,row.active,row.skills)	
	print("-----------------------------------------------------------------------------------------------------")


	#Updating records
	print("-----------------------------------------------------------------------------------------------------")
	user_fetch_stmt = session.prepare("SELECT * from employee WHERE email=?")
	result = session.execute(user_fetch_stmt, ['rahul@gmail.com'])
	print(f"Employee before update: {result[0]}")
	print("Updatting records")
	user_update_stmt = session.prepare("UPDATE employee SET experience=? WHERE email=?")
	rows = session.execute(user_update_stmt, [9,'rahul@gmail.com'])	
	result = session.execute(user_fetch_stmt, ['rahul@gmail.com'])
	print(f"Employee after update: {result[0]}")
	print("-----------------------------------------------------------------------------------------------------")

	#Deleting record
	print("-----------------------------------------------------------------------------------------------------")
	user_fetch_stmt = session.prepare("SELECT * from employee WHERE email=?")
	result = session.execute(user_fetch_stmt, ['yaswanth@gmail.com'])
	print(f"Employee before update: {result.current_rows}")
	print("Deleting records")
	user_update_stmt = session.prepare("DELETE FROM employee WHERE  email=?")
	rows = session.execute(user_update_stmt, ['yaswanth@gmail.com'])	
	result = session.execute(user_fetch_stmt, ['yaswanth@gmail.com'])
	print(f"Employee after update: {result.current_rows}")
	print("-----------------------------------------------------------------------------------------------------")	