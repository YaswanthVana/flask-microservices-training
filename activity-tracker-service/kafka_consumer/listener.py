from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster


def update_to_cassandra_db(data):
    cluster = Cluster(['localhost'],port=9042)
	
	#Connecting to the cluster 
    session = cluster.connect()
    print("-----------------------------------------------------------------------------------------------------")
    print(f"Connected to Cassandra cluster: {session}")
    
    #Setting the session keyspace
    keyspace = 'activity_tracker_keyspace'
    session.set_keyspace(keyspace)
    print("-----------------------------------------------------------------------------------------------------")
    print(f"Setting the session to keyspace: {keyspace}")
    values = list(data.values())
    print('------------------------------------------------')
    #print(type(values))
    record = '''
			INSERT INTO activity_tracker_table (activity_id, store_location, person_detected, activity_type, occurance_timestamp)
			VALUES ({},'{}','{}','{}','{}');'''.format(values[0],values[1],values[2],values[3],values[4])
    print(record)
    session.execute(record)


    #Filtering records/Prepared statement
    print("-----------------------------------------------------------------------------------------------------")
    print("Fetching all the records from user table using activity_id")
    user_lookup_stmt = session.prepare("SELECT * FROM activity_tracker_table WHERE activity_id=? ALLOW FILTERING")
    rows = session.execute(user_lookup_stmt, [values[0]])
    for row in rows:
		#Accessing each data element from row
        print(row.activity_id,row.store_location,row.person_detected,row.activity_type,row.occurance_timestamp)	
    print("-----------------------------------------------------------------------------------------------------")
    print('Data updated to cassandra successfully')
    pass

def read(ws, topic_name):
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(topic_name,
                         group_id='demo-group',
                         bootstrap_servers=['localhost:9092'])
    
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        #print(f"topic: {message.topic}, partition: {message.partition}, offset: {message.offset}, key: {message.key}, value: {message.value}")
        ws.send({'topic': message.topic, 'partition': message.partition, 'offset': message.offset, 'key': message.key, 'value': message.value})
        #print(type(message.value))
        #print(message.value)
        data = json.loads(message.value)
        #print(data)
        #print(type(data))
        update_to_cassandra_db(data)
