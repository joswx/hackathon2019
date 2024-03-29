import psycopg2
from config import config

class DBconnection:
	def __init__(self):
		self.params = config()
		self.conn = None
		self.cur = None

	def connect(self):
		if (not self.conn):
			try:
				self.conn = psycopg2.connect(**self.params)
				self.cur = self.conn.cursor()
				print("DB connection established")
			except Exception as ex:
				print(str(ex))
		else:
			print("connection alr established")

	def disconnect(self):
		if (self.conn):
			try:
				self.cur.close()
				print("Connection is now closed")
			except (Exception, psycopg2.DatabaseError) as error:
				print(error)
		else:
			print("Connection not established")

	def select_pending_entries(self):
		""" select pending rows in table in the PostgreSQL database"""
		command = (
			"""
			SELECT * FROM Topic_Info WHERE state = 'pending'
			""")
		if (self.conn):
			try:
				self.cur.execute(command)
				records = self.cur.fetchall()
				print("Selecting rows from mobile table using cursor.fetchall")
				return records
			except Exception as ex:
				print(str(ex))
			finally:
				self.disconnect()
		else:
			print("connection not established")


	def update_pending_entry_to_success(self,id):
		""" select pending rows in table to update its status to created """
		command = (
			"""
			UPDATE Topic_Info set state = %s where id = %s
			""")
		if (self.conn):
			try:
				self.cur.execute(command,('created',id))
				self.conn.commit()
				print("Record Updated successfully ")
			except Exception as ex:
				print (str(ex))
		else:
			print("Connection not established")


	def extract_zookeeper_details_by_id(self,id):
		""" select zookeeper connection info based on id from CONNECTION_DETAILS table"""
		command = (
			"""
			SELECT * FROM CONNECTION_DETAILS WHERE id =  %s
			""")		
		if (self.conn):
			try:
				self.cur.execute(command,(id))
				record = self.cur.fetchall()
				return record[3]
			except Exception as ex:
				print (str(ex))
		else:
			print("Connection not established")

	def record_processing(self):
		out ={}
		if (self.conn):
			records = self.select_pending_entries()
			while records:
				record = records.pop()
				id, topic_name, replicas, partition = record[0], record[1], record[2], record[3]
				zoo_conn = self.extract_zookeeper_details_by_id(id)
				out[id] = [topic_name,replicas, partition, zoo_conn]
				self.update_pending_entry_to_success(id)
			return out
		else:
			print("connection not established")













