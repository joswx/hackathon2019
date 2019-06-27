from DBconnection import DBconnection
from AdminClient import AdminClient
from time import sleep

if __name__ == "__main__":
	try:
		while True:
			db = DBconnection()
			db.connect()
			topics = db.record_processing()
			db.disconnect()
			for key,item in topics:
				client = AdminClient(item)
				client.create_kafka_topic()
			sleep(15)
	except KeyboardInterrupt:
		print('Interrupted, terminating the process')

