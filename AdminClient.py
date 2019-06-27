from kafka.admin import KafkaAdminClient, NewTopic

class AdminClient:
	def __init__(self,record):
		'''
		record: not null, a list of input needed to create kafka topic
		'''
		self.topic_name = record[0]
		self.replication_factor = record[1]
		self.partition = record[2]
		self.zoo_conn = record[3]

		self.client = KafkaAdminClient(bootstrap_servers=zoo_conn)

	def create_kafka_topic(self):
		try:
			self.client.creat_topics(NewTopic(name=self.topic_name, num_partitions=self.partition, replication_factor=self.replication_factor),validate_only=False)
		except Exception as ex:
			print("Error creating Kafka Topic \n")
			print(str(ex))

	def delete_kafka_topic(self,topic_name):
		try:
			self.client.delete_topics(NewTopic(name=topic_name))
			print(topic_name + ' is successfully deleted')
		except Exception as ex:
			print('error deleting topic')
			print(str(ex))




