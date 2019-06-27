from time import sleep
from json import dumps
from kafka import KafkaProducer

class Producer:
	# Initializer / Instance Attributes
	def __init__(self, hostname, port):
		self.hostname = hostname
		self.port = port
		self.producer = None

	def connection_kafka_producer(self):
	'''
	Establish connection to a particular Kafka producer
	hostname: str
	port:str
	'''
		host = self.hostname + ":" + self.port
		_producer = None
		try:
			_producer = KafkaProducer(bootstrap_servers=host, api_version(0,10))
		except Exception as ex:
			print('Exception while connection Kafka')
			print(str(ex))
		finally:
			self.producer =  _producer
			print('connection success')

	def publish_meesage(self, topic_name, key, value):
		try:
			key_bytes = bytes(key, encoding='utf-8')
			value_bytes = bytes(value, encoding='utf-8')
			self.producer.send(topic_name,key=key_bytes,value_bytes)
			self.producer.flush()
			print('Message published successfully')
		except Exception as es:
			print('Exception in publishing message')
			print(str(ex))







