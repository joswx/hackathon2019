from DBconnection import DBconnection

if __name__ == '__main__':
	db = DBconnection()
	db.connect()
	output = db.record_processing()
	print(output)
	db.disconnect()

