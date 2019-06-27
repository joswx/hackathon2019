import psycopg2
from config import config
 
 
def create_tables():
    """ create tables in the PostgreSQL database"""
    create_table_command = (
        """
        CREATE TABLE Topic_Info (
            id SERIAL PRIMARY KEY,
            topic_name VARCHAR(255) NOT NULL,
            replication_factor INTEGER NOT NULL,
            partition INTEGER NOT NULL,
            state VARCHAR(255) NOT NULL,
            mail_id INTEGER NOT NULL,
            Description VARCHAR(255) NOT NULL

        )"""
    )
    insert_table_command = (
        """
        INSERT INTO Topic_Info (topic_name, replication_factor, partition,
        state, mail_id, Description)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
    )
    print(insert_table_command)
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        cur.execute(insert_table_command,
        ('test1', 1 , 1,'pending',123,'Empty'))
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
 
if __name__ == '__main__':
    create_tables()