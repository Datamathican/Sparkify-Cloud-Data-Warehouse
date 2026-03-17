import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all tables defined in sql_queries.py to reset the database."""
    print("Dropping tables...")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("All tables dropped successfully.") 
    
    
def create_tables(cur, conn):
    """Creates all staging, fact, and dimension tables defined in sql_queries.py."""
    print("Creating tables...")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("All tables created successfully.")

def main():
    """
    Reads config, connects to Redshift, then calls functions to drop and create tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    
    try:
       
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            config.get('CLUSTER', 'HOST'),
            config.get('CLUSTER', 'DB_NAME'),
            config.get('CLUSTER', 'DB_USER'),
            config.get('CLUSTER', 'DB_PASSWORD'),
            config.get('CLUSTER', 'DB_PORT')
        ))
        cur = conn.cursor()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
        print("\nDatabase Schema setup completed. Run etl.py next.")
    
    except Exception as e:
      
        print(f"Error connecting to Redshift or executing SQL: {e}") 
        print("Please check your dwh.cfg file and ensure your Redshift cluster is running.")
        

if __name__ == "__main__":
    main()