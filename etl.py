import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads raw data from S3 into staging tables using COPY command."""
    print("Loading data into staging tables...")
    for i, query in enumerate(copy_table_queries):
        cur.execute(query)
        conn.commit()
        print(f" - staging table {i+1} loaded.")


def insert_tables(cur, conn):
    """Transform data from staging tables into the final star schema table."""
    print("Inserting data into final tables...")
    for i, query in enumerate(insert_table_queries):
        cur.execute(query)
       
        conn.commit() 
        print(f" - final table {i+1} loaded.")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            config.get('CLUSTER','HOST'),
            config.get('CLUSTER','DB_NAME'),
            config.get('CLUSTER','DB_USER'),
            config.get('CLUSTER','DB_PASSWORD'),
            config.get('CLUSTER','DB_PORT')
        ))
        cur = conn.cursor()
        
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        
        conn.close()
      
        print("ETL pipeline completed successfully")
    
    except Exception as e:
        
        print(f"Error connecting to Redshift or executing ETL: {e}") 
        
        
if __name__ == "__main__":
    main()