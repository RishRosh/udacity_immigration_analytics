import configparser
import psycopg2
import create_tables2
from sql_queries2 import copy_table_queries, insert_table_queries, count_table_queries, visa_misclassification_queries


def load_staging_tables(cur, conn):
    """ Function to load staging tables from a list. """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Function to load fact and dimension 
    tables from a list. Based on staging tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def count_tables(cur, conn):
    """ Function to count tables and 
    raise error if zeor"""
    for query in count_table_queries:
        cur.execute(query)
        result = cur.fetchall()[0][0]
        if result == 0:
            err_msg = "Query {} returned 0".format(query)
            raise Exception(err_msg)
        conn.commit()
        
def check_counts_visa(cur, conn):
    """ Function to count misclassification
    of visas in categories"""
    query1 = visa_misclassification_queries[0]
    cur.execute(query1)
    result1 = cur.fetchall()[0][0]
    query2 = visa_misclassification_queries[1]
    cur.execute(query2)
    result2 = cur.fetchall()[0][0]
    if result1 != result2:
        err_msg = "Visas have been mis-classified! Total visas = {}, Distinct visas={}".format(result1, result2)
        raise Exception(err_msg)
    conn.commit()        


def main():
    # Create tables
    create_tables2.main()
    
    config = configparser.ConfigParser()
    config.read('dwh2.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load staging tables from S3
    load_staging_tables(cur, conn)
    
    # Insert from staging tables
    insert_tables(cur, conn)
    
    # Data quality check #1 - counts
    count_tables(cur, conn)

    # Data quality check #2 - visa misclassification
    check_counts_visa(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()