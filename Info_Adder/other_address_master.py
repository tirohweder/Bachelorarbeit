from neo4j import GraphDatabase
import numpy as np
import psycopg2
from collections import Counter

def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="hallo123",
                               host="localhost",
                               port="8877",
                               database="trohwede")
        cur = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        biggest_2_add(cur, con)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            con.close()
            print("PostgreSQL connection is closed")

class Neo4jConnection:

    def __init__(self, uri, user ,pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None

        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user,self.__pwd))
        except Exception as e:
            print ("Failed to create the driver: ",e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None

        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response= list (session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
                if session is not None:
                        session.close()
        return response


def biggest_2_add(cur, con):
    conn = Neo4jConnection(uri='bolt://localhost:7687',user= 'trohwede', pwd='1687885@uma')
    list_of_all_addr = []

    statement = "SELECT address FROM potential_deposit_address WHERE real_out_deg - real_conn_with_host != 0 AND " \
                "real_out_deg - real_conn_with_host< 20"

    cur.execute(statement)
    con.commit()


    for i in cur:
        #nimmt addresse und guckt welche transactions zu der wallet führen
        query2= '''
        MATCH (a:Address)-[r:SENDS]->(tr:Transaction)-[:RECEIVES]->(a2:Address)
        WHERE a.address='{}'
        RETURN a2.address AS address2
        '''.format(i[0])

        result2 = conn.query(query2)

        for address2 in result2:
            list_of_all_addr.append(address2[0])


    c= Counter(list_of_all_addr)

    for letter, count in c.most_common(20):
        print(letter, count)


main()