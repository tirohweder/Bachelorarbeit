import numpy as np
import psycopg2
import datetime
from collections import Counter
from neo4j import GraphDatabase


def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="1687885@uma",
                               host="localhost",
                               port="5432",
                               database="trohwede")
        cur = con.cursor()
        cur2 = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        #moreInfo(cur, con, cur2)

        #connectionWithHostDoeOnlyOnce(cur, con)
        #connectionWithHost(cur, con, cur2)
        getRealOutDegree(cur,con,cur2)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            cur2.close()
            con.close()
            print("PostgreSQL connection is closed")


class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None

        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver: ", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None

        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


def moreInfo(cur, con, cur2):
    conn = Neo4jConnection(uri='bolt://localhost:7687', user='trohwede', pwd='1687885@uma')

    selection = 'SELECT address FROM unique_address ' \
                'WHERE in_degree IS NULL'

    # print(selection)
    cur.execute(selection)
    for row in cur:
        # nimmt addresse und guckt welche transactions zu der wallet führen
        query2 = '''
        MATCH (a:Address)
        WHERE a.address='{0}'
        RETURN a.inDegree AS inDegree, a.outDegree AS outDegree
        '''.format(row[0])

        result2 = conn.query(query2)
        # print(result2[0]["inDegree"],result2[0]["outDegree"])

        statement = "UPDATE unique_address " \
                    "SET in_degree = " + str(result2[0]["inDegree"]) + ", out_degree = " + \
                    str(result2[0]["outDegree"]) + \
                    " WHERE address = " + "'" + row[0] + "'"

        # print(statement)
        cur2.execute(statement)
        con.commit()


def connectionWithHostDoeOnlyOnce(cur, con):
    statement = "UPDATE unique_address " \
                "SET connections_with_host = 1" \
                " WHERE in_degree = 1"

    cur.execute(statement)
    con.commit()


def connectionWithHost(cur, con, cur2):
    conn = Neo4jConnection(uri='bolt://localhost:7687',user= 'trohwede', pwd='1687885@uma')
    list_of_all_addr = []

    #nimmt addresse und guckt welche transactions zu der wallet führen
    query= '''
    MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
    WHERE tr.address='1EEqRvnS7XqMoXDcaGL7bLS3hzZi1qUZm1'
    RETURN t.txid AS t_txid
    '''

    result = conn.query(query)

    for incoming_transactions in result:
       #here i get all address that are part of a transaktion
        query3 = '''
        MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
        WHERE tr.txid='{0}'
        RETURN a.address AS address
        '''.format(incoming_transactions["t_txid"])

        result3= conn.query(query3)

        for x in result3:
            list_of_all_addr.append(x["address"])

    counts = dict(Counter(list_of_all_addr))
    duplicates = {key:value for key, value in counts.items()}
    for keys in duplicates.keys():
        statement = "UPDATE unique_address " \
                     "SET connections_with_host= " + str(duplicates[keys]) + \
                     " WHERE address = " + "'" + keys+ "'"

        #print(statement)
        cur2.execute(statement)
        con.commit()

def getRealOutDegree(cur,con,cur2):
    conn = Neo4jConnection(uri='bolt://localhost:7687', user='trohwede', pwd='1687885@uma')


    selection = 'SELECT address FROM unique_address ' \
                'WHERE real_out_deg IS NULL'

    #print(selection)
    cur.execute(selection)
    for row in cur:

        #nimmt addresse und guckt welche transactions zu der wallet führen
        query2= '''
        MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
        WHERE a.address='{0}'
        RETURN tr.txid AS txid
        '''.format(row[0])


        result2 = conn.query(query2)

        #print(result2)
        all_trid_of_outEdge = list()

        for x in result2:
            all_trid_of_outEdge.append(x["txid"])

        temp = np.asarray(all_trid_of_outEdge)

        unique_outerEdge = np.unique(temp)
        print(len(temp), len(unique_outerEdge))

main()
