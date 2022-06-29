from neo4j import GraphDatabase
import numpy as np
import psycopg2

def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="hallo123",
                               host="localhost",
                               port="5432",
                               database="trohwede")
        cur = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        find_transactions(cur, con)

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



def find_transactions(cur, con):
    conn = Neo4jConnection(uri='bolt://localhost:7687',user= 'trohwede', pwd='1687885@uma')
    list_of_all_addr = []

    #nimmt addresse und guckt welche transactions zu der wallet führen
    query2= '''
    MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
    WHERE tr.address='1EEqRvnS7XqMoXDcaGL7bLS3hzZi1qUZm1'
    RETURN t.txid AS t_txid
    '''

    result2 = conn.query(query2)

    for incoming_transactions in result2:
       #here i get all address that are part of a transaktion
        query3 = '''
        MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
        WHERE tr.txid='{0}'
        RETURN a.address AS address
        '''.format(incoming_transactions["t_txid"])

        result3= conn.query(query3)

        list_of_all_addr.extend(x["address"] for x in result3)
    #print("got here ")
    temp= np.asarray(list_of_all_addr)
    print(temp.size)
    list_of_all_addr_uniq= np.unique(temp)
    print(list_of_all_addr_uniq.size)

    for x in list_of_all_addr_uniq:
        statement2= "INSERT INTO unique_address(address) " \
                "VALUES ("+ "'"+x+ "'"")"
        cur.execute(statement2)
        con.commit()

    for x in list_of_all_addr_uniq:
        #This final query gives the transaction id of the
        query4 = '''
        MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
        MATCH (t)-[b:BELONGS_TO]->(bl:Block)
        WHERE tr.address='{0}'
        RETURN t.txid AS txid, bl.hash AS hash, bl.mediantime AS time, tr.address AS address
        '''.format(x)

        result4 = conn.query(query4)

        #print(query4)

        for xx in result4:
            #CHANGE TO ADDRESS THAT WE WANT
            if(xx["address"]!='1EEqRvnS7XqMoXDcaGL7bLS3hzZi1qUZm1'):
                #temp = (xx["txid"][1:-1], xx["address"], xx["hash"], xx["time"])
                statement = "INSERT INTO incoming_transactions(txid, block_hash, time, inc_address) " \
                            "VALUES (" + "'"+xx["txid"] +"'"+ "," + "'"+xx["hash"]+"'"+ "," +"'"+str(xx[
                            "time"]).replace("T", " ")[:19]+"'" +"," + "'"+xx["address"]+"'"+ ")"

                cur.execute(statement)
                con.commit()


main()