from neo4j import GraphDatabase
import pandas as pd
import numpy as np

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

conn = Neo4jConnection(uri='bolt://localhost:7687',user= 'trohwede', pwd='1687885@uma')




#Nimmt txid und guckt welche adresse das resultat ist
query= '''
MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
WHERE t.txid='e8b13b0f9bdcab94be93d5f47f996ee3bafe39262cc1044b7c0859321ef525c2'
RETURN tr.address AS tr_address
'''

params= {}
result = conn.query(query)

df = pd.DataFrame([dict(_) for _ in conn.query(query)])
print (df.head(10))

for x in result:
    print (x["tr_address"])


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
    MATCH (tr)-[b:BELONGS_TO]->(bl:Block)
    WHERE tr.txid='{0}'
    RETURN tr.txid AS txid, a.address AS address,bl.hash AS hash, bl.mediantime AS time
    '''.format(incoming_transactions["t_txid"])

    result3= conn.query(query3)

    for x in result3:
        temp= (x["txid"], x["address"], x["hash"], x["time"])
        #print(temp)

        #This final query gives the transaction id of the
        query4 = '''
        MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
        MATCH (t)-[b:BELONGS_TO]->(bl:Block)
        WHERE tr.address='{0}'
        RETURN t.txid AS txid, bl.hash AS hash, bl.mediantime AS time, tr.address AS address
        '''.format(x["address"])

        result4 = conn.query(query4)

        #print(query4)

        for xx in result4:
            if(xx["address"]!='1EEqRvnS7XqMoXDcaGL7bLS3hzZi1qUZm1'):
                temp = (xx["txid"], xx["address"], xx["hash"], xx["time"])
                print (temp)






#nimmt txid und gibt alle einzahlenden adressen dazu
query4= '''
MATCH (t:Address)-[r:SENDS]->(tr:Transaction)
WHERE tr.txid='28f9585aa6abdf28202d759b78e531dbfc2f8f3b83592321c3414ce33504a84d'
RETURN t.address AS tr_address
'''

query5= '''
MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
MATCH (tr)-[b:BELONGS_TO]->(b:Block)
WHERE tr.txid='28f9585aa6abdf28202d759b78e531dbfc2f8f3b83592321c3414ce33504a84d'
RETURN t.address AS tr_address, tr.inSum, b.mediantime AS b_time
'''