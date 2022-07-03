import json

import numpy as np
import pandas as pd
import psycopg2
import datetime
from collections import Counter


import requests
from requests.auth import HTTPBasicAuth
from neo4j import GraphDatabase
import pandas as pds

def main():

    try:
        con = psycopg2.connect(user="trohwede",
                               password="hallo123",
                               host="localhost",
                               port="8877",
                               database="trohwede")
        cur = con.cursor()
        cur2 = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        #moreInfo(cur, con, cur2)

        #connectionWithHostDoeOnlyOnce(cur, con)
        #connectionWithHost(cur, con, cur2)
        #getRealOutDegree(cur,con,cur2)
        #getRealInDegree(cur,con,cur2)
        #doOnlyOnce(cur,con)
        #getUSDValueForETHtran(cur,con,cur2)
        #originChecker(cur,con,cur2)
        densityChecker(cur,con,cur2)
        #originLabel(cur,con,cur2)

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


def inDegree_outDegree(cur, con, cur2):
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


def getRealConnectionWithHost(cur, con, cur2):
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
        temp = list()
        for x in result3:
            temp.append(x["address"])

        temp2 = np.asarray(temp)
        unique_address_in_transaction = np.unique(temp2)

        for x in unique_address_in_transaction:
            list_of_all_addr.append(x)

    counts = dict(Counter(list_of_all_addr))
    duplicates = {key:value for key, value in counts.items()}
    for keys in duplicates.keys():
        statement = "UPDATE unique_address " \
                     "SET real_conn_with_host= " + str(duplicates[keys]) + \
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


        statement = "UPDATE unique_address " \
                     "SET real_out_deg= " + str(len(unique_outerEdge)) + \
                     " WHERE address = " + "'" + row[0]+ "'"

        #print(statement)
        cur2.execute(statement)
        con.commit()

def getRealInDegree(cur,con,cur2):
    conn = Neo4jConnection(uri='bolt://localhost:7687', user='trohwede', pwd='1687885@uma')


    selection = 'SELECT address FROM unique_address ' \
                'WHERE real_in_deg IS NULL'

    #print(selection)
    cur.execute(selection)
    for row in cur:

        #nimmt addresse und guckt welche transactions zu der wallet führen
        query2= '''
        MATCH (tr:Transaction)-[s:RECEIVES]->(a:Address)
        WHERE a.address='{0}'
        RETURN tr.txid AS txid
        '''.format(row[0])

        result2 = conn.query(query2)

        #print(result2)
        all_trid_of_inEdge = list()

        for x in result2:
            all_trid_of_inEdge.append(x["txid"])

        temp = np.asarray(all_trid_of_inEdge)
        unique_innerEdge = np.unique(temp)

        statement = "UPDATE unique_address " \
                    "SET real_in_deg= " + str(len(unique_innerEdge)) + \
                    " WHERE address = " + "'" + row[0]+ "'"
        #print(statement)

        cur2.execute(statement)
        con.commit()


def getUSDValueForETHtran(cur, con, cur2):
    selection = 'SELECT id, qty, price, timestamp FROM hitbtc_trans_eth ' \
                'WHERE usd_total IS NULL'

    #print(selection)
    cur.execute(selection)
    for row in cur:

        statement = '''
                        UPDATE hitbtc_trans_eth 
                        SET usd_total = {1} * {2}* 
                            (SELECT price FROM hitbtc_trans_usdt 
                            WHERE EXTRACT(EPOCH FROM ('{3}'- hitbtc_trans_usdt.timestamp)) < 
                            1000 LIMIT 1)
                        WHERE id = {0}'''.format(row[0], row[1], row[2], row[3])
        #print(statement)

        cur2.execute(statement)
        con.commit()


def originChecker(cur, con, cur2):
    conn = Neo4jConnection(uri='bolt://localhost:7687', user='trohwede', pwd='1687885@uma')
    list_of_all_addr = []
    qty = []
    selection = '''Select address FROM deposit_address '''

    #print(selection)
    cur.execute(selection)


    for address in cur:
        try:
            #nimmt addresse und guckt welche transactions zu der addresse führen
            query2= '''
           MATCH (tr:Transaction)-[r:RECEIVES]->(a:Address)
            WHERE a.address='{0}'
            MATCH (a2:Address)-[s:SENDS]->(tr2:Transaction)
            WHERE tr2.txid = tr.txid
            RETURN a2.address , r.value
            '''.format(address[0])

            origin = conn.query(query2)

            #print(query2)
            
            for address2 in origin:
                #print(address2[0])
                list_of_all_addr.append(address2[0])
                qty.append(address2[1])

        except (Exception) as error:
            print("Error while connecting to PostgreSQL", error)
            print(query2)


    df = pds.DataFrame({'address':list_of_all_addr, 'qty':qty})
    df2 = df.groupby('address').agg(Count=('qty', 'sum'), Value=('qty', 'count'))


    for addr2, count, qty2 in df2.itertuples():
        statement = ''' 
                INSERT INTO origin (address, count,qty)
                VALUES ('{0}',{1},'{2}')
        '''.format(addr2,count, qty2)

        cur2.execute(statement)
        con.commit()

def originLabel(cur, con,cur2):
    conn = Neo4jConnection(uri='bolt://localhost:7687', user='trohwede', pwd='1687885@uma')
    list_of_all_addr = []
    qty = []
    selection = '''SELECT address FROM origin WHERE label IS NULL'''

    #print(selection)
    cur.execute(selection)

    for row in cur:

        try:

            url = "https://api.graphsense.info/btc/addresses/" + row[0] +"/tags?pagesize=10"
            headers = {'Accept': 'application/json', 'Authorization':  '/trlZnh9014X8kosj4mNW6ZaAep+e8+1'}

            response = requests.get(url, headers=headers)
            edited = json.loads(response.text)

            if (len(edited['address_tags'])>0):
                statement = '''
                            UPDATE origin 
                            SET label = {0}, source ={1} 
                            WHERE address = '{2}'  '''.format(edited['address_tags'][0]['label'], edited['address_tags'][
                    0]['source'], row[0])
                print(edited['address_tags'][0]['label'])
                cur2.execute(statement)
                con.commit()
            else:
                statement = '''
                            UPDATE origin 
                            SET label = '0', source ='0' 
                            WHERE address = '{0}'  '''.format(row[0])
                cur2.execute(statement)
                con.commit()
        except Exception:
            print(url)


def densityChecker(cur,con,cur2):

    # USDT, ETH, USDC
    currency = ['usdt', 'eth', 'usdc']
    # USDT USDC = SELL, ETH = BUY
    side = ['sell', 'buy', 'sell']
    argument = ['', '', r"WHERE time BETWEEN '2018-12-26 16:50:20.971000' and '2023-12-01' "]

    for id in range (1,len(currency)):
        curr = currency[id]
        density3 = []
        density2 = []
        density1 = []

        selection = '''
        SELECT time, qty, txid, inc_address FROM deposit_transactions {0} 
        '''.format(argument[id])

        cur.execute(selection)

        for row in cur:
            #print(curr)

            timebordertemp = row[0]
            timediff0 = datetime.timedelta(minutes=10)
            lowertime = timebordertemp + timediff0

            timediff3 = datetime.timedelta(hours=3)
            timediff2 = datetime.timedelta(hours=2)
            timediff1 = datetime.timedelta(hours=1)
            timeborder3 = timebordertemp + timediff3
            timeborder2 = timebordertemp + timediff2
            timeborder1 = timebordertemp + timediff1

            statement3 = '''
                SELECT COUNT(*)
                FROM hitbtc_trans_{3} 
                WHERE side = '{2}' AND timestamp BETWEEN '{0}' AND '{1}'
                '''.format(str(lowertime), str(timeborder3), side[id], curr)

            statement2 = '''
                SELECT COUNT(*)
                FROM hitbtc_trans_{3} 
                WHERE side = '{2}' AND timestamp BETWEEN '{0}' AND '{1}'
                '''.format(str(lowertime), str(timeborder2), side[id], curr)

            statement1 = '''
                SELECT COUNT(*)
                FROM hitbtc_trans_{3} 
                WHERE side = '{2}' AND timestamp BETWEEN '{0}' AND '{1}'
                '''.format(str(lowertime), str(timeborder1), side[id], curr)

            #print(statement3)
            cur2.execute(statement3)
            density3.append(cur2.fetchone()[0])


            cur2.execute(statement2)
            density2.append(cur2.fetchone()[0])


            cur2.execute(statement1)
            density1.append(cur2.fetchone()[0])


        print(curr)
        print(" Beta = 3 has avg. density of", sum(density3) / len(density3), len(density3), "", sum(density3))
        print(" Beta = 2 has avg. density of", sum(density2) / len(density2), len(density2), "", sum(density2))
        print(" Beta = 1 has avg. density of", sum(density1) / len(density1), len(density1), "", sum(density1))

main()
