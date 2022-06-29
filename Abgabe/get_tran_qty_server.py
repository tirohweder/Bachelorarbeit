import json
import subprocess
import psycopg2
import requests
from neo4j import GraphDatabase

def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="hallo123",
                               host="localhost",
                               port="5432",
                               database="trohwede")
        cur = con.cursor()
        cur2 = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        find_qty_api(cur, con, cur2)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error[0])
        print("Error Type", type(error))

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

def find_qty_bitcoinclient(cur, con, cur2):
    selection = 'SELECT * FROM incoming_transactions ' \
                "WHERE qty IS NULL"
    # writing at 2,5 transactions per second -> i will need 1255725.2 seconds to complete whole stuff -> 14,53 days yay
    cur.execute(selection)

    # print("here")
    for row in cur:
        # print(row[0])
        txid = row[0]
        block_hash = row[1]
        address = row[3]

        command = ["bitcoin-cli", "-rpcuser=bitcoin", "-rpcpassword=bitcoin", "getrawtransaction", txid, "true",
                   block_hash]
        # print(command)

        outBtcCliTemp = subprocess.Popen(command, stdout=subprocess.PIPE)
        outBtcCli = outBtcCliTemp.stdout

        formated = json.load(outBtcCli)
        # print (formated["vout"])

        for i in formated["vout"]:
            # print(i["scriptPubKey"])
            # ERRORS because of null data - when a transaction is provably unspendable, it stores null data so nodes
            # dont have to store data to prevent bloating the UTXO database
            try:
                if i["scriptPubKey"]["address"] == address:
                    statement = '''
                                UPDATE incoming_transactions 
                                SET qty =  {0}
                                WHERE txid= '{1}' AND inc_address= '{2}'
                    '''.format(str(i["value"]),txid,address)

                    cur2.execute(statement)
                    con.commit()
            except Exception:
                pass



def find_qty_api(cur, con, cur2):
    selection = '''SELECT * FROM depositing_transactions
                   WHERE qty IS NULL'''

    cur.execute(selection)


    for row in cur:
        try:
            txid = row[0]
            address = row[2]

            response = requests.get("https://blockchain.info/rawtx/" + txid)
            edited= json.loads(response.text)

            value= 0
            try:
                for i in edited["out"]:
                    try:
                        if i["addr"] == address:
                             value= value + int(i["value"])
                    except Exception:
                        pass
            except Exception:
                pass

            #print(txid, address, value)
            statement = "UPDATE depositing_transactions " \
                        "SET qty = " + str(value) + \
                       " WHERE txid= " + "'" + txid + "'" + " AND inc_address= " + "'" + address + "'"

            # print(statement)

            cur2.execute(statement)
            con.commit()
        except Exception:
            pass


def find_qty_neo4j(cur, con, cur2):
    conn = Neo4jConnection(uri='bolt://localhost:7687', user='trohwede', pwd='1687885@uma')

    selection = 'SELECT * FROM incoming_transactions ' \
                "WHERE qty IS NULL"

    cur.execute(selection)

    for row in cur:
        query = '''
                MATCH (t:Transaction)-[r:RECEIVES]->(a:Address)
                WHERE t.txid = '{0}' AND a.address = '{1}'
                RETURN r.value AS qty
                '''.format(row[0], row[3])

        result = conn.query(query)

        total_sum= 0
        for i in result:
            total_sum= total_sum+i["qty"]


        statement = "UPDATE depositing_transactions " \
                    "SET qty = " + str(total_sum / 100000000) + \
                    " WHERE txid= " + "'" + row[0] + "'" + " AND inc_address= " + "'" + row[3] + "'"

        #print(statement)
        cur2.execute(statement)
        con.commit()
main()
