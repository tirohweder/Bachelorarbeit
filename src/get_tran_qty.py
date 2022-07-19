import json
import subprocess
import requests
import settings

# Uncomment the version that you want to use
def main():
    try:
        con = settings.con

        cur = con.cursor()
        cur2 = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        # find_qty_bitcoin_client(cur, con, cur2)
        find_qty_api(cur, con, cur2)
        # find_qty_neo4j(cur, con, cur2)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error[0])
        print("Error Type", type(error))

    finally:
        if (con):
            cur.close()
            cur2.close()
            con.close()
            print("PostgreSQL connection is closed")

def find_qty_bitcoin_client(cur, con, cur2):
    selection = '''
                SELECT * FROM potential_depositing_transactions_with_blockhash
                WHERE qty IS NULL 
                '''
    cur.execute(selection)

    for row in cur:
        txid = row[0]
        block_hash = row[1]
        address = row[3]

        command = ["bitcoin-cli", "-rpcuser=bitcoin", "-rpcpassword=bitcoin", "getrawtransaction", txid, "true",
                   block_hash]

        out_btc_cli_temp = subprocess.Popen(command, stdout=subprocess.PIPE)
        out_btc_cli = out_btc_cli_temp.stdout

        formatted = json.load(out_btc_cli)

        for i in formatted["vout"]:
            # ERRORS because of null data - when a transaction is provably unspendable, it stores null data so nodes
            # don't have to store data to prevent bloating the UTXO database
            try:
                if i["scriptPubKey"]["address"] == address:
                    statement = '''
                                UPDATE incoming_transactions 
                                SET qty =  {0}
                                WHERE txid= '{1}' AND inc_address= '{2}'
                    '''.format(str(i["value"]), txid, address)

                    cur2.execute(statement)
                    con.commit()
            except Exception:
                pass


def find_qty_api(cur, con, cur2):
    selection = '''
                SELECT * FROM potential_depositing_transactions
                WHERE qty IS NULL
                '''

    cur.execute(selection)

    for row in cur:
        try:
            txid = row[0]
            address = row[2]

            response = requests.get("https://blockchain.info/rawtx/" + txid)
            edited = json.loads(response.text)

            value = 0
            try:
                for i in edited["out"]:
                    try:
                        if i["addr"] == address:
                            value = value + int(i["value"])
                    except Exception:
                        pass
            except Exception:
                pass

            statement = ''' 
                        UPDATE potential_depositing_transactions 
                        SET qty = {0}
                        WHERE txid= '{1}' AND inc_address = '{2}' 
                        '''.format(str(value), txid, address)
            cur2.execute(statement)
            con.commit()
        except Exception:
            pass


def find_qty_neo4j(cur, con, cur2):
    conn = settings.conn

    selection = '''
                SELECT * FROM potential_depositing_transactions
                WHERE qty IS NULL
                '''

    cur.execute(selection)

    for row in cur:
        query = '''
                MATCH (t:Transaction)-[r:RECEIVES]->(a:Address)
                WHERE t.txid = '{0}' AND a.address = '{1}'
                RETURN r.value AS qty
                '''.format(row[0], row[2])

        result = conn.query(query)

        total_sum = 0
        for i in result:
            total_sum = total_sum + i["qty"]

        statement = '''
                    UPDATE potential_depositing_transactions
                    SET qty = {0} 
                    WHERE txid= '{1}' AND inc_address= '{2}'
                    '''.format(str(total_sum / 100000000), row[0], row[2])

        cur2.execute(statement)
        con.commit()


main()
