import json
import subprocess
import psycopg2
import requests

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

        find_qty2(cur, con, cur2)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error[0])
        print("Error Type", type(error))

    finally:
        if (con):
            cur.close()
            cur2.close()
            con.close()
            print("PostgreSQL connection is closed")


def find_qty(cur, con, cur2):
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
                    # print(i["value"])

                    statement = "UPDATE incoming_transactions " \
                                "SET qty = " + str(i["value"]) + \
                                " WHERE txid= " + "'" + txid + "'" + " AND inc_address= " + "'" + address + "'"

                    # print(statement)

                    cur2.execute(statement)
                    con.commit()
            except Exception:
                pass

    # value_of_depos = formated["vout"][0]["value"]
    # destin_addr = formated["vout"][0]["scriptPubKey"]["address"]
    # print (value_of_depos, destin_addr)
    # print(json.dumps(formated, indent=4, sort_keys=True))

    #ssh.close()

def find_qty2(cur, con, cur2):
    selection = 'SELECT * FROM depositing_transactions ' \
                "WHERE qty IS NULL"

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
main()
