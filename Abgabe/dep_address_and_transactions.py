import numpy as np
import settings

master_address = settings.master_address
conn = settings.conn


def main():
    try:
        con = settings.con
        cur = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        # Inserts potential_deposit_address and potential_depositing_transactions/_with_blockhash
        find_transactions(cur, con)

        # Applies filter, and creates deposit_address, deposit_transactions
        insert_deposit_address_transactions(cur, con)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            con.close()
            print("PostgreSQL connection is closed")


def find_transactions(cur, con):
    list_of_all_addr = []

    # Returns all transactions which the master address receives
    query2 = '''
    MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
    WHERE tr.address= '{0}'
    RETURN t.txid AS t_txid
    '''.format(master_address)

    result2 = conn.query(query2)

    for incoming_transactions in result2:
        # Returns all addresses that are included in the transactions
        query3 = '''
        MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
        WHERE tr.txid='{0}'
        RETURN a.address AS address
        '''.format(incoming_transactions["t_txid"])

        result3 = conn.query(query3)
        list_of_all_addr.extend(x["address"] for x in result3)

    # Creates a unique set of addresses
    temp = np.asarray(list_of_all_addr)
    list_of_all_addr_uniq = np.unique(temp)

    # Inserts unique addresses into Table -> unique_address
    for x in list_of_all_addr_uniq:
        query_in_out_degree = '''
        MATCH (a:Address)
        WHERE a.address='{0}'
        RETURN a.inDegree AS inDegree, a.outDegree AS outDegree
        '''.format(x)

        result_in_out_degree = conn.query(query_in_out_degree)

        statement_update_unique_address = '''
                        INSERT INTO potential_deposit_address(address, in_degree, out_degree) 
                        VALUES ('{0}', '{1}' ,{2}) 
                        '''.format(x, result_in_out_degree[0]["inDegree"],
                                   result_in_out_degree[0]["outDegree"])
        cur.execute(statement_update_unique_address)
        con.commit()

    # Returns deposit transactions txid, the time of the transaction and the receiving address
    for x in list_of_all_addr_uniq:
        query4 = '''
        MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
        MATCH (t)-[b:BELONGS_TO]->(bl:Block)
        WHERE tr.address='{0}'
        RETURN t.txid AS txid, bl.hash AS hash, bl.mediantime AS time, tr.address AS address
        '''.format(x)

        result4 = conn.query(query4)

        # Inserts results into database, except of transactions that originate from the master address itself
        for xx in result4:
            if (xx["address"] != master_address):
                statement = '''
                            INSERT INTO potential_depositing_transactions_with_blockhash(txid, block_hash, time, inc_address) 
                            VALUES ('{0}','{1}' ,'{2}' , '{3}') 
                            '''.format(xx["txid"], xx["hash"], str(xx["time"]).replace("T", " ")[:19], +xx["address"])

                cur.execute(statement)
                con.commit()

                statement = '''
                            INSERT INTO potential_depositing_transactions(txid, time, inc_address) 
                            VALUES ('{0}','{1}' ,'{2}') 
                            '''.format(xx["txid"], str(xx["time"]).replace("T", " ")[:19], +xx["address"])

                cur.execute(statement)
                con.commit()


def insert_deposit_address_transactions(cur, con):
    statement = '''
                CREATE TABLE deposit_address AS
                SELECT *
                FROM
                potential_deposit_address
                WHERE real_out_deg - real_conn_with_host = 0
                '''
    cur.execute(statement)
    con.commit()

    statement2 = '''
                CREATE TABLE deposit_transactions AS
                SELECT txid, time,inc_address, qty, nr_match_usdt
                FROM potential_depositing_transactions
                WHERE exists(SELECT 1 FROM deposit_address WHERE address=inc_address)'''

    cur.execute(statement2)
    con.commit()


main()
