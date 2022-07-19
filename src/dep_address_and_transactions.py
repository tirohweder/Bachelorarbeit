import numpy as np
import settings
from collections import Counter

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
        find_address_transactions(cur, con)

        # Applies filter, and creates deposit_address, deposit_transactions
        insert_deposit_address_transactions(cur, con)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            con.close()
            print("PostgreSQL connection is closed")


def find_address_transactions(cur, con):
    list_of_all_addr = []
    list_conn_with_host = []
    list_real_conn_with_host = []

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

        temp_real_con = []
        for x in result3:
            list_conn_with_host.append(x["address"])
            temp_real_con.append(x["address"])

        temp2 = np.asarray(temp_real_con)
        unique_real_con = np.unique(temp2)

        for x in unique_real_con:
            list_real_conn_with_host.append(x)

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

        #####################

        query_in_deg = '''
        MATCH (tr:Transaction)-[s:RECEIVES]->(a:Address)
        WHERE a.address='{0}'
        RETURN tr.txid AS txid
        '''.format(x[0])

        query_out_deg = '''
        MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
        WHERE a.address='{0}'
        RETURN tr.txid AS txid
        '''.format(x[0])

        result_in_deg = conn.query(query_in_deg)
        result_out_deg = conn.query(query_out_deg)

        list_in_deg = []
        list_out_deg = []

        for x in result_in_deg:
            list_in_deg.append(x["txid"])

        for x in result_out_deg:
            list_out_deg.append(x["txid"])

        temp_in_deg = np.asarray(list_in_deg)
        unique_in_deg = np.unique(temp_in_deg)

        temp_out_deg = np.asarray(list_out_deg)
        unique_out_deg = np.unique(temp_out_deg)

        ########



        statement_update_unique_address = '''
                        INSERT INTO potential_deposit_address(address, in_degree, out_degree, real_in_deg,real_out_deg) 
                        VALUES ('{0}', '{1}' ,{2}, {3},{4}) 
                        '''.format(x, result_in_out_degree[0]["inDegree"],
                                   result_in_out_degree[0]["outDegree"],str(len(unique_in_deg)), str(len(unique_out_deg)))
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



    ###################
    counts_conn_with_host = dict(Counter(list_conn_with_host))
    duplicates_conn_with_host = {key: value for key, value in counts_conn_with_host.items()}

    counts_real_conn_with_host = dict(Counter(list_real_conn_with_host))
    duplicates_real_conn_with_host = {key: value for key, value in counts_real_conn_with_host.items()}


    for keys in duplicates_conn_with_host.keys():
        statement = '''
                    UPDATE potential_deposit_address 
                    SET connections_with_host= {0}
                    WHERE address = '{1}'
                    '''.format(str(duplicates_conn_with_host[keys]), keys)

        cur.execute(statement)
        con.commit()

    for keys in duplicates_real_conn_with_host.keys():
        statement = '''
                    UPDATE potential_deposit_address 
                    SET connections_with_host= {0}
                    WHERE address = '{1}'
                    '''.format(str(duplicates_real_conn_with_host[keys]), keys)

        cur.execute(statement)
        con.commit()
    ######################################

def insert_deposit_address_transactions(cur, con):
    statement = '''
                INSERT INTO deposit_address 
                SELECT * FROM potential_deposit_address
                WHERE real_out_deg - real_conn_with_host = 0
                '''
    cur.execute(statement)
    con.commit()

    statement2 = '''
                INSERT INTO deposit_transactions (txid, time, inc_address, qty)
                SELECT txid, time,inc_address, qty
                FROM potential_depositing_transactions
                WHERE exists(SELECT 1 FROM deposit_address WHERE address=inc_address)'''

    cur.execute(statement2)
    con.commit()


main()
