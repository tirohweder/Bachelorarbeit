import json
import numpy as np
import datetime
from collections import Counter
import requests
import pandas as pd
import yaml
from pathlib import Path
import settings


master_address = settings.master_address
path_tagpacks = Path(settings.path_tagpacks).glob('**/*.yaml')


def main():
    try:
        con = settings.con
        cur = con.cursor()
        cur2 = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        # quicken_data(cur,con)

        # in_out_degree(cur,con,cur2)

        # real_in_out_degree(cur,con,cur2)


        # conn_with_host(cur, con)
        real_conn_with_host(cur, con)
        # get_usd_value_for_eth_tran(cur,con,cur2)

        # address_from_tagpack(cur, con)
        # origin_checker(cur,con,cur2)
        # origin_label(cur,con,cur2)

        # entity_adder(cur, con, cur2)
        # entity_label(cur, con, cur2)

        # density_checker(cur,con,cur2)


    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            cur2.close()
            con.close()
            print("PostgreSQL connection is closed")


def in_out_degree(cur, con, cur2):
    conn = settings.conn

    selection = '''
                SELECT address FROM potential_deposit_address
                WHERE in_degree IS NULL
                '''

    cur.execute(selection)
    for address in cur:
        # Returns in and out degree of each address
        query2 = '''
        MATCH (a:Address)
        WHERE a.address='{0}'
        RETURN a.inDegree AS inDegree, a.outDegree AS outDegree
        '''.format(address[0])

        result2 = conn.query(query2)

        statement = '''
                    UPDATE potential_deposit_address 
                    SET in_degree = {0} , out_degree = {1}  
                    WHERE address = '{2}'
                    '''.format(str(result2[0]["inDegree"]), str(result2[0]["outDegree"]), address[0])

        cur2.execute(statement)
        con.commit()

def real_in_out_degree(cur, con, cur2):
    conn = settings.conn

    selection = '''
                SELECT address FROM potential_deposit_address
                WHERE real_in_deg IS NULL or real_out_deg IS NULL
                '''

    cur.execute(selection)

    for row in cur:
        # nimmt addresse und guckt welche transactions zu der wallet führen
        query_in_deg = '''
        MATCH (tr:Transaction)-[s:RECEIVES]->(a:Address)
        WHERE a.address='{0}'
        RETURN tr.txid AS txid
        '''.format(row[0])

        query_out_deg = '''
        MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
        WHERE a.address='{0}'
        RETURN tr.txid AS txid
        '''.format(row[0])

        result_in_deg = conn.query(query_in_deg)
        result_out_deg = conn.query(query_out_deg)

        list_in_deg = [x["txid"] for x in result_in_deg]
        list_out_deg = [x["txid"] for x in result_out_deg]
        temp_in_deg = np.asarray(list_in_deg)
        unique_in_deg = np.unique(temp_in_deg)

        temp_out_deg = np.asarray(list_out_deg)
        unique_out_deg = np.unique(temp_out_deg)

        statement = '''
                    UPDATE potential_deposit_address
                    SET real_in_deg= {0}, real_out_deg = {2} 
                    WHERE address = '{1}'
                    '''.format(str(len(unique_in_deg)), row[0],str(len(unique_out_deg)))

        cur2.execute(statement)
        con.commit()




def conn_with_host(cur, con):
    conn = settings.conn
    list_conn_with_host = []
    list_real_conn_with_host = []
    # Returns all transactions that lead to the master address
    query = '''
    MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
    WHERE tr.address='{0}'
    RETURN t.txid AS t_txid
    '''.format(master_address)

    result = conn.query(query)

    for incoming_transactions in result:
        # Returns all addresses that are part of the transaction
        query3 = '''
        MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
        WHERE tr.txid='{0}'
        RETURN a.address AS address
        '''.format(incoming_transactions["t_txid"])

        result3 = conn.query(query3)

        temp_real_con = []
        for x in result3:
            list_conn_with_host.append(x["address"])
            temp_real_con.append(x["address"])

        temp2 = np.asarray(temp_real_con)
        unique_real_con = np.unique(temp2)

        list_real_conn_with_host.extend(iter(unique_real_con))
    counts_conn_with_host = dict(Counter(list_conn_with_host))
    duplicates_conn_with_host = dict(counts_conn_with_host)

    counts_real_conn_with_host = dict(Counter(list_real_conn_with_host))
    duplicates_real_conn_with_host = dict(counts_real_conn_with_host)

    # Returns how often the address in involved in a transaction with the master address
    for keys in duplicates_conn_with_host:
        statement = '''
                    UPDATE potential_deposit_address 
                    SET conn_with_host= {0}
                    WHERE address = '{1}'
                    '''.format(str(duplicates_conn_with_host[keys]), keys)

        cur.execute(statement)
        con.commit()

    for keys in duplicates_real_conn_with_host:
        statement = '''
                    UPDATE potential_deposit_address 
                    SET real_conn_with_host= {0}
                    WHERE address = '{1}'
                    '''.format(str(duplicates_real_conn_with_host[keys]), keys)

        cur.execute(statement)
        con.commit()


def real_conn_with_host(cur, con):
    conn = settings.conn
    list_of_all_addr = []

    # Returns all transactions that lead to the master address
    query = '''
    MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
    WHERE tr.address='{0}'
    RETURN t.txid AS t_txid
    LIMIT 50
    '''.format(master_address)

    result = conn.query(query)

    for incoming_transactions in result:
        # here i get all address that are part of a transaktion
        query3 = '''
        MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
        WHERE tr.txid='{0}'
        RETURN a.address AS address
        '''.format(incoming_transactions["t_txid"])

        result3 = conn.query(query3)
        temp = [x["address"] for x in result3]
        temp2 = np.asarray(temp)
        unique_address_in_transaction = np.unique(temp2)

        list_of_all_addr.extend(iter(unique_address_in_transaction))
        counts = dict(Counter(list_of_all_addr))
        duplicates = dict(counts)
        for keys in duplicates:
            statement = '''
                        UPDATE potential_deposit_address
                        SET real_conn_with_host= {0}
                        WHERE address = '{1}'
                        '''.format(str(duplicates[keys]), keys)

            cur.execute(statement)
            con.commit()





def set_usd_value_for_eth(cur, con, cur2):
    selection = '''
                SELECT id, qty, price, timestamp FROM hitbtc_trans_eth
                WHERE usd_total IS NULL
                '''

    cur.execute(selection)

    for row in cur:
        statement = '''
                        UPDATE hitbtc_trans_eth 
                        SET usd_total = {1} * {2}* 
                            (SELECT price FROM hitbtc_trans_usdt 
                            WHERE EXTRACT(EPOCH FROM ('{3}'- hitbtc_trans_usdt.timestamp)) < 
                            1000 LIMIT 1)
                        WHERE id = {0}'''.format(row[0], row[1], row[2], row[3])

        cur2.execute(statement)
        con.commit()


def origin_checker(cur, con, cur2):
    conn = settings.conn
    list_of_all_addr = []
    qty = []
    selection = '''
                Select address FROM deposit_address 
                '''

    cur.execute(selection)

    for address in cur:
        try:
            query2 = '''
            MATCH (tr:Transaction)-[r:RECEIVES]->(a:Address)
            WHERE a.address='{0}'
            MATCH (a2:Address)-[s:SENDS]->(tr2:Transaction)
            WHERE tr2.txid = tr.txid
            RETURN a2.address , r.value
            '''.format(address[0])

            origin = conn.query(query2)

            for address2 in origin:
                list_of_all_addr.append(address2[0])
                qty.append(address2[1])

        except (Exception) as error:
            print("Error while connecting to PostgreSQL", error)
            print(query2)

    df = pd.DataFrame({'address': list_of_all_addr, 'qty': qty})
    df2 = df.groupby('address').agg(Count=('qty', 'sum'), Value=('qty', 'count'))

    for addr2, count, qty2 in df2.itertuples():
        statement = ''' 
                INSERT INTO origin (address, count,qty)
                VALUES ('{0}',{1},'{2}')
        '''.format(addr2, count, qty2)

        cur2.execute(statement)
        con.commit()


def address_from_tagpack(cur, con):
    for path in path_tagpacks:
        with open(str(path), "r") as stream:
            try:
                data = (yaml.safe_load(stream))
                for row in data['tags']:
                    statement = ''' 
                            INSERT INTO tags (address)
                            VALUES ('{0}')
                    '''.format(row['address'])
                    cur.execute(statement)
                    con.commit()
            except yaml.YAMLError as exc:
                print(exc)


def origin_label(cur, con, cur2):
    selection = '''
                SELECT address, count FROM origin ORDER BY count desc LIMIT 100
                '''

    cur.execute(selection)

    for row in cur:
        try:
            url = "https://api.graphsense.info/btc/addresses/" + row[0] + "/tags?pagesize=10"
            headers = {'Accept': 'application/json', 'Authorization': settings.graphsense_api_key}

            response = requests.get(url, headers=headers)
            edited = json.loads(response.text)

            if len(edited['address_tags']) > 0:
                statement = '''
                            UPDATE origin 
                            SET label = '{0}', source ='{1}' 
                            WHERE address = '{2}'  
                            '''.format(edited['address_tags'][0]['label'],
                                       edited['address_tags'][
                                           0]['source'], row[0])
                print(row[0], edited['address_tags'][0]['label'])
                print(statement)
            else:
                statement = '''
                            UPDATE origin 
                            SET label = '0', source ='0' 
                            WHERE address = '{0}'  '''.format(row[0])
            cur2.execute(statement)
            con.commit()
        except Exception:
            print("url")


def entity_adder(cur, con, cur2):
    selection = '''SELECT address, qty FROM origin ORDER BY qty desc LIMIT 100 '''
    cur.execute(selection)

    for row in cur:

        try:

            url = "https://api.graphsense.info/btc/addresses/" + row[0] + "/entity?include_tags=true"
            headers = {'Accept': 'application/json', 'Authorization': settings.graphsense_api_key}

            response = requests.get(url, headers=headers)
            edited = json.loads(response.text)
            print(edited['entity'])

            statement = '''
                            UPDATE origin 
                            SET entity = {0} 
                            WHERE address = '{1}'  '''.format(edited['entity'], row[0])
            print(row[0], edited['entity'])
            print(statement)
            cur2.execute(statement)

            con.commit()
        except Exception as e:
            print(e)


def entity_label(cur, con, cur2):
    selection = '''
                SELECT entity, address, qty FROM origin ORDER BY qty desc LIMIT 100 
                '''

    cur.execute(selection)
    for row in cur:
        try:
            url = f"https://api.graphsense.info/btc/entities/{str(row[0])}/tags?level=entity&pagesize=10"

            headers = {'Accept': 'application/json', 'Authorization': settings.graphsense_api_key}
            response = requests.get(url, headers=headers)
            edited = json.loads(response.text)
            if len(edited['entity_tags']) > 0:
                statement = '''
                                UPDATE origin 
                                SET label_2 = '{0}', source_2 ='{1}' 
                                WHERE address = '{2}'  '''.format(edited['entity_tags'][0]['label'],
                                                                  edited['entity_tags'][
                                                                      0]['source'], row[1])
                print(row[0], edited['entity_tags'][0]['label'])
                print(statement)
            else:
                statement = '''
                                UPDATE origin 
                                SET label_2 = '0', source_2 ='0' 
                                WHERE address = '{0}'  '''.format(row[1])
            cur2.execute(statement)

            con.commit()
        except Exception as e:
            print(e)


# Prints how many HitBTC transactions there are on average for alpha = 1,2,3 and currency usdt, eth, usdc
def density_checker(cur, cur2):
    # USDT, ETH, USDC
    currency = ['usdt', 'eth', 'usdc']
    # USDT USDC = SELL, ETH = BUY
    side = ['sell', 'buy', 'sell']
    argument = ['', '', r"WHERE time BETWEEN '2018-12-26 16:50:20.971000' and '2023-12-01' "]

    for id in range(1, len(currency)):
        curr = currency[id]
        density3 = []
        density2 = []
        density1 = []

        selection = '''
                    SELECT time, qty, txid, inc_address FROM deposit_transactions {0} 
                    '''.format(argument[id])

        cur.execute(selection)
        for row in cur:
            time_border_temp = row[0]
            timediff0 = datetime.timedelta(minutes=10)
            lower_time = time_border_temp + timediff0

            timediff3 = datetime.timedelta(hours=3)
            timediff2 = datetime.timedelta(hours=2)
            timediff1 = datetime.timedelta(hours=1)
            time_border3 = time_border_temp + timediff3
            time_border2 = time_border_temp + timediff2
            time_border1 = time_border_temp + timediff1

            statement3 = '''
                SELECT COUNT(*)
                FROM hitbtc_trans_{3} 
                WHERE side = '{2}' AND timestamp BETWEEN '{0}' AND '{1}'
                '''.format(str(lower_time), str(time_border3), side[id], curr)

            statement2 = '''
                SELECT COUNT(*)
                FROM hitbtc_trans_{3} 
                WHERE side = '{2}' AND timestamp BETWEEN '{0}' AND '{1}'
                '''.format(str(lower_time), str(time_border2), side[id], curr)

            statement1 = '''
                SELECT COUNT(*)
                FROM hitbtc_trans_{3} 
                WHERE side = '{2}' AND timestamp BETWEEN '{0}' AND '{1}'
                '''.format(str(lower_time), str(time_border1), side[id], curr)

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
