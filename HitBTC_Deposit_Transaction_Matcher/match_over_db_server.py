import psycopg2
import datetime

def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="hallo123",
                               host="localhost",
                               port="5432",
                               database="trohwede")
        cur = con.cursor()
        cur2= con.cursor()
        cur3= con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        find_match(cur, con,cur2,cur3)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            cur2.close()
            cur3.close()
            con.close()
            print("PostgreSQL connection is closed")


def find_match(cur, con,cur2,cur3):

    #CHANGE USDT HERE -------------------------------------------------------------------------------------------------
    selection = '''
    SELECT time, qty, txid, inc_address FROM deposit_transactions 
    WHERE match_usdc_3_2_2 IS NULL
    '''

    cur.execute(selection)

    for row in cur:

        timebordertemp = row[0]
        timediff1 = datetime.timedelta(minutes=10)
        lowertime= timebordertemp+timediff1

        timediff2 = datetime.timedelta(hours=3)
        timeborder = timebordertemp + timediff2

        # CHANGE USDT HERE --------------------------------------------------------------------------------------------
        statement = '''
            SELECT id, trade_size_btc, timestamp 
            FROM hitbtc_trans_eth 
            WHERE side = 'sell' AND timestamp BETWEEN '{0}' AND '{1}'
            '''.format(str(lowertime), str(timeborder))

        cur2.execute(statement)
        count= 0
        for row2 in cur2:
            # 2 % border | when doing market trading there could be a deviation of upto 2% when doing limit trading
            if float(row2[1]) <= float(row[1]) and float(row2[1]) >= float(row[1]) - (float(row[1]) / 100) * 2:
                diff = round((row2[2] - row[0]).total_seconds() / 60, 2)
                count = count + 1
                #CHANGE USDT HERE -------------------------------------------------------------------------------------
                statement2 = \
                    '''
                INSERT INTO matches_eth_2 (txid, time_diff, tran_qty,dep_qty, pair, tran_id, inc_address)
                VALUES ('{0}','{1}',{2},{3},'{4}',{5},'{6}')'''.format(str(row[2]), str(diff), str(row2[1]),
                                                                        str(row[1]),
                                                                 "USDC",str(row2[0]), row[3])

                cur3.execute(statement2)
                con.commit()

        #CHANGE USDT HERE ---------------------------------------------------------------------------------------------
        statement3 = ''' 
                UPDATE deposit_transactions
                SET match_eth_3_2_2 = '{0}'
                WHERE txid = '{1}' AND inc_address ='{2}'
                '''.format(count, row[2], row[3])

        cur3.execute(statement3)
        con.commit()


main()