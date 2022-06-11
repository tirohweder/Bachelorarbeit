import psycopg2
import datetime

def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="1687885@uma",
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
    SELECT time, qty, txid, inc_address FROM real_deposit_transactions 
    WHERE qty IS NOT NULL AND time > '2013-12-30 
    00:00:00.000000' AND nr_match_usdt IS NULL
    '''

    cur.execute(selection)

    for row in cur:

        timebordertemp = row[0]
        timediff = datetime.timedelta(hours=2)
        timeborder = timebordertemp + timediff

        # CHANGE USDT HERE --------------------------------------------------------------------------------------------
        statement = '''
            SELECT id, qty, timestamp 
            FROM hitbtc_trans_usdt 
            WHERE side = 'sell' AND timestamp BETWEEN '{0}' AND '{1}'
            '''.format(str(row[0]), str(timeborder))

        cur2.execute(statement)
        #print("w")
        count= 0
        for row2 in cur2:
            #print (row2[1], row[1])
            # 2 % border | when doing market trading there could be a deviation of upto 2% when doing limit trading
            if float(row2[1]) <= float(row[1]) and float(row2[1]) >= float(row[1]) - (float(row[1]) / 100) * 2:
                diff = round((row2[2] - row[0]).total_seconds() / 60, 2)
                count = count + 1
                #print("x")
                #CHANGE USDT HERE -------------------------------------------------------------------------------------
                statement2 = \
                    '''
                INSERT INTO matches (txid, time_diff, tran_qty,dep_qty, pair, tran_id)
                VALUES ('{0}','{1}',{2},{3},'{4}',{5})'''.format(str(row[2]), str(diff), str(row2[1]), str(row[1]),
                                                                 "USDT",str(row2[0]))

                cur3.execute(statement2)
                con.commit()

        #CHANGE USDT HERE ---------------------------------------------------------------------------------------------
        statement3 = ''' 
                UPDATE real_deposit_transactions
                SET nr_match_usdt = '{0}'
                WHERE txid = '{1}' AND inc_address ='{2}'
                '''.format(count, row[2], row[3])

        cur3.execute(statement3)
        con.commit()


main()