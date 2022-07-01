import math

import psycopg2
import datetime

def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="hallo123",
                               host="localhost",
                               port="5432", #8877/5432
                               database="trohwede")
        cur = con.cursor()
        cur2= con.cursor()
        cur3= con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        find_match_trunc(cur, con,cur2,cur3)

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
    WHERE match_eth_3_2 IS NULL
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
                INSERT INTO matches_eth (txid, time_diff, tran_qty,dep_qty, pair, tran_id, inc_address)
                VALUES ('{0}','{1}',{2},{3},'{4}',{5},'{6}')'''.format(str(row[2]), str(diff), str(row2[1]),
                                                                        str(row[1]),
                                                                 "USDC",str(row2[0]), row[3])

                cur3.execute(statement2)
                con.commit()

        #CHANGE USDT HERE ---------------------------------------------------------------------------------------------
        statement3 = ''' 
                UPDATE deposit_transactions
                SET match_eth_3_2 = '{0}'
                WHERE txid = '{1}' AND inc_address ='{2}'
                '''.format(count, row[2], row[3])

        cur3.execute(statement3)
        con.commit()

def find_match_trunc(cur, con,cur2,cur3):
    # USDT and ETH = 24/01/19, USDC = 12/02/19
    changeDate = '24/01/19'
    #USDT, ETH, USDC
    currency= 'usdt'
    # USDT USDC = SELL, ETH = BUY
    side= 'sell'
    #USDT, USDC = qty, ETH= trade_size_btc as qty
    qtySel = 'qty'


    selection = '''
    SELECT time, qty, txid, inc_address FROM deposit_transactions 
    WHERE match_{0}_3_1 IS NULL
    '''.format(currency)

    cur.execute(selection)

    for row in cur:

        timebordertemp = row[0]
        timediff1 = datetime.timedelta(minutes=10)
        lowertime= timebordertemp+timediff1

        timediff2 = datetime.timedelta(hours=3)
        timeborder = timebordertemp + timediff2


        if row[0]< datetime.datetime.strptime(changeDate, '%d/%m/%y'):
            trunc = math.floor(row[1] * 1000)/1000.0
        else:
            trunc = math.floor(row[1] * 100000)/100000.0
        statement = '''
            SELECT id, {4}, timestamp
            FROM hitbtc_trans_{3} 
            WHERE side = '{2}' AND timestamp BETWEEN '{0}' AND '{1}'
            '''.format(str(lowertime), str(timeborder), side,currency,qtySel)

        cur2.execute(statement)
        count= 0
        for row2 in cur2:
            if float(row2[1]) == trunc :
                diff = round((row2[2] - row[0]).total_seconds() / 60, 2)
                count = count + 1
                statement2 = \
                    '''
                INSERT INTO matches_{8}_trunc (txid, time_diff, tran_qty,dep_qty, pair, tran_id, inc_address, og_qty)
                VALUES ('{0}','{1}',{2},{3},'{4}',{5},'{6}','{7}')'''.format(str(row[2]), str(diff), str(row2[1]),
                                                                             str(trunc),"USDT2", str(row2[0]), row[3],
                                                                             str(row[1]), currency)
                cur3.execute(statement2)
                con.commit()

        statement3 = ''' 
                UPDATE deposit_transactions
                SET match_{3}_3_1= '{0}'
                WHERE txid = '{1}' AND inc_address ='{2}'
                '''.format(count, row[2], row[3],currency)

        cur3.execute(statement3)
        con.commit()
main()