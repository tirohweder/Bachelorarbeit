import math
import settings
import datetime

global currency
global side

currency = settings.currency

if currency == 'eth':
    side = 'buy'
else:
    side = 'sell'


def main():
    try:
        con = settings.con
        cur = con.cursor()
        cur2 = con.cursor()
        cur3 = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        find_match_trunc(cur, con, cur2, cur3)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            cur2.close()
            cur3.close()
            con.close()
            print("PostgreSQL connection is closed")


def find_match(cur, con, cur2, cur3):
    selection = '''
                SELECT time, qty, txid, inc_address FROM deposit_transactions 
                WHERE match_{0}_3_2 IS NULL
                '''.format(currency)

    cur.execute(selection)
    for row in cur:
        time_border_temp = row[0]
        timediff1 = datetime.timedelta(minutes=10)
        lower_time = time_border_temp + timediff1

        timediff2 = datetime.timedelta(hours=3)
        time_border = time_border_temp + timediff2

        statement = '''
                    SELECT id, trade_size_btc, timestamp 
                    FROM hitbtc_trans_{2} 
                    WHERE side = '{3}' AND timestamp BETWEEN '{0}' AND '{1}'
                    '''.format(str(lower_time), str(time_border), currency, side)

        cur2.execute(statement)
        count = 0
        for row2 in cur2:
            # 2 % border | when doing market trading there could be a deviation of upto 2% when doing limit trading
            if float(row2[1]) <= float(row[1]) and float(row2[1]) >= float(row[1]) - (float(row[1]) / 100) * 2:
                diff = round((row2[2] - row[0]).total_seconds() / 60, 2)
                count = count + 1
                statement2 = \
                    '''
                INSERT INTO matches_{4} (txid, time_diff, tran_qty,dep_qty, tran_id, inc_address)
                VALUES ('{0}','{1}',{2},{3},{5},'{6}')'''.format(str(row[2]), str(diff), str(row2[1]),
                                                                 str(row[1]),
                                                                 currency, str(row2[0]), row[3])

                cur3.execute(statement2)
                con.commit()

        statement3 = ''' 
                UPDATE deposit_transactions
                SET match_{3}_3_2 = '{0}'
                WHERE txid = '{1}' AND inc_address ='{2}'
                '''.format(count, row[2], row[3], currency)

        cur3.execute(statement3)
        con.commit()


# Sets number of matches for each parameter, exceute at end
def set_parameter_matches(cur, con):
    currency_list = ['usdt', 'eth', 'usdc']

    parameter_list = ['3_2', '2_2', '1_2', '3_0', '2_0', '1_0']
    alpha = [180, 120, 60, 180, 120, 60]

    extended_statement = ['', '', '', 'AND matches_eth.dep_qty= matches_eth.tran_qty',
             'AND matches_eth.dep_qty= matches_eth.tran_qty', 'AND matches_eth.dep_qty= matches_eth.tran_qty']

    for i, currency in enumerate(currency_list):
        for j, parameter in enumerate(parameter_list):
            statement = '''
                        UPDATE deposit_transactions
                        SET match_{0}_{1} = (select count(*) from matches_{0} where matches_{0}.txid =
                        deposit_transactions.txid 
                        AND matches_{0}.inc_address = deposit_transactions.inc_address 
                        AND matches_{0}.time_diff<={2}
                        {3});
                        '''.format(currency, parameter, alpha[j],extended_statement[j])
            cur.execute(statement)
            con.commit()


# Experimental matching method for beta, not used
def find_match_trunc(cur, con, cur2, cur3):
    # USDT and ETH = 24/01/19, USDC = 12/02/19
    change_date = '24/01/19'
    # USDT, USDC = qty, ETH= trade_size_btc as qty
    qty_sel = 'qty'

    selection = '''
    SELECT time, qty, txid, inc_address FROM deposit_transactions 
    WHERE match_{0}_3_1 IS NULL
    '''.format(currency)

    cur.execute(selection)

    for row in cur:

        time_border_temp = row[0]
        timediff1 = datetime.timedelta(minutes=10)
        lower_time = time_border_temp + timediff1

        timediff2 = datetime.timedelta(hours=3)
        time_border = time_border_temp + timediff2

        # Floor needs to be changed according to lot/step size ##################################################
        if row[0] < datetime.datetime.strptime(change_date, '%d/%m/%y'):
            trunc = math.floor(row[1] * 1000) / 1000.0
        else:
            trunc = math.floor(row[1] * 100000) / 100000.0
        statement = '''
            SELECT id, {4}, timestamp
            FROM hitbtc_trans_{3} 
            WHERE side = '{2}' AND timestamp BETWEEN '{0}' AND '{1}'
            '''.format(str(lower_time), str(time_border), side, currency, qty_sel)

        cur2.execute(statement)
        count = 0
        for row2 in cur2:
            if float(row2[1]) == trunc:
                diff = round((row2[2] - row[0]).total_seconds() / 60, 2)
                count = count + 1
                statement2 = \
                    '''
                INSERT INTO matches_{8}_trunc (txid, time_diff, tran_qty,dep_qty,tran_id, inc_address, og_qty)
                VALUES ('{0}','{1}',{2},{3},{4},'{4}','{6}')'''.format(str(row[2]), str(diff), str(row2[1]),
                                                                       str(trunc), str(row2[0]), row[3],
                                                                       str(row[1]), currency)
                cur3.execute(statement2)
                con.commit()

        statement3 = ''' 
                UPDATE deposit_transactions
                SET match_{3}_3_1= '{0}'
                WHERE txid = '{1}' AND inc_address ='{2}'
                '''.format(count, row[2], row[3], currency)

        cur3.execute(statement3)
        con.commit()


main()
