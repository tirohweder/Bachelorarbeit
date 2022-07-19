import decimal
import ssl
import matplotlib
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import ticker
from matplotlib.dates import DateFormatter
from sqlalchemy import create_engine
from sqlalchemy import sql
import pandas as pds
import dask.dataframe as dd
from scipy import stats
from scipy.stats import chisquare
from scipy.stats import chi2_contingency
from scipy.stats import chi2
from scipy.interpolate import make_interp_spline, BSpline
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
import math
from bioinfokit.analys import stat, get_data
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)

########## FINISHED ######################
# Prints Table 5.1 / Transactions per year
def table_5_1():
    list1 = ['usdt', 'eth', 'usdc']
    list5 = ['sell', 'buy', 'sell']

    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    for id, i in enumerate(list1):
        print(list1[id], ": ")
        for j in range(9):
            statement = '''SELECT COUNT(*) FROM "hitbtc_trans_{0}" WHERE timestamp BETWEEN '{1}-01-01 ' AND 
            '{2}-01-01' AND side='{3}' '''.format(i, 2014 + j, 2015 + j, list5[id])

            df = pds.read_sql(statement, conn)
            print("     ", 2014 + j, f"{df['count'][0]:,}")

    print("Deposit Transactions")
    for j in range(9):
        statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE time BETWEEN '{0}-01-01' AND '{1}-01-01' 
        '''.format(2014 + j, 2015 + j)

        df = pds.read_sql(statement, conn)
        print("     ", 2014 + j, f"{df['count'][0]:,}")

# Prints Table 5.2 / Analysis of Matches
def table_5_2():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    list1 = ['usdt', 'eth', 'usdc']
    list2 = ['3_2', '2_2', '1_2', '3_0', '2_0', '1_0']
    list3 = [180, 120, 60, 180, 120, 60]
    # USDT/ETH 2117028 USDC 1457894 and to select and replace for USDT/ETH/USDC
    total = [2117028, 2117028, 1457894]

    list6 = ['& 3 & 2 ', '& 2 & 2 ', ' & 1 & 2', '& 3 & 0', '& 2 & 0', '& 1 & 0']
    list7 = ['', '', '', 'AND tran_qty = dep_qty', 'AND tran_qty = dep_qty', 'AND tran_qty = dep_qty']

    print("Sum Matches")
    for id, i in enumerate(list1):
        print(i, ": ")
        for idx, j in enumerate(list2):
            statement = '''
                            SELECT COUNT(*), AVG(match_{0}_{1})
                            FROM deposit_transactions WHERE match_{0}_{1} != 0
                           '''.format(list1[id], list2[idx])

            statement2 = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_{0}_{1} = 1'''.format(list1[id],
                                                                                                        list2[idx])
            df3 = pds.read_sql(statement, conn)
            df4 = pds.read_sql(statement2, conn)

            statement3 = '''SELECT COUNT(*) FROM matches_{0} WHERE time_diff <= {1} {2} '''.format(list1[id],
                                                                                                   list3[idx],
                                                                                                   list7[idx])

            df = pds.read_sql(statement3, conn)

            print("     ",
                  list6[idx], " & ",
                  f"{df['count'][0]:,}", " & ",
                  f"{df3['count'][0]:,}", " & ",
                  round((df3['count'][0] / total[id]) * 100, 2), " & ",
                  round(df3['avg'][0], 2), " & ", f"{df4['count'][0]:,}", " & ",
                  round((df4['count'][0] / df3['count'][0]) * 100, 2), " & ",
                  round((df4['count'][0] / total[id]) * 100, 2))

# Appendix Table solo
def table_5_3():
    list1 = ['usdt', 'eth', 'usdc']
    list2 = ['3_2', '2_2', '1_2', '3_0', '2_0', '1_0']
    list3 = [180, 120, 60, 180, 120, 60]
    list4 = [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
    list5 = [95, 65, 35, 95, 65, 35]
    list6 = ['3 & 2', '2 & 2', '1 & 2', '3 & 0', '2 & 0', '1 & 0']
    list7 = ['qty', 'trade_size_btc', 'qty']
    list8= ['','','','AND tran_qty= dep_qty','AND tran_qty= dep_qty','AND tran_qty= dep_qty']
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    for_test = []
    expected = [95, 65, 35, 95, 65, 35, 95, 65, 35, 95, 65, 35, 95, 65, 35, 95, 65, 35]

    for id, i in enumerate(list1):
        print(i, ": ")
        for idx, j in enumerate(list2):

            statement = '''
            SELECT AVG(te) as avg_time_diff, COUNT(*) as unique_hits , AVG(tw) as avg_usd_total,
            AVG(tx) as avg_qty FROM(
            SELECT AVG(time_diff) as te, COUNT(*), AVG(usd_total) as tw, AVG(hitbtc_trans_{0}.{6}) as tx,tran_id
            FROM matches_{0}
            INNER JOIN deposit_transactions
            on matches_{0}.inc_address = deposit_transactions.inc_address
            AND matches_{0}.txid = deposit_transactions.txid
            AND deposit_transactions.match_usdt_{1} = {3}
            AND deposit_transactions.match_eth_{1} = {4}
            AND deposit_transactions.match_usdc_{1} = {5}
            AND time_diff <= {2}
            {7}
            INNER JOIN hitbtc_trans_{0}
            on matches_{0}.tran_id = hitbtc_trans_{0}.id
            GROUP BY matches_{0}.inc_address,matches_{0}.txid, tran_id) as cw
            ;
            '''.format(i, j, list3[idx], list4[id][0], list4[id][1], list4[id][2], list7[id],list8[idx])

            print(statement)
            df = pds.read_sql(statement, conn)
            print("          &",
                  list6[idx], "&",  # Parameters
                  f"{df['unique_hits'][0]:,}", "&",  # Unique Hits Between USDT, ETH, USDC
                  round((df['avg_time_diff'][0]), 2), "&",  # Avg Time Found
                  round(df['avg_usd_total'][0], 2), "&",  # Avg USD Total
                  f"{df['avg_qty'][0]:.4f}", "&",  # AVG qty
                  f"{round(df['avg_usd_total'][0] / df['avg_qty'][0], 2):,}")  # Avg Price Purchased

            # statement2 = '''
            # SELECT AVG(time_diff) as time_diff, COUNT(*)
            # FROM matches_{0}
            # INNER JOIN deposit_transactions
            # on matches_{0}.inc_address = deposit_transactions.inc_address
            # AND matches_{0}.txid = deposit_transactions.txid
            # AND deposit_transactions.match_usdt_{1} = {3}
            # AND deposit_transactions.match_eth_{1} = {4}
            # AND deposit_transactions.match_usdc_{1} = {5}
            # AND time_diff <= {2}
            # GROUP BY matches_{0}.inc_address,matches_{0}.txid
            # '''.format(i, j, list3[idx], list4[id][0], list4[id][1], list4[id][2])
            #
            # df2 = pds.read_sql(statement2, conn)
            # print(df2['time_diff'].mean())
            # result= stats.ttest_1samp(a=df2, popmean=list5[idx])
            # p_value= result[1][0]
            # if( p_value<=0.05):
            #    print("Difference: YES", p_value)
            # else:
            #    print("Difference: NO",p_value)

#Table 5 8
def table_5_8_both():
    list1 = ['usdt', 'eth', 'usdc']
    list2 = ['3_2', '2_2', '1_2', '3_0', '2_0', '1_0']
    list3 = [180, 120, 60, 180, 120, 60]
    list4 = [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
    list5 = [95, 65, 35, 95, 65, 35]
    list6 = ['3 & 2', '2 & 2', '1 & 2', '3 & 0', '2 & 0', '1 & 0']

    list8= ['','','','AND tran_qty= dep_qty','AND tran_qty= dep_qty','AND tran_qty= dep_qty']

    list7 = ['qty', 'trade_size_btc', 'qty']
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    total= [2117028,2117028,1457894]

    for_test = []
    expected = [95, 65, 35, 95, 65, 35, 95, 65, 35, 95, 65, 35, 95, 65, 35, 95, 65, 35]

    for id, i in enumerate(list1):
        print(i, ": ")
        for idx, j in enumerate(list2):

            statement = '''
            SELECT AVG(time_diff) as time_diff, COUNT(*), AVG(usd_total) as usd_total, AVG(hitbtc_trans_{0}.{6}) as 
            qty,
            tran_id
            FROM matches_{0}
            INNER JOIN deposit_transactions
            on matches_{0}.inc_address = deposit_transactions.inc_address
            AND matches_{0}.txid = deposit_transactions.txid
            AND deposit_transactions.match_usdt_{1} = {3}
            AND deposit_transactions.match_eth_{1} = {4}
            AND deposit_transactions.match_usdc_{1} = {5}
            AND time_diff <= {2}
            {7}
            INNER JOIN hitbtc_trans_{0}
            on matches_{0}.tran_id = hitbtc_trans_{0}.id
            GROUP BY  tran_id
            HAVING count(*)=1
            ;
            '''.format(i, j, list3[idx], list4[id][0], list4[id][1], list4[id][2], list7[id],list8[idx])

            print(statement)
            df = pds.read_sql(statement, conn)

            df2 = df.drop_duplicates(subset=['tran_id'])

            #print(df2)

            print("          &",
                  list6[idx], "&",  # Parameters
                  f"{len(df2.index):,}", "&" ,round((len(df2.index)/total[id])*100,2) , "&   &",  # Unique Hits
                  # Between
                  # USDT, ETH, USDC
                  round((df2['time_diff'].mean()), 2), "&",  # Avg Time Found
                  f"{round(df2['usd_total'].mean()):,}", "&",  # Avg USD Total
                  f"{df2['qty'].mean():.4f}"  # AVG qty
                     , r" \\")

# Prints Monthly Transaction Volume as a Table
def table_month_disto():
    pair = ['usdt', 'eth', 'usdc']
    side = ['sell', 'buy', 'sell']
    year_offset = [0, 2, 5]

    label = ['January', 'February', 'March', 'April', 'May ', 'June ', 'July ', 'August', 'September', 'October',
             'November', 'December']

    total_years= [8,6,3]

    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    dic = [[0.0] * 12 for i in range(3)]  # not weighted
    dic2 = [[0.0] * 12 for i in range(3)]  # weigted
    for id, currency in enumerate(pair):
        for j in range(total_years[id]):
            for month in range(12):

                statement2 = '''SELECT COUNT(*) FROM "hitbtc_trans_{0}" WHERE timestamp BETWEEN '{1}-01-01 ' AND 
                '{2}-01-01' AND side='{3}' '''.format(currency, 2014 + j + year_offset[id], 2015 + j + year_offset[id], side[id])

                if (month < 11):
                    statement = '''SELECT COUNT(*) FROM "hitbtc_trans_{0}" WHERE timestamp BETWEEN '{1}-{4}-01 ' AND 
                    '{2}-{5}-01' AND side='{3}' '''.format(currency, 2014 + j + year_offset[id], 2014 + j + year_offset[id], side[id],
                                                           1 + month, 2 + month)

                else:
                    statement = '''SELECT COUNT(*) FROM "hitbtc_trans_{0}" WHERE timestamp BETWEEN '{1}-{4}-01 ' AND 
                    '{2}-{5}-01' AND side='{3}' '''.format(currency, 2014 + j + year_offset[id], 2015 + j + year_offset[id], side[id],
                                                           1 + month, 1)




                df = pds.read_sql(statement, conn)
                df2 = pds.read_sql(statement2, conn)

                dic[id][month] = dic[id][month] + df['count'][0]

                if (df['count'][0] != 0 and df2['count'][0] != 0):
                    dic2[id][month] = dic2[id][month] + (df['count'][0] / df2['count'][0])

                # if(id ==1):
                #   print(df['count'][0], df2['count'][0], dic2[id][k])

    #for i in range(3):
     #   print(list1[i], ": ")
    #    for k in range(12):
    #        # print(dic2[i][k])
    #        print(round(dic[i][k] / totals[i] * 100, 2), '&', round((dic2[i][k] / 9) * 100, 2))



    for k in range(12):
        # print(dic2[i][k])
        print(label[k], '&',
              round((dic[0][k] / 43034262) * 100, 2), '&', #unfitted
              round((dic2[0][k] / 8) * 100, 2), '&',
              round((dic[1][k] / 22897573) * 100, 2), '&',
              round((dic2[1][k] / 6) * 100, 2), '&',
              round((dic[2][k] / 1725647) * 100,2), '&',
              round((dic2[2][k] / 3) * 100, 2), r' \\')



    print(sum(dic[0]))
    print(sum(dic[1]))
    print(sum(dic[2]))

    print(sum(dic2[0]))
    print(sum(dic2[1]))
    print(sum(dic2[2]))

    label2= [1,2,3,4,5,6,7,8,9,10,11,12]


    plt.plot(label2, dic2[0],  label="test")
    plt.plot(label2,dic2[1],  label="test")
    plt.plot(label2,dic2[2],  label="test")

    plt.legend()

    plt.show()
def plot_month_disto():
    month= [1,2,3,4,5,6,7,8,9,10,11,12]
    usdt_fitted = [9.2, 11.26, 7.29, 7.94, 8.19, 7.57, 7.73, 8.23, 7.01, 7.13, 9.38, 9.07]
    eth_fitted = [7.68, 7.74, 8.31, 7.89, 9.97, 6.55, 9.1, 8.22, 6.39, 5.53, 8.49, 14.12]
    usdc_fitted = [3.39, 4.0,4.65,6.03,13.41,8.46, 9.14, 6.81,8.12,10.35, 11.14, 14.51]

    usdt_unfitted=  [7.34,9.78,11.75,10.68,12.45,11.47,7.75,5.48,4.85,4.63,7.11,6.68]
    eth_unfitted = [9.67,9.21,8.31,8.42,10.25,7.1,11.14,8.2,5.71,4.78,7.56,9.66]
    usdc_unfitted = [6.01,5.69,6.17,8,16.01,7.79,6.37,6.59,6.72,8.91,10.69,11.05]

    fig, (ax1,ax2)= plt.subplots(1,2, sharey=True)
    fig.set_size_inches(14,6)

    ax1.plot(month, usdt_fitted,  label="USDT", color='#fee8c8',  marker='o')
    ax1.plot(month, eth_fitted,  label="ETH", color='#fdbb84',marker='o')
    ax1.plot(month, usdc_fitted,  label="USDC", color='#e34a33',  marker='o')
    ax1.axhline(y=8.33, color='r', linewidth=0.7)
    ax1.set_xlabel('Month')
    ax1.set_title('Fitted')
    ax1.set_ylabel('Percent')
    ax1.legend()

    ax2.plot(month, usdt_unfitted,  label="USDT", color='#e5f5f9',  marker='o')
    ax2.plot(month, eth_unfitted,  label="ETH", color='#99d8c9',  marker='o')
    ax2.plot(month, usdc_unfitted,  label="USDC", color='#2ca25f',  marker='o')
    ax2.axhline(y=8.33, color='r', linewidth=0.7)
    ax2.set_title('Unfitted')
    ax2.set_xlabel('Month')
    ax2.legend()

    plt.legend()
    plt.show()

    print(sum(usdt_fitted), sum(eth_fitted), sum(usdc_fitted),sum(usdt_unfitted), sum(eth_unfitted), sum(usdc_unfitted))

def plot_daily_disto2():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    pair = ['usdt', 'eth', 'usdc']
    side = ['sell', 'buy', 'sell']

    unfitted = [[0.0] * 7 for i in range(3)]

    total_years= [8,6,3]
    year_offset = [0, 2, 5]

    print("Unfitted")

    for i, currency in enumerate(pair):
        print(currency)
        for j in range(7):
            statement = '''SELECT COUNT(*)
                           FROM hitbtc_trans_{3}
                           WHERE side='{1}' AND {0} = (SELECT extract(dow from timestamp)) 
                           AND timestamp between '{2}-01-01' 
                           and '2022-01-01'
                           '''.format(j, side[i],2014+year_offset[i],currency)

            unfitted_df = pds.read_sql(statement, conn)
            unfitted[i][j] = unfitted_df['count'][0]

    for i in range(3):
        unfitted[i][:] = [round((x / sum(unfitted[i])) * 100, 2) for x in unfitted[i]]

    print("Fitted")

    fitted = [[0.0] * 7 for i in range(3)]
    for i, currency in enumerate(pair):
        how_many_week = 0
        print(currency)
        for j in range(total_years[i]):
            print(2014+j+year_offset[i])
            for week in range(2, 51):
                total_week= '''SELECT COUNT(*)
                                FROM hitbtc_trans_{0}
                                WHERE side='{1}' AND {2} = (SELECT extract(year from timestamp))
                                AND {3} = (SELECT extract(week from timestamp))
                               '''.format(currency, side[i], 2014+j+year_offset[i], week)

                weeksum_df = pds.read_sql(total_week, conn)
                how_many_week = how_many_week + 1
                for day in range(7):

                    total_day = '''SELECT COUNT(*)
                                    FROM hitbtc_trans_{0} WHERE side='{1}'
                                    AND {2} = (SELECT extract(year from timestamp))
                                    AND {3} = (SELECT extract(week from timestamp))
                                    AND {4} = (SELECT extract(dow from timestamp))
                                   '''.format(currency, side[i], 2014 + j + year_offset[i], week,day)

                    day_df = pds.read_sql(total_day, conn)
                    fitted[i][day] = fitted[i][day] + (day_df['count'][0]/weeksum_df['count'][0])

        for k in range(7):
            fitted[i][k]= fitted[i][k] / how_many_week


    fig, (ax1,ax2)= plt.subplots(1,2, sharey=True)
    fig.set_size_inches(14,6)

    ax1.plot(day, fitted[0],  label="USDT", color='#fee8c8',  marker='o')
    ax1.plot(day, fitted[1],  label="ETH", color='#fdbb84',marker='o')
    ax1.plot(day, fitted[2],  label="USDC", color='#e34a33',  marker='o')
    ax1.axhline(y=8.33, color='r', linewidth=0.7)
    ax1.set_xlabel('Weekday')
    ax1.set_title('Fitted')
    ax1.set_ylabel('Percent')
    ax1.legend()

    ax2.plot(day, unfitted[0],  label="USDT", color='#e5f5f9',  marker='o')
    ax2.plot(day, unfitted[1],  label="ETH", color='#99d8c9',  marker='o')
    ax2.plot(day, unfitted[2],  label="USDC", color='#2ca25f',  marker='o')
    ax2.axhline(y=8.33, color='r', linewidth=0.7)
    ax2.set_title('Weekday')
    ax2.set_xlabel('Month')
    ax2.legend()

    plt.legend()
    plt.show()

    print(fitted, unfitted)

    print(sum(fitted[0]), sum(fitted[1]), sum(fitted[2]),sum(unfitted[0]), sum(unfitted[1]), sum(unfitted[2]))
def plot_daily_disto():
    month= [1,2,3,4,5,6,7]
    usdt_fitted1 = [0.119357,0.152467,0.151739,0.154077,0.153398,0.14539,0.123564]
    eth_fitted1 = [0.126511,0.148043,0.153797,0.153321,0.145502,0.145096,0.127736]
    usdc_fitted1 = [0.126504,0.148855,0.147126,0.150647,0.155876,0.146587,0.124411]

    usdt_fitted = np.round([i * 100 for i in usdt_fitted1],2)
    eth_fitted = np.round([i * 100 for i in eth_fitted1],2)
    usdc_fitted = np.round([i * 100 for i in usdc_fitted1],2)



    usdt_unfitted=  [12.77, 14.84, 15.06, 15.51, 15.08, 14.36, 12.37]
    eth_unfitted = [13.04, 14.99, 14.57, 15.16, 14.95, 14.68, 12.6]
    usdc_unfitted = [12.94, 14.55, 14.46, 15.14, 14.98, 14.9, 13.03]

    print(sum(usdt_fitted),sum(eth_fitted),sum(usdc_fitted),sum(usdt_unfitted),sum(eth_unfitted),sum(usdc_unfitted))


    fig, (ax1,ax2)= plt.subplots(1,2, sharey=True)
    fig.set_size_inches(14,6)

    ax1.plot(month, usdt_fitted,  label="USDT", color='#fee8c8',  marker='o')
    ax1.plot(month, eth_fitted,  label="ETH", color='#fdbb84',marker='o')
    ax1.plot(month, usdc_fitted,  label="USDC", color='#e34a33',  marker='o')
    ax1.axhline(y=14.2857, color='r', linewidth=0.7)
    ax1.set_xlabel('Weekday')
    ax1.set_title('Fitted')
    ax1.set_ylabel('Percent')
    ax1.legend()

    ax2.plot(month, usdt_unfitted,  label="USDT", color='#e5f5f9',  marker='o')
    ax2.plot(month, eth_unfitted,  label="ETH", color='#99d8c9',  marker='o')
    ax2.plot(month, usdc_unfitted,  label="USDC", color='#2ca25f',  marker='o')
    ax2.axhline(y=14.2857, color='r', linewidth=0.7)
    ax2.set_title('Unfitted')
    ax2.set_xlabel('Weekday')
    ax2.legend()

    plt.legend()
    plt.show()
# Prints Figure 5.2 / Monthly Transaction Volume (Graph)

# Prints Figure 5.3 / Distribution of Trade Size in USD
def tran_usd_size():
    print("w")
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    list1 = ['usdt', 'eth', 'usdc']

    #######################################
    statement_usdt = '''SELECT usd_total FROM "hitbtc_trans_usdt" WHERE side='sell' '''
    df_usdt = pds.read_sql(statement_usdt, conn)
    print("wq")
    df_usdt['usdt_buckets'] = pds.cut(df_usdt.usd_total, bins=[0, 1000, 5000, 20000, 50000, 200000, 1000000000000],
                                      right=False)
    df2_usdt = df_usdt['usdt_buckets'].value_counts()
    df2_usdt = (df2_usdt / df2_usdt.sum()) * 100
    #######################################
    statement_eth = '''SELECT usd_total FROM "hitbtc_trans_eth" WHERE side='buy' '''.format(list1[2])
    df_eth = pds.read_sql(statement_eth, conn)

    df_eth['usdt_buckets'] = pds.cut(df_eth.usd_total, bins=[0, 1000, 5000, 20000, 50000, 200000, 1000000000000],
                                     right=False)
    df2_eth = df_eth['usdt_buckets'].value_counts()
    df2_eth = (df2_eth / df2_eth.sum()) * 100
    #######################################
    statement_usdc = '''SELECT usd_total FROM "hitbtc_trans_usdc" WHERE side='sell' '''.format(list1[2])
    df_usdc = pds.read_sql(statement_usdc, conn)

    df_usdc['usdt_buckets'] = pds.cut(df_usdc.usd_total, bins=[0, 1000, 5000, 20000, 50000, 200000, 1000000000000],
                                      right=False)
    df2_usdc = df_usdc['usdt_buckets'].value_counts()
    df2_usdc = (df2_usdc / df2_usdc.sum()) * 100

    # fig, (ax1, ax2) = plt.subplots(1,2, figsize=(10, 5))

    xlabel = [r'0 to \$1k', r'\$1k to \$5k', r'\$5k to \$1k', r'\$20k to \$50k', r'\$50k to \$200k', r'Over \$200k']
    x = np.arange(len(xlabel))

    fig, ax = plt.subplots()
    fig.set_size_inches(12,7)

    print("USDT:",df2_usdt.values )
    print("ETH :", df2_eth.values)
    print("USDC:", df2_usdc.values)

    usdt =ax.bar(x - 0.25, df2_usdt.values, width=0.25, color='#fee8c8', label='USDT')
    eth = ax.bar(x, df2_eth.values, width=0.25, color='#fdbb84', label='ETH')
    usdc = ax.bar(x + 0.25, df2_usdc.values, width=0.25, color='#e34a33', label='USDC')
    ax.set_xticks(x, xlabel)
    ax.autoscale(tight=True)
    ax.set_axisbelow(True)
    ax.grid(axis='y',color='gray')
    ax.set_ylabel('% of Total Trades')


    ax.bar_label(usdt, fmt='%0.2f')
    ax.bar_label(eth, fmt='%0.2f')
    ax.bar_label(usdc, fmt='%0.2f')

    ax.legend()
    # plt.subplots_adjust(bottom=0.2)
    # plt.yscale('log')
    # ax.set_xticks(rotation='vertical')
    plt.show()
    print("Done")

# Prints Figure 5.4 / Number of Deposit Address in % by outgoing Transactions
def connWithHost():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement_usdt = '''SELECT real_in_deg as real_conn_with_host, count(*) from deposit_address GROUP BY real_in_deg'''

    df = pds.read_sql(statement_usdt, conn)
    df['count']= (df['count']/df['count'].sum())*100

    tmp = df[df.real_conn_with_host > 5]
    df = df[df.real_conn_with_host <= 5]

    #rest = {'real_conn_with_host': 6, 'count':tmp['count'].sum()}
    rest = pds.DataFrame([[6,tmp['count'].sum()]], columns=['real_conn_with_host','count'])
    df = pds.concat([df,rest])



    xlabel = ['',r'1', r'2', r'3', r'4', r'5', r'>5']
    x = np.arange(len(xlabel))

    fig, ax = plt.subplots()
    bars = ax.bar(df['real_conn_with_host'], df['count'], label='Deposit Addresses', color='#2ca25f')

    ax.bar_label(bars, fmt='%0.2f')
    ax.set_xlabel('Incoming Transactions')
    ax.set_ylabel('Percent')
    ax.set_xticks(x, xlabel)
    plt.legend()
    plt.yscale('log')
    plt.show()

    print("Done")

#Prints Figure / Benfords Law of Traiding Pairs
def benfords_law2():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    statement = '''SELECT qty FROM hitbtc_trans_usdt WHERE side='sell' '''

    statement2 = '''SELECT trade_size_btc as qty FROM hitbtc_trans_eth WHERE side='buy' '''  # Matches > 0

    statement3 = '''SELECT qty FROM hitbtc_trans_usdc WHERE side='sell' '''

    #statement4 = '''SELECT txid, inc_address,dep_qty as qty, COUNT(*)  FROM "matches_usdc" GROUP BY dep_qty,
    #    inc_address, txid HAVING COUNT(*)=1 '''  # Matches = 1
    #statement2 = '''SELECT qty FROM deposit_transactions WHERE match_usdc_1_2 != 0'''  # Matches > 0

    #statement3 = '''SELECT dep_qty as qty FROM "matches_usdc" WHERE time_diff < 60 '''

    df = pds.read_sql(statement, conn)
    df['qty'] = (df['qty']) * 1000000
    df['leading_digit'] = df['qty'].astype(str).str[0].astype(int)
    df_f = df['leading_digit'].value_counts(normalize=True) * 100
    df_f.sort_index(inplace=True)

    df2 = pds.read_sql(statement2, conn)
    df2['qty'] = (df2['qty']) * 1000000
    df2['leading_digit'] = df2['qty'].astype(str).str[0].astype(int)
    df_f2 = df2['leading_digit'].value_counts(normalize=True) * 100
    df_f2.sort_index(inplace=True)

    df3 = pds.read_sql(statement3, conn)
    df3['qty'] = (df3['qty']) * 1000000
    df3['leading_digit'] = df3['qty'].astype(str).str[0].astype(int)
    df_f3 = df3['leading_digit'].value_counts(normalize=True) * 100
    df_f3.sort_index(inplace=True)

    df_b = pd.DataFrame({'Benford´s Law': [30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]})

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, sharey=True)
    fig.set_size_inches(15, 7.5)

    ax1.bar(df_f.index.values, df_f.values, color='#fee8c8')
    ax1.plot(df_f.index.values, df_b.values, marker='o', color='#215b3a', label='Benford\'s Law')
    ax1.set_xlabel("Leading Digit (qty)")
    ax1.set_ylabel("Probability")
    ax1.set_title("USDT")
    ax1.legend()

    # print(df_f2.index.values)
    ax2.bar(df_f2.index.values, df_f2.values, color='#fdbb84')
    ax2.plot(df_f2.index.values, df_b.values, marker='o', color='#215b3a', label='Benford\'s Law')
    ax2.set_xlabel("Leading Digit (btc_qty)")
    ax2.set_title("ETH")
    ax2.legend()

    ax3.bar(df_f3.index.values, df_f3.values, color='#e34a33')
    ax3.plot(df_f2.index.values, df_b.values, marker='o', color='#215b3a', label='Benford\'s Law')
    ax3.set_xlabel("Leading Digit (qty)")
    ax3.set_title("USDC")
    ax3.legend()

    print(chisquare(df_f.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    print(chisquare(df_f2.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    print(chisquare(df_f3.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))

    plt.show()


#Prints occurance of qty by pair
def occurance_qty_by_pair():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    # SHOWS QTY: THEN HOW MANY OF HITBTC TRAN IS THERE, THEN HOW MANY DEPOSIT TRAN


    ##############################################################################################
    #                                   USDT
    ###############################################################################################
    statement_usdt = '''SELECT qty,count(*) as deposit_transactions FROM "deposit_transactions"  GROUP BY qty 
    HAVING count(*) >0; '''
    df_usdt = pds.read_sql(statement_usdt, conn)
    statement2_usdt = '''SELECT qty, COUNT(*) as hitbtc FROM "hitbtc_trans_usdt" WHERE side= 'sell' 
    GROUP BY qty
    HAVING COUNT(*)>0'''
    df2_usdt = pds.read_sql(statement2_usdt, conn)

    merged_df_usdt = df_usdt.merge(df2_usdt, on='qty', how='outer')

    merged_df_usdt['deposit_transactions'] = merged_df_usdt['deposit_transactions'].fillna(0)
    merged_df_usdt['hitbtc'] = merged_df_usdt['hitbtc'].fillna(0)

    merged_df_usdt['deposit_transactions'] = (merged_df_usdt['deposit_transactions']/sum(merged_df_usdt['deposit_transactions']))*100
    merged_df_usdt['hitbtc'] = (merged_df_usdt['hitbtc']/sum(merged_df_usdt['hitbtc']))*100


    merged_df_usdt = merged_df_usdt[merged_df_usdt.qty <= 1]
    merged_df_usdt.sort_values(by=['qty'], inplace=True)
    merged_df_usdt['diff_count_sum'] = merged_df_usdt.deposit_transactions - merged_df_usdt.hitbtc


    ##############################################################################################
    #                                            ETH
    ###############################################################################################
    statement_eth = '''SELECT qty,count(*) as deposit_transactions FROM "deposit_transactions"  GROUP BY qty 
    HAVING count(*) >0; '''
    df_eth = pds.read_sql(statement_eth, conn)
    statement2_eth = '''SELECT trade_size_btc as qty, COUNT(*) as hitbtc FROM "hitbtc_trans_eth" WHERE side= 'buy' 
    GROUP BY trade_size_btc
    HAVING COUNT(*)>0'''
    df2_eth = pds.read_sql(statement2_eth, conn)

    merged_df_eth = df_eth.merge(df2_eth, on='qty', how='outer')

    merged_df_eth['deposit_transactions'] = merged_df_eth['deposit_transactions'].fillna(0)
    merged_df_eth['hitbtc'] = merged_df_eth['hitbtc'].fillna(0)

    merged_df_eth['deposit_transactions'] = (merged_df_eth['deposit_transactions'] / sum(
        merged_df_eth['deposit_transactions'])) * 100
    merged_df_eth['hitbtc'] = (merged_df_eth['hitbtc'] / sum(merged_df_eth['hitbtc'])) * 100

    merged_df_eth = merged_df_eth[merged_df_eth.qty <= 1]
    merged_df_eth.sort_values(by=['qty'], inplace=True)
    merged_df_eth['diff_count_sum'] = merged_df_eth.deposit_transactions - merged_df_eth.hitbtc

    ##############################################################################################
    #                                            USDC
    ###############################################################################################

    statement_usdc = '''SELECT qty,count(*) as deposit_transactions FROM "deposit_transactions"  GROUP BY qty 
    HAVING count(*) >0; '''
    df_usdc = pds.read_sql(statement_usdc, conn)
    statement2_usdc = '''SELECT qty, COUNT(*) as hitbtc FROM "hitbtc_trans_usdc" WHERE side= 'sell' 
    GROUP BY qty
    HAVING COUNT(*)>0'''
    df2_usdc = pds.read_sql(statement2_usdc, conn)

    merged_df_usdc = df_usdc.merge(df2_usdc, on='qty', how='outer')

    merged_df_usdc['deposit_transactions'] = merged_df_usdc['deposit_transactions'].fillna(0)
    merged_df_usdc['hitbtc'] = merged_df_usdc['hitbtc'].fillna(0)

    merged_df_usdc['deposit_transactions'] = (merged_df_usdc['deposit_transactions']/sum(merged_df_usdc['deposit_transactions']))*100
    merged_df_usdc['hitbtc'] = (merged_df_usdc['hitbtc']/sum(merged_df_usdc['hitbtc']))*100


    merged_df_usdc = merged_df_usdc[merged_df_usdc.qty <= 1]
    merged_df_usdc.sort_values(by=['qty'], inplace=True)
    merged_df_usdc['diff_count_sum'] = merged_df_usdc.deposit_transactions - merged_df_usdc.hitbtc

        ##############################################################################################
    #                                            GRAPH USDT
    ###############################################################################################
    print(merged_df_usdt.sort_values(by=['hitbtc']))
    print(merged_df_usdc.sort_values(by=['hitbtc']))


    fig, axs= plt.subplots(3,2)
    fig.set_size_inches(10.5, 14.85)

    axs[0,0].plot(merged_df_usdt['qty'],merged_df_usdt['hitbtc'], color="#fee8c8",label='USDT',alpha=0.6)
    axs[0,0].plot(merged_df_usdt['qty'], merged_df_usdt['deposit_transactions'], color='#43a2ca',label='DepTran',alpha=0.8)
    axs[0,0].set_ylabel('% of all Transactions')
    axs[0,0].set_title('Occurrence of Quantity')
    axs[0,0].legend()

    axs[0,0].set_xlim(0,1)
    axs[0,0].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[0,0].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[0,0].grid(which='major', axis='x', linestyle='--')
    axs[0,0].grid(which='minor', axis='x', linestyle=':')


    axs[0,1].plot(merged_df_usdt['qty'], merged_df_usdt['diff_count_sum'],color='#2ca25f')
    axs[0,1].set_ylabel('% Difference')
    axs[0,1].set_title('Difference between occurrence')
    axs[0,1].axhline(y=0, color='r', linewidth=0.7)

    axs[0,1].set_xlim(0,1)
    axs[0,1].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[0,1].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[0,1].grid(which='major', axis='x', linestyle='--')
    axs[0,1].grid(which='minor', axis='x', linestyle=':')


    ##############################################################################################
    #                                            GRAPH ETH
    ###############################################################################################
    axs[1,0].plot(merged_df_eth['qty'],merged_df_eth['hitbtc'], color="#fdbb84",label='ETH',alpha=0.6)
    axs[1,0].plot(merged_df_eth['qty'], merged_df_eth['deposit_transactions'], color='#43a2ca',label='DepTran',
                  alpha=0.8)
    axs[1,0].set_ylabel('% of all Transactions')
    axs[1,0].legend()
    axs[1,0].set_xlim(0,1)
    axs[1,0].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[1,0].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[1,0].grid(which='major', axis='x', linestyle='--')
    axs[1,0].grid(which='minor', axis='x', linestyle=':')

    axs[1,1].plot(merged_df_eth['qty'], merged_df_eth['diff_count_sum'],color='#2ca25f')
    axs[1,1].set_ylabel('% Difference')
    axs[1,1].axhline(y=0, color='r', linewidth=0.7)
    axs[1,1].set_xlim(0, 1)
    axs[1,1].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[1,1].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[1,1].grid(which='major', axis='x', linestyle='--')
    axs[1,1].grid(which='minor', axis='x', linestyle=':')

    ##############################################################################################
    #                                            GRAPH USDC
    ###############################################################################################


    axs[2,0].plot(merged_df_usdc['qty'],merged_df_usdc['hitbtc'], color="#e34a33",label='USDC',alpha=0.6)
    axs[2,0].plot(merged_df_usdc['qty'], merged_df_usdc['deposit_transactions'], color='#43a2ca',label='DepTran',
                  alpha=0.8)
    axs[2,0].set_xlabel('Quantity')
    axs[2,0].set_ylabel('% of all Transactions')
    axs[2,0].legend()
    axs[2,0].set_xlim(0,1)
    axs[2,0].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[2,0].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[2,0].grid(which='major', axis='x', linestyle='--')
    axs[2,0].grid(which='minor', axis='x', linestyle=':')

    axs[2,1].plot(merged_df_usdc['qty'], merged_df_usdc['diff_count_sum'],color='#2ca25f')
    axs[2,1].set_xlabel('Quantity')
    axs[2,1].set_ylabel('% Difference')
    axs[2,1].axhline(y=0, color='r', linewidth=0.7)
    axs[2,1].set_xlim(0, 1)
    axs[2,1].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[2,1].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[2,1].grid(which='major', axis='x', linestyle='--')
    axs[2,1].grid(which='minor', axis='x', linestyle=':')



    plt.show()
    print("Done")

#Lookig at trade size distro
def occurance_qty_eth_vs_btc():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    # SHOWS QTY: THEN HOW MANY OF HITBTC TRAN IS THERE, THEN HOW MANY DEPOSIT TRAN

    ##############################################################################################
    #                                   USDT
    ###############################################################################################

    statement2_usdt = '''SELECT qty, COUNT(*) as hitbtc FROM "hitbtc_trans_usdt" WHERE side= 'sell' 
    GROUP BY qty
    HAVING COUNT(*)>0'''
    df_usdt = pds.read_sql(statement2_usdt, conn)
    df_usdt['hitbtc'] = (df_usdt['hitbtc']/sum(df_usdt['hitbtc']))*100
    _df_usdt = df_usdt[df_usdt.qty <= 1]
    df_usdt.sort_values(by=['qty'], inplace=True)



    ##############################################################################################
    #                                            ETH
    ###############################################################################################

    statement2_eth = '''SELECT trade_size_btc as qty, COUNT(*) as hitbtc FROM "hitbtc_trans_eth" WHERE side= 'buy' 
    GROUP BY trade_size_btc
    HAVING COUNT(*)>0'''
    df_eth = pds.read_sql(statement2_eth, conn)
    df_eth['hitbtc'] = (df_eth['hitbtc'] / sum(df_eth['hitbtc'])) * 100
    df_eth = df_eth[df_eth.qty <= 1]
    df_eth.sort_values(by=['qty'], inplace=True)

    statement2_eth2 = '''SELECT qty, COUNT(*) as hitbtc FROM "hitbtc_trans_eth" WHERE side= 'buy' 
    GROUP BY qty
    HAVING COUNT(*)>0'''
    df_eth2 = pds.read_sql(statement2_eth2, conn)
    df_eth2['hitbtc'] = (df_eth2['hitbtc'] / sum(df_eth2['hitbtc'])) * 100
    df_eth2 = df_eth2[df_eth2.qty <= 1]
    df_eth2.sort_values(by=['qty'], inplace=True)

    print(sum(df_eth2['hitbtc']))

    ##############################################################################################
    #                                            USDC
    ###############################################################################################

    statement2_usdc = '''SELECT qty, COUNT(*) as hitbtc FROM "hitbtc_trans_usdc" WHERE side= 'sell' 
    GROUP BY qty
    HAVING COUNT(*)>0'''
    df_usdc = pds.read_sql(statement2_usdc, conn)
    df_usdc['hitbtc'] = (df_usdc['hitbtc']/sum(df_usdc['hitbtc']))*100
    df_usdc = df_usdc[df_usdc.qty <= 1]
    df_usdc.sort_values(by=['qty'], inplace=True)


    ##############################################################################################
    #                                            GRAPH USDT
    ###############################################################################################

    fig, axs= plt.subplots(2,2, sharey= True)
    fig.set_size_inches(15, 10)

    axs[0,0].plot(df_usdt['qty'],df_usdt['hitbtc'], color="#fee8c8",label='USDT')
    axs[0,0].set_ylabel('% of all Transactions')

    axs[0,0].legend()
    axs[0,0].set_xlim(0,1)
    axs[0,0].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[0,0].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[0,0].grid(which='major', axis='x', linestyle='--')
    axs[0,0].grid(which='minor', axis='x', linestyle=':')
    axs[0,0].set_yscale('log')


    ##############################################################################################
    #                                            GRAPH ETH
    ###############################################################################################
    axs[1,0].plot(df_eth['qty'],df_eth['hitbtc'], color="#fdbb84",label='ETH (qty in BTC)')
    axs[1,0].legend()
    axs[1,0].set_xlim(0,1)
    axs[1,0].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[1,0].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[1,0].grid(which='major', axis='x', linestyle='--')
    axs[1,0].grid(which='minor', axis='x', linestyle=':')
    axs[1,0].set_yscale('log')
    axs[1,0].set_ylabel('% of all Transactions')
    axs[1,0].set_xlabel('Quantity')

    axs[1,1].plot(df_eth2['qty'],df_eth2['hitbtc'], color="#fdbb84",label='ETH (qty in ETH)')
    axs[1,1].legend()
    axs[1,1].set_xlim(0,1)
    axs[1,1].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[1,1].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[1,1].grid(which='major', axis='x', linestyle='--')
    axs[1,1].grid(which='minor', axis='x', linestyle=':')
    axs[1,1].set_yscale('log')
    axs[1,1].set_xlabel('Quantity')
    ##############################################################################################
    #                                            GRAPH USDC
    ###############################################################################################


    axs[0,1].plot(df_usdc['qty'],df_usdc['hitbtc'], color="#e34a33",label='USDC')
    axs[0,1].legend()
    axs[0,1].set_xlim(0,1)
    axs[0,1].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[0,1].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[0,1].grid(which='major', axis='x', linestyle='--')
    axs[0,1].grid(which='minor', axis='x', linestyle=':')
    axs[0,1].set_yscale('log')

    plt.show()
    print("Done")

def occurance_parameter_2_vs_0():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    # SHOWS QTY: THEN HOW MANY OF HITBTC TRAN IS THERE, THEN HOW MANY DEPOSIT TRAN

    ##############################################################################################
    #                                   USDT 3_2 vs 3_0
    ###############################################################################################

    statement2_usdt = '''SELECT qty, COUNT(*) as hitbtc FROM "deposit_transactions" WHERE match_usdt_3_2 > 0
    GROUP BY qty '''
    df_usdt = pds.read_sql(statement2_usdt, conn)
    df_usdt['hitbtc'] = (df_usdt['hitbtc']/sum(df_usdt['hitbtc']))*100
    _df_usdt = df_usdt[df_usdt.qty <= 1]
    df_usdt.sort_values(by=['qty'], inplace=True)

    print(sum(df_usdt['hitbtc']))


    statement2_eth = '''SELECT qty, COUNT(*) as hitbtc FROM "deposit_transactions" WHERE match_usdt_3_0 > 0
    GROUP BY qty '''
    df_eth = pds.read_sql(statement2_eth, conn)
    df_eth['hitbtc'] = (df_eth['hitbtc'] / sum(df_eth['hitbtc'])) * 100
    df_eth = df_eth[df_eth.qty <= 1]
    df_eth.sort_values(by=['qty'], inplace=True)

    print(sum(df_eth['hitbtc']))
    ##############################################################################################
    #                                            ETH
    ###############################################################################################



    statement2_eth2 = '''SELECT qty, COUNT(*) as hitbtc FROM "deposit_transactions" WHERE match_eth_3_2 > 0
    GROUP BY qty '''
    df_eth2 = pds.read_sql(statement2_eth2, conn)
    df_eth2['hitbtc'] = (df_eth2['hitbtc'] / sum(df_eth2['hitbtc'])) * 100
    df_eth2 = df_eth2[df_eth2.qty <= 1]
    df_eth2.sort_values(by=['qty'], inplace=True)

    print(sum(df_eth2['hitbtc']))


    statement2_usdc = '''SELECT qty, COUNT(*) as hitbtc FROM "deposit_transactions" WHERE match_eth_3_0 > 0
    GROUP BY qty '''
    df_usdc = pds.read_sql(statement2_usdc, conn)
    df_usdc['hitbtc'] = (df_usdc['hitbtc']/sum(df_usdc['hitbtc']))*100
    df_usdc = df_usdc[df_usdc.qty <= 1]
    df_usdc.sort_values(by=['qty'], inplace=True)

    print(sum(df_usdc['hitbtc']))
    ##############################################################################################
    #                                            USDC
    ###############################################################################################
    statement2_eth22 = '''SELECT qty, COUNT(*) as hitbtc FROM "deposit_transactions" WHERE match_usdc_3_2 > 0
    GROUP BY qty '''
    df_eth22 = pds.read_sql(statement2_eth22, conn)
    df_eth22['hitbtc'] = (df_eth22['hitbtc'] / sum(df_eth22['hitbtc'])) * 100
    df_eth22 = df_eth22[df_eth22.qty <= 1]
    df_eth22.sort_values(by=['qty'], inplace=True)

    print(sum(df_eth22['hitbtc']))


    statement2_usdc2 = '''SELECT qty, COUNT(*) as hitbtc FROM "deposit_transactions" WHERE match_usdc_3_0 > 0
    GROUP BY qty '''
    df_usdc2 = pds.read_sql(statement2_usdc2, conn)
    df_usdc2['hitbtc'] = (df_usdc2['hitbtc']/sum(df_usdc2['hitbtc']))*100
    df_usdc2 = df_usdc2[df_usdc2.qty <= 1]
    df_usdc2.sort_values(by=['qty'], inplace=True)

    print(sum(df_usdc2['hitbtc']))


    ##############################################################################################
    #                                            GRAPH USDT
    ###############################################################################################

    fig, axs= plt.subplots(3,2, sharey= True)
    fig.set_size_inches(15, 10)

    axs[0,0].plot(df_usdt['qty'],df_usdt['hitbtc'], color="#fee8c8",label='USDT 3_2')
    axs[0,0].set_ylabel('% of all Transactions')

    axs[0,0].legend()
    axs[0,0].set_xlim(0,1)
    axs[0,0].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[0,0].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[0,0].grid(which='major', axis='x', linestyle='--')
    axs[0,0].grid(which='minor', axis='x', linestyle=':')
    axs[0,0].set_yscale('log')

    axs[0,1].plot(df_eth['qty'],df_eth['hitbtc'], color="#fee8c8",label='USDT 3_0')
    axs[0,1].legend()
    axs[0,1].set_xlim(0,1)
    axs[0,1].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[0,1].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[0,1].grid(which='major', axis='x', linestyle='--')
    axs[0,1].grid(which='minor', axis='x', linestyle=':')
    axs[0,1].set_yscale('log')
    ##############################################################################################
    #                                            GRAPH ETH
    ###############################################################################################
    axs[1,0].plot(df_eth2['qty'], df_eth2['hitbtc'], color = "#fdbb84", label = 'ETH 3_2' )
    axs[1,0].legend()
    axs[1,0].set_xlim(0,1)
    axs[1,0].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[1,0].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[1,0].grid(which='major', axis='x', linestyle='--')
    axs[1,0].grid(which='minor', axis='x', linestyle=':')
    axs[1,0].set_yscale('log')
    axs[1,0].set_ylabel('% of all Transactions')


    axs[1,1].plot( df_usdc['qty'], df_usdc['hitbtc'], color = "#fdbb84", label = 'ETH 3_0')
    axs[1,1].legend()
    axs[1,1].set_xlim(0,1)
    axs[1,1].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[1,1].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[1,1].grid(which='major', axis='x', linestyle='--')
    axs[1,1].grid(which='minor', axis='x', linestyle=':')
    axs[1,1].set_yscale('log')

    ##############################################################################################
    #                                            GRAPH USDC
    ###############################################################################################
    axs[2,0].plot(df_eth22['qty'], df_eth22['hitbtc'], color = "#e34a33", label = 'USDC 3_2' )
    axs[2,0].legend()
    axs[2,0].set_xlim(0,1)
    axs[2,0].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[2,0].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[2,0].grid(which='major', axis='x', linestyle='--')
    axs[2,0].grid(which='minor', axis='x', linestyle=':')
    axs[2,0].set_yscale('log')
    axs[2,0].set_ylabel('% of all Transactions')
    axs[2,0].set_xlabel('Quantity')

    axs[2,1].plot(df_usdc2['qty'], df_usdc2['hitbtc'], color = "#e34a33", label = 'USDC 3_0')
    axs[2,1].legend()
    axs[2,1].set_xlim(0,1)
    axs[2,1].xaxis.set_major_locator(MultipleLocator(0.1))
    axs[2,1].xaxis.set_minor_locator(AutoMinorLocator(2))
    axs[2,1].grid(which='major', axis='x', linestyle='--')
    axs[2,1].grid(which='minor', axis='x', linestyle=':')
    axs[2,1].set_yscale('log')
    axs[2,1].set_xlabel('Quantity')



    plt.show()
    print("Done")


# Analyses how many deposit addresses belong to what
def deposit_address_analyse():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    statement = '''SELECT address FROM deposit_address '''

    df = pds.read_sql(statement, conn)
    df['wallet_type'] = df['address'].astype(str).str[0]

    df_1 = df['wallet_type'].value_counts(normalize=True) * 100
    print(df_1)


#time diff analyzed
def graph_match():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    list1 = ['usdt', 'eth', 'usdc']
    list2 = ['3_2', '2_2', '1_2', '3_0', '2_0', '1_0']
    list3 = [180, 120, 60, 180, 120, 60]
    list4 = [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
    list5 = ['USDT', 'ETH', 'USDC']
    list6 = ['3 & 2', '2 & 2', '1 & 2', '3 & 0', '2 & 0', '1 & 0']
    list7 = ['qty', 'trade_size_btc', 'qty']

    list8= ['','','','AND tran_qty= dep_qty','AND tran_qty= dep_qty','AND tran_qty= dep_qty']


    for_test = []
    expected = [95, 65, 35, 95, 65, 35, 95, 65, 35, 95, 65, 35, 95, 65, 35, 95, 65, 35]
    colours = ['#f1eef6', '#d0d1e6', '#a6bddb', '#74a9cf','#2b8cbe','#045a8d']

    fig, (ax) = plt.subplots(1, 3)
    fig.set_size_inches(15, 7.5)

    for id, i in enumerate(list1):
        print(i, ": ")
        for idx, j in enumerate(list2):
            statement = '''
            SELECT ROUND(time_diff) as time_diff, COUNT(*)
            FROM matches_{0}
            INNER JOIN deposit_transactions
            on matches_{0}.inc_address = deposit_transactions.inc_address
            AND matches_{0}.txid = deposit_transactions.txid
            AND deposit_transactions.match_usdt_{1} = {3}
            AND deposit_transactions.match_eth_{1} = {4}
            AND deposit_transactions.match_usdc_{1} = {5}
            AND time_diff <= {2}
            GROUP BY matches_{0}.inc_address,matches_{0}.txid, time_diff
            '''.format(i, j, list3[idx], list4[id][0], list4[id][1], list4[id][2])


            statement2 = '''
            SELECT AVG(time_diff) as time_diff, tran_id
            FROM matches_{0}
            INNER JOIN deposit_transactions
            on matches_{0}.inc_address = deposit_transactions.inc_address
            AND matches_{0}.txid = deposit_transactions.txid
            AND deposit_transactions.match_usdt_{1} = {3}
            AND deposit_transactions.match_eth_{1} = {4}
            AND deposit_transactions.match_usdc_{1} = {5}
            AND time_diff <= {2}
            {7}
            INNER JOIN hitbtc_trans_{0}
            on matches_{0}.tran_id = hitbtc_trans_{0}.id
            GROUP BY  tran_id
            HAVING count(*)=1;
            ;
            '''.format(i, j, list3[idx], list4[id][0], list4[id][1], list4[id][2], list7[id],list8[idx])


            #print(statement2)
            df = pds.read_sql(statement2, conn)

            df2= df.drop_duplicates(subset=['tran_id'])
            df2['time_diff']= df2.time_diff.apply(np.round)


            df3= df2["time_diff"].value_counts().to_frame()
            df3.reset_index(inplace=True)
            df3.sort_values(by=["index"], inplace=True)

            #print(df3)
            ax[id].scatter(df3["index"], df3["time_diff"], c=colours[idx],label= list6[idx])
            a, b = np.polyfit(df3["index"].iloc[1:-1],df3["time_diff"].iloc[1:-1],1)
            ax[id].plot(df3["index"], a*df3["index"]+b,linestyle='--')
            print(len(df2.index),'   y = ' + '{:.2f}'.format(b) + ' + {:.2f}'.format(a) + 'x')
        ax[id].legend()
        ax[id].set_xlabel("Time Difference in Minutes")
        ax[id].set_title(list5[id])
        ax[id].set_yscale('log')



    ax[0].set_ylabel("Number of Matches")
    plt.show()
    print("Done")



def time():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement_deposit= ''' SELECT extract(hour from time) FROM deposit_transactions'''
    df_deposit = pds.read_sql(statement_deposit, conn)
    df2_deposit = df_deposit["date_part"].value_counts().to_frame()
    df2_deposit['date_part'] = (df2_deposit['date_part'] / sum(df2_deposit['date_part'])) * 100
    df2_deposit.reset_index(inplace=True)
    df2_deposit.sort_values(by=["index"], inplace=True)


    statement_usdt = ''' SELECT extract(hour from timestamp) FROM hitbtc_trans_usdt WHERE side='sell' '''
    df_usdt = pds.read_sql(statement_usdt, conn)
    df2_usdt = df_usdt["date_part"].value_counts().to_frame()
    df2_usdt['date_part'] = (df2_usdt['date_part'] / sum(df2_usdt['date_part'])) * 100
    df2_usdt.reset_index(inplace=True)
    df2_usdt.sort_values(by=["index"], inplace=True)

    statement_eth = ''' SELECT extract(hour from timestamp) FROM hitbtc_trans_eth WHERE side='buy' '''
    df_eth = pds.read_sql(statement_eth, conn)
    df2_eth = df_eth["date_part"].value_counts().to_frame()
    df2_eth['date_part'] = (df2_eth['date_part'] / sum(df2_eth['date_part'])) * 100
    df2_eth.reset_index(inplace=True)
    df2_eth.sort_values(by=["index"], inplace=True)

    statement_usdc= '''  SELECT extract(hour from timestamp) FROM hitbtc_trans_usdc WHERE side='sell' '''
    df_usdc = pds.read_sql(statement_usdc, conn)
    df2_usdc = df_usdc["date_part"].value_counts().to_frame()
    df2_usdc['date_part'] = (df2_usdc['date_part'] / sum(df2_usdc['date_part'])) * 100
    df2_usdc.reset_index(inplace=True)
    df2_usdc.sort_values(by=["index"], inplace=True)

    fig, ax = plt.subplots()

    xnew_deposit = np.linspace(df2_deposit["index"].iloc[0], df2_deposit["index"].iloc[-1], 300)
    spl_deposit = make_interp_spline(df2_deposit["index"], df2_deposit["date_part"], k=3)
    power_smooth_deposit = spl_deposit(xnew_deposit)
    ax.plot(xnew_deposit, power_smooth_deposit, label="Dep", color='#43a2ca')

    xnew_usdt = np.linspace(df2_usdt["index"].iloc[0], df2_usdt["index"].iloc[-1], 300)
    spl_usdt = make_interp_spline(df2_usdt["index"], df2_usdt["date_part"], k=3)
    power_smooth_usdt = spl_usdt(xnew_usdt)
    ax.plot(xnew_usdt, power_smooth_usdt, label="USDT", color="#fee8c8")

    xnew_eth = np.linspace(df2_eth["index"].iloc[0], df2_eth["index"].iloc[-1], 300)
    spl_eth = make_interp_spline(df2_eth["index"], df2_eth["date_part"], k=3)
    power_smooth_eth = spl_eth(xnew_eth)
    ax.plot(xnew_eth, power_smooth_eth, label="ETH", color="#fdbb84")

    xnew_usdc = np.linspace(df2_usdc["index"].iloc[0], df2_usdc["index"].iloc[-1], 300)
    spl_usdc  = make_interp_spline(df2_usdc["index"], df2_usdc["date_part"], k=3)
    power_smooth_usdc  = spl_usdc (xnew_usdc )
    ax.plot(xnew_usdc ,power_smooth_usdc, label="USDC", color="#e34a33" )

    # ax.plot(df2_deposit["index"], df2_deposit["date_part"], label="Dep", color='#43a2ca')
    # ax.plot(df2_usdt["index"], df2_usdt["date_part"], label="USDT", color="#fee8c8")
    # ax.plot(df2_eth["index"], df2_eth["date_part"], label="ETH", color="#fdbb84")
    # ax.plot(df2_usdc["index"], df2_usdc["date_part"], label="USDC", color="#e34a33")

    ax.xaxis.set_major_locator(MultipleLocator(2))
    #plt.xaxis.set_minor_locator(AutoMinorLocator(2))
    ax.set_ylabel("Number of Matches in Percent")
    ax.set_xlabel("Hour")
    plt.legend()
    plt.show()

########## IN WORK ######################

# Table 5.4
def matches_alone():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    pd.options.mode.chained_assignment = None
    list1 = ['usdt', 'eth', 'usdc']
    list2 = ['3_2', '2_2', '1_2', '3_0', '2_0', '1_0']
    list6 = ['3 & 2', '2 & 2', '1 & 2', '3 & 0', '2 & 0', '1 & 0']
    list3 = ['sell', 'buy', 'sell']
    list4 = ['qty', 'trade_size_btc', 'qty']

    beta= ['','','','AND dep_qty = tran_qty','AND dep_qty = tran_qty','AND dep_qty = tran_qty']
    time_diff=[180,120,60,180,120,60]
    for id, currency in enumerate(list1):
        statement2 = '''SELECT {2} as qty, COUNT(*) as hitbtc_qty_count FROM "hitbtc_trans_{0}" WHERE side= '{1}' 
        GROUP BY {2} 
        HAVING COUNT(*)>0'''.format(currency, list3[id], list4[id])
        df2 = pds.read_sql(statement2, conn)
        print(f"{len(df2.index):,}")

        for idi,j in enumerate(list2):
            statement = '''SELECT dep_qty,COUNT(*) FROM "matches_{0}"  WHERE time_diff <= {2} {3} GROUP BY 
            dep_qty'''.format(
                currency, j,time_diff[idi], beta[idi])
            df = pds.read_sql(statement, conn, )

            statement3 = '''SELECT tran_qty,COUNT(*) FROM "matches_{0}"  WHERE time_diff <= {2} {3} GROUP BY 
            tran_qty'''.format(
                currency, j,time_diff[idi], beta[idi])
            df3 = pds.read_sql(statement3, conn, )


            print("&","&",list6[idi],  "&", f"{len(df3.index):,}",  "&", f"{len(df.index):,}", r" \\")


# Totally Alone
def aloner():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    pd.options.mode.chained_assignment = None
    list1 = ['usdt', 'eth', 'usdc']
    list2 = ['3_2', '2_2', '1_2', '3_0', '2_0', '1_0']
    counter_balance = ['2_2','3_2',  '1_2', '3_0', '2_0', '1_0']
    list6 = ['3 & 2', '2 & 2', '1 & 2', '3 & 0', '2 & 0', '1 & 0']
    list3 = ['sell', 'buy', 'sell']
    list4 = ['qty', 'ROUND(trade_size_btc,4)', 'qty']

    for id, currency in enumerate(list1):
        print(currency)
        for idi,j in enumerate(list2):
            statement = '''SELECT Count(*) FROM deposit_transactions WHERE match_{0}_{3}= 1 and match_{0}_{4} =0 and 
            match_{0}_{5} =0 and match_{0}_{6} =0 and match_{0}_{7} =0 and match_{0}_{8} =0 and 
            match_{1}_3_2= 0 and 
            match_{1}_2_2 =0 and 
            match_{1}_1_2 =0 and match_{1}_3_0 =0 and match_{1}_2_0 =0 and match_{1}_1_0 =0 and match_{2}_3_2= 0 and 
            match_{2}_2_2 =0 and 
            match_{2}_1_2 =0 and match_{2}_3_0 =0 and match_{2}_2_0 =0 and match_{2}_1_0 =0 '''.format(list1[id%3],
                                                                                                       list1[(id+1)%3],
                                                                                                       list1[(id+2)%3],
                                                                                                       list2[(idi)%6],
                                                                                                       list2[(idi+1)%6],
                                                                                                       list2[(idi+2)%6],
                                                                                                       list2[(idi+3)%6],
                                                                                                       list2[(idi+4)%6],
                                                                                                       list2[(idi+5)%6])
            print(statement)
            df = pds.read_sql(statement, conn)
            print(list6[idi],df['count'][0])



# Dont know yet
def plotNrOfMatches():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement_usdt = '''SELECT real_conn_with_host, count(*) from deposit_address GROUP BY real_conn_with_host'''

    df = pds.read_sql(statement_usdt, conn)

    labels = ['0', '1', '2', '3', '4', '5']
    x = np.arange(len(labels))

    fig, (ax1, ax2) = plt.subplots(1, 2, sharey=True)

    nr_match_1_0 = [1927030, 60371, 24318, 14049, 9869, 7285]
    nr_match_2_0 = [1867552, 69265, 31376, 18640, 12453, 9300]
    nr_match_3_0 = [1831956, 71774, 35166, 21304, 14519, 11025]
    nr_match_1_2 = [1114174, 358233, 198593, 112279, 69403, 45792]
    nr_match_2_2 = [826978, 284921, 213332, 153337, 110900, 81872]
    nr_match_3_2 = [706800, 212976, 187772, 151682, 121658, 96476]

    ax1.bar(x - 0.3, nr_match_1_0, width=0.3, label=r'$\alpha_1,  \beta_0$', align='center', color='#FC766AFF')
    ax1.bar(x, nr_match_2_0, width=0.3, label='2 hours, 0%', align='center', color='#B0B8B4FF')
    ax1.bar(x + 0.3, nr_match_3_0, width=0.3, label='3 hours, 0%', align='center', color='#184A45FF')

    ax1.set_ylabel('Count of matches')
    ax1.set_yscale('log')
    ax1.set_xlabel('Numer of matches found')
    ax1.legend()

    ax2.bar(x - 0.3, nr_match_1_2, width=0.3, label=' 1 hour , 2%', align='center', color='#FC766AFF')
    ax2.bar(x, nr_match_2_2, width=0.3, label='2 hours, 2%', align='center', color='#B0B8B4FF')
    ax2.bar(x + 0.3, nr_match_3_2, width=0.3, label='3 hours, 2%', align='center', color='#184A45FF')
    ax2.legend()

    plt.xlabel('Number of matches found')

    ax2.set_yscale('log')
    plt.show()


# Was with left join, now outer might need to fix more here
def df_tran_sum_graph_all():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    ##############################################################################################
    x = 0.3;
    ###############################################################################################
    statement_usdt = '''SELECT qty,SUM(match_usdt_1_2) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
                   SUM(match_usdt_1_2) >0 '''
    df_usdt = pds.read_sql(statement_usdt, conn)
    statement2_usdt = '''SELECT qty, COUNT(*) as count_e FROM "hitbtc_trans_usdt" WHERE side= 'sell' GROUP BY qty 
    HAVING COUNT(*)>0'''
    df2_usdt = pds.read_sql(statement2_usdt, conn)

    merged_df_usdt = df2_usdt.merge(df_usdt, how='outer', on=['qty'])

    df3_usdt = merged_df_usdt[merged_df_usdt['sum_e'].notna()]
    df3_usdt = df3_usdt.reset_index()

    df3_usdt['count_e'] = (df3_usdt['count_e'] / df3_usdt['count_e'].sum()) * 100
    df3_usdt['sum_e'] = (df3_usdt['sum_e'] / df3_usdt['sum_e'].sum()) * 100
    df3_usdt.drop('index', axis=1, inplace=True)

    df3_usdt['diff_count_sum'] = df3_usdt.sum_e - df3_usdt.count_e

    # CHANGE THIS BELOW FOR EVERY VALUE
    # USDT 0.3 USDC 1 ETH
    df3_usdt.drop(df3_usdt[df3_usdt.qty > x].index, inplace=True)
    df3_usdt.sort_values(by=['qty'], inplace=True)
    ###############################################################################################
    statement_eth = '''SELECT qty,SUM(match_eth_1_2) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
                   SUM(match_eth_1_2) >0 '''
    df_eth = pds.read_sql(statement_eth, conn)
    statement2_eth = '''SELECT trade_size_btc as qty, COUNT(*) as count_e FROM "hitbtc_trans_eth" WHERE side= 'buy' 
    GROUP BY 
    trade_size_btc HAVING 
    COUNT(*)>0'''
    df2_eth = pds.read_sql(statement2_eth, conn)

    merged_df_eth = df2_eth.merge(df_eth, how='outer', on=['qty'])

    df3_eth = merged_df_eth[merged_df_eth['sum_e'].notna()]
    df3_eth = df3_eth.reset_index()

    df3_eth['count_e'] = (df3_eth['count_e'] / df3_eth['count_e'].sum()) * 100
    df3_eth['sum_e'] = (df3_eth['sum_e'] / df3_eth['sum_e'].sum()) * 100
    df3_eth.drop('index', axis=1, inplace=True)

    df3_eth['diff_count_sum'] = df3_eth.sum_e - df3_eth.count_e

    # CHANGE THIS BELOW FOR EVERY VALUE
    # USDT 0.3 USDC 1 ETH
    df3_eth.drop(df3_eth[df3_eth.qty > x].index, inplace=True)
    df3_eth.sort_values(by=['qty'], inplace=True)

    ###############################################################################################
    statement_usdc = '''SELECT qty,SUM(match_usdc_1_2) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
                   SUM(match_usdc_1_2) >0 '''
    df_usdc = pds.read_sql(statement_usdc, conn)
    statement2_usdc = '''SELECT qty, COUNT(*) as count_e FROM "hitbtc_trans_usdc" WHERE side= 'sell' GROUP BY qty HAVING 
    COUNT(*)>0'''
    df2_usdc = pds.read_sql(statement2_usdc, conn)

    merged_df_usdc = df2_usdc.merge(df_usdc, how='left', on=['qty'])

    df3_usdc = merged_df_usdc[merged_df_usdc['outer'].notna()]
    df3_usdc = df3_usdc.reset_index()

    df3_usdc['count_e'] = (df3_usdc['count_e'] / df3_usdc['count_e'].sum()) * 100
    df3_usdc['sum_e'] = (df3_usdc['sum_e'] / df3_usdc['sum_e'].sum()) * 100
    df3_usdc.drop('index', axis=1, inplace=True)

    df3_usdc['diff_count_sum'] = df3_usdc.sum_e - df3_usdc.count_e

    # CHANGE THIS BELOW FOR EVERY VALUE
    # USDT 0.3 USDC 1 ETH
    df3_usdc.drop(df3_usdc[df3_usdc.qty > x].index, inplace=True)
    df3_usdc.qty = df3_usdc.qty.astype(float)
    df3_usdc.sort_values(by=['qty'], inplace=True)
    ###############################################################################################

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, sharey=True)
    fig.set_size_inches(15, 7.5)

    ax1.plot(df3_usdt['qty'], df3_usdt['diff_count_sum'])
    ax1.axhline(y=0, color='r', linewidth=0.7)
    ax1.set_title('USDT')
    ax1.set_ylabel('Percentage Difference')
    ax1.set_xlabel('qty')

    ax2.plot(df3_eth['qty'], df3_eth['diff_count_sum'])
    ax2.axhline(y=0, color='r', linewidth=0.7)
    ax2.set_title('ETH')
    ax2.set_xlabel('qty')

    ax3.plot(df3_usdc['qty'], df3_usdc['diff_count_sum'])
    ax3.axhline(y=0, color='r', linewidth=0.7)
    ax3.set_title('USDC')
    ax3.set_xlabel('qty')

    # CHANGE threshhold
    df3_usdt.drop(df3_usdt[abs(df3_usdt.diff_count_sum) < 2.5].index, inplace=True)
    df3_eth.drop(df3_eth[abs(df3_eth.diff_count_sum) < 2.5].index, inplace=True)
    df3_usdc.drop(df3_usdc[abs(df3_usdc.diff_count_sum) < 2.5].index, inplace=True)
    df3_usdt = df3_usdt.reset_index()
    df3_eth = df3_eth.reset_index()
    df3_usdc = df3_usdc.reset_index()

    for i in range(len(df3_usdt.index)):
        if (df3_usdt.at[i, 'qty'] < 0.0001):
            ax1.text(df3_usdt.at[i, 'qty'], df3_usdt.at[i, 'diff_count_sum'], format(df3_usdt.at[i, 'qty'], ".5f"),
                     size=12)
        else:
            ax1.text(df3_usdt.at[i, 'qty'], df3_usdt.at[i, 'diff_count_sum'], df3_usdt.at[i, 'qty'], size=12)
    for i in range(len(df3_eth.index)):
        if (df3_eth.at[i, 'qty'] < 0.0001):
            ax2.text(df3_eth.at[i, 'qty'], df3_eth.at[i, 'diff_count_sum'], format(df3_eth.at[i, 'qty'], ".5f"),
                     size=12)
        else:
            ax2.text(df3_eth.at[i, 'qty'], df3_eth.at[i, 'diff_count_sum'], df3_eth.at[i, 'qty'], size=12)
    for i in range(len(df3_usdc.index)):
        if (df3_usdc.at[i, 'qty'] < 0.0001):
            ax3.text(df3_usdc.at[i, 'qty'], df3_usdc.at[i, 'diff_count_sum'], format(df3_usdc.at[i, 'qty'], ".5f"),
                     size=12)
        else:
            ax3.text(df3_usdc.at[i, 'qty'], df3_usdc.at[i, 'diff_count_sum'], df3_usdc.at[i, 'qty'],
                     size=12)
    plt.show()


def occurance_parameter_2_vs_02():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    list1 = ['usdt', 'eth', 'usdc']
    list12= ['USDT', 'ETH', 'USDC']
    list2 = ['1_2', '1_0' ] # ['3_2', '3_0' ]  ['2_2', '2_0' ]
    list3 = [60, 60] #  [180, 180] [120, 120]
    list4 = [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
    list8= ['','AND tran_qty= dep_qty']
    color= ["#fee8c8","#fdbb84","#e34a33"]

    fig, axs= plt.subplots(3,2)
    fig.set_size_inches(15, 10)

    for id, i in enumerate(list1):
        print(i, ": ")
        for idx, j in enumerate(list2):
            statement = '''
            SELECT avg(deposit_transactions.qty) as qty, tran_id, COUNT(*)
            FROM matches_{0}
            INNER JOIN deposit_transactions
            on matches_{0}.inc_address = deposit_transactions.inc_address
            AND matches_{0}.txid = deposit_transactions.txid
            AND deposit_transactions.match_usdt_{1} = {3}
            AND deposit_transactions.match_eth_{1} = {4}
            AND deposit_transactions.match_usdc_{1} = {5}
            AND time_diff <= {2}
            {6}
            INNER JOIN hitbtc_trans_{0}
            on matches_{0}.tran_id = hitbtc_trans_{0}.id
            GROUP BY tran_id
            HAVING count(*)=1
            ;
            '''.format(i, j, list3[idx], list4[id][0], list4[id][1], list4[id][2], list8[idx])

            #print(statement)
            df = pds.read_sql(statement, conn)

            df2 = df['qty'].value_counts(normalize=True) * 100
            df2.sort_index(inplace=True)

            # df= df_usdc2[df_usdc2.qty <= 1]
            #df.sort_values(by=['qty'], inplace=True)

            axs[id][idx].plot(df2.index.values, df2.values,color=color[id], marker='D', label=(list12[id]+" "+list2[
                idx]))
            axs[id][idx].legend()

            #axs[1,0].set_xlim(0,1)
            #axs[1,0].xaxis.set_major_locator(MultipleLocator(0.1))
            #axs[1,0].xaxis.set_minor_locator(AutoMinorLocator(2))
            axs[id,idx].grid(which='major', axis='x', linestyle='--')
            #axs[id,idx].grid(which='minor', axis='x', linestyle=':')



            axs[id][idx].set_yscale('log')

    axs[2, 1].set_xlabel('Quantity')
    axs[2, 0].set_xlabel('Quantity')
    axs[2, 0].set_ylabel('% of all Transactions')
    axs[1, 0].set_ylabel('% of all Transactions')
    axs[0, 0].set_ylabel('% of all Transactions')

    plt.show()
    print("Done")



#benfords law on usdt 20169 vs 2021
def benfords_law3():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    statement = '''SELECT qty FROM hitbtc_trans_usdt WHERE side='sell' AND timestamp between '2016-01-01' AND 
    '2017-01-01' '''

    statement2 = '''SELECT qty FROM hitbtc_trans_usdt WHERE side='sell' AND timestamp between '2017-01-01' AND 
    '2018-01-01' '''  # Matches > 0

    statement3 = '''SELECT qty FROM hitbtc_trans_usdt WHERE side='sell' AND timestamp between '2018-01-01' AND 
    '2019-01-01' '''

    statement4 = '''SELECT qty FROM hitbtc_trans_usdt WHERE side='sell' AND timestamp between '2019-01-01' AND 
    '2020-01-01' '''

    statement5 = '''SELECT qty FROM hitbtc_trans_usdt WHERE side='sell' AND timestamp between '2020-01-01' AND 
    '2021-01-01' '''  # Matches > 0

    statement6 = '''SELECT qty FROM hitbtc_trans_usdt WHERE side='sell' AND timestamp between '2021-01-01' AND 
    '2022-01-01' '''



    df = pds.read_sql(statement, conn)
    df['qty'] = (df['qty']) * 10000000
    df['leading_digit'] = df['qty'].astype(str).str[0].astype(int)
    df_f = df['leading_digit'].value_counts(normalize=True) * 100
    df_f.sort_index(inplace=True)
    print(df['leading_digit'])

    df2 = pds.read_sql(statement2, conn)
    df2['qty'] = (df2['qty']) * 10000000
    df2['leading_digit'] = df2['qty'].astype(str).str[0].astype(int)
    df_f2 = df2['leading_digit'].value_counts(normalize=True) * 100
    df_f2.sort_index(inplace=True)

    df3 = pds.read_sql(statement3, conn)
    df3['qty'] = (df3['qty']) * 10000000
    df3['leading_digit'] = df3['qty'].astype(str).str[0].astype(int)
    df_f3 = df3['leading_digit'].value_counts(normalize=True) * 100
    df_f3.sort_index(inplace=True)


    df4 = pds.read_sql(statement4, conn)
    df4['qty'] = (df4['qty']) * 10000000
    df4['leading_digit'] = df4['qty'].astype(str).str[0].astype(int)
    df_f4 = df4['leading_digit'].value_counts(normalize=True) * 100
    df_f4.sort_index(inplace=True)

    df5 = pds.read_sql(statement5, conn)
    df5['qty'] = (df5['qty']) * 10000000
    df5['leading_digit'] = df5['qty'].astype(str).str[0].astype(int)
    df_f5 = df5['leading_digit'].value_counts(normalize=True) * 100
    df_f5.sort_index(inplace=True)

    df6 = pds.read_sql(statement6, conn)
    df6['qty'] = (df6['qty']) * 10000000
    df6['leading_digit'] = df6['qty'].astype(str).str[0].astype(int)
    df_f6 = df6['leading_digit'].value_counts(normalize=True) * 100
    df_f6.sort_index(inplace=True)



    df_b = pd.DataFrame({'Benford´s Law': [30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]})

    fig, axs = plt.subplots(2, 3, sharey=True)
    fig.set_size_inches(15, 7.5)

    axs[0,0].bar(df_f.index.values, df_f.values, color='#fee8c8')
    axs[0,0].plot(df_f.index.values, df_b.values, marker='o', color='#215b3a', label='Benford\'s Law')
    axs[0,0].set_ylabel("Probability")
    axs[0,0].set_title("2016")
    axs[0,0].legend()

    axs[0,1].bar(df_f2.index.values, df_f2.values, color='#fdbb84')
    axs[0,1].plot(df_f2.index.values, df_b.values, marker='o', color='#215b3a', label='Benford\'s Law')
    axs[0,1].set_title("2017")
    axs[0,1].legend()

    axs[0,2].bar(df_f3.index.values, df_f3.values, color='#e34a33')
    axs[0,2].plot(df_f2.index.values, df_b.values, marker='o', color='#215b3a', label='Benford\'s Law')
    axs[0,2].set_title("2018")
    axs[0,2].legend()

    ########
    axs[1,0].bar(df_f.index.values, df_f4.values, color='#fee8c8')
    axs[1,0].plot(df_f.index.values, df_b.values, marker='o', color='#215b3a', label='Benford\'s Law')
    axs[1,0].set_xlabel("Leading Digit (qty)")
    axs[1,0].set_ylabel("Probability")
    axs[1,0].set_title("2019")
    axs[1,0].legend()

    axs[1,1].bar(df_f2.index.values, df_f5.values, color='#fdbb84')
    axs[1,1].plot(df_f2.index.values, df_b.values, marker='o', color='#215b3a', label='Benford\'s Law')
    axs[1,1].set_xlabel("Leading Digit (btc_qty)")
    axs[1,1].set_title("2020")
    axs[1,1].legend()

    axs[1,2].bar(df_f3.index.values, df_f6.values, color='#e34a33')
    axs[1,2].plot(df_f2.index.values, df_b.values, marker='o', color='#215b3a', label='Benford\'s Law')
    axs[1,2].set_xlabel("Leading Digit (qty)")
    axs[1,2].set_title("2021")
    axs[1,2].legend()



    print(chisquare(df_f.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    print(chisquare(df_f2.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    print(chisquare(df_f3.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    print(chisquare(df_f4.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    print(chisquare(df_f5.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    print(chisquare(df_f6.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    plt.show()



def plot_grwoth():
    month= [1,2,3]
    usdt_2 = [0.18,0.14,0.12]
    usdt_0 = [0.44,0.34,0.29]
    eth_fitted = [0,0,0]
    usdc_fitted = [0,0,0]

    eth_unfitted = [177058,176060,175518]
    usdc_unfitted = [84948,84385,84162.9]

    fig, (ax1,ax2)= plt.subplots(1,2)
    fig.set_size_inches(14,6)

    ax1.plot(month,usdt_2,  label="USDT", color='#fee8c8',  marker='o')
    ax1.plot(month,eth_fitted,  label="ETH", color='#fdbb84',marker='o')
    ax1.plot(month,usdc_fitted,  label="USDC", color='#e34a33',  marker='o')

    ax1.set_xlabel('Weekday')
    ax1.set_title('Fitted')
    ax1.set_ylabel('Percent')
    ax1.legend()



    ax1.plot(month,usdt_0,  label="USDT", color='#e2f5f9',  marker='o')
    ax2.plot(month,eth_unfitted,  label="ETH", color='#99d8c9',  marker='o')
    ax2.plot(month,usdc_unfitted,  label="USDC", color='#2ca25f',  marker='o')

    ax2.set_title('Unfitted')
    ax2.set_xlabel('Weekday')
    ax2.set_yscale('log')
    ax2.legend()

    plt.legend()
    plt.show()


def plot_API():
    df = pd.read_csv(r'/src\Additonal_Data\plot-data-API.csv')
    df2 = pd.read_csv(r'/src\Additonal_Data\plot-data-client.csv')
    df3 = pd.read_csv(r'/src\Additonal_Data\plot-data-neo4j.csv')
    print(df.columns)
    print(df["y"])
    plt.plot(df["y"], color= "#de2d26", label= "API")
    plt.plot(df2["y"], color= "#fc9272", label= "core")
    plt.plot(df3["y"], color= "#fee0d2", label = "Neo4j")
    plt.ylabel("PostgreSQL Update")
    plt.xlabel("Seconds")
    plt.legend()
    plt.show()



########## IN WORK ######################




######## SIMPLER VERSION FOR ONLY ONE OR NO GRAPH JUST DATA ##############
def benfords_law():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    # statement = '''SELECT qty FROM "hitbtc_trans_usdt" WHERE side='sell' '''
    # statement = '''SELECT qty FROM "deposit_transactions" '''
    statement = '''SELECT qty as qty FROM "deposit_transactions" '''
    df = pds.read_sql(statement, conn)
    df['qty'] = (df['qty']) * 1000000
    df['leading_digit'] = df['qty'].astype(str).str[0].astype(int)
    df_f = df['leading_digit'].value_counts(normalize=True) * 100
    df_f.sort_index(inplace=True)
    print(df_f)

    ax = df_f.plot.bar(color='#4d6910')
    df_b = pd.DataFrame({'Benford´s Law': [30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]})
    df_b.plot(ax=ax, marker='o', color='#ffa600', label='')
    plt.ylabel("Probability")
    plt.xlabel("Leading Digit")
    plt.show()

    print(chisquare(df_f.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))

# For the qty x, there are y transactions in hitbtc_trans_x and in real_deposit_transaction total matches found GRAPH
def df_tran_sum_graph_single():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement = '''SELECT qty,SUM(match_eth_1_2) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
                   SUM(match_eth_1_2) >0 '''
    dataFrame = pds.read_sql(statement, conn)
    statement2 = '''SELECT trade_size_btc as qty, COUNT(*) as count_e FROM "hitbtc_trans_eth" WHERE side= 'buy' GROUP BY 
    trade_size_btc
    HAVING 
    COUNT(*)>0'''
    dataFrame2 = pds.read_sql(statement2, conn)

    merged_df = dataFrame2.merge(dataFrame, how='left', on=['qty'])

    less_df = merged_df[merged_df['sum_e'].notna()]
    less_df = less_df.reset_index()

    less_df['count_e'] = (less_df['count_e'] / less_df['count_e'].sum()) * 100
    less_df['sum_e'] = (less_df['sum_e'] / less_df['sum_e'].sum()) * 100
    less_df.drop('index', axis=1, inplace=True)


    less_df['diff_count_sum'] = less_df.sum_e - less_df.count_e

    print(less_df)
    print(less_df.sort_values(by=['count_e']))
    print(less_df.sort_values(by=['sum_e']))
    print(less_df.sort_values(by=['diff_count_sum']))

    # CHANGE Only QTY difference below
    #less_df.drop(less_df[less_df.qty > 1].index, inplace=True)

    # CHANGE threshhold Show Only Divergence after x
    less_df.drop(less_df[abs(less_df.diff_count_sum) < 2.5].index, inplace=True)
    less_df = less_df.reset_index()

    print(less_df)

############# DONT KNOW WHY ITS STILL HERE ###################

def df_tran_sum_graph_all2():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    ##############################################################################################
    x = 2.5;
    ###############################################################################################
    statement_usdt = '''SELECT qty,count(*) as deposit_transactions FROM "deposit_transactions" GROUP BY qty 
    HAVING count(*) >0; '''
    df_usdt = pds.read_sql(statement_usdt, conn)
    statement2_usdt = '''SELECT qty, COUNT(*) as hitbtc FROM "hitbtc_trans_usdt" WHERE side= 'sell' GROUP BY qty 
    HAVING COUNT(*)>0'''
    df2_usdt = pds.read_sql(statement2_usdt, conn)

    merged_df_usdt = df2_usdt.merge(df_usdt, how='outer', on=['qty'])

    df3_usdt = merged_df_usdt[merged_df_usdt['deposit_transactions'].notna()]
    df3_usdt = df3_usdt.reset_index()

    df3_usdt['hitbtc'] = (df3_usdt['hitbtc'] / df3_usdt['hitbtc'].sum()) * 100
    df3_usdt['deposit_transactions'] = (df3_usdt['deposit_transactions'] / df3_usdt['deposit_transactions'].sum() ) * 100
    df3_usdt.drop('index', axis=1, inplace=True)

    df3_usdt['diff_count_sum'] = df3_usdt.deposit_transactions - df3_usdt.hitbtc

    # CHANGE THIS BELOW FOR EVERY VALUE
    # USDT 0.3 USDC 1 ETH
    df3_usdt.drop(df3_usdt[df3_usdt.qty > x].index, inplace=True)
    df3_usdt.sort_values(by=['qty'], inplace=True)
    ###############################################################################################
    statement_eth = '''SELECT qty,Count(*) as deposit_transactions FROM "deposit_transactions" WHERE match_usdt_1_0 
    >0 GROUP BY qty 
    HAVING
                   count(*) >0;'''
    df_eth = pds.read_sql(statement_eth, conn)
    statement2_eth = '''SELECT  qty, COUNT(*) as hitbtc FROM "hitbtc_trans_usdt" WHERE side= 'sell' 
    GROUP BY 
    qty HAVING 
    COUNT(*)>0'''
    df2_eth = pds.read_sql(statement2_eth, conn)

    merged_df_eth = df2_eth.merge(df_eth, how='outer', on=['qty'])

    df3_eth = merged_df_eth[merged_df_eth['deposit_transactions'].notna()]
    df3_eth = df3_eth.reset_index()

    df3_eth['hitbtc'] = (df3_eth['hitbtc'] / df3_eth['hitbtc'].sum()) * 100
    df3_eth['deposit_transactions'] = (df3_eth['deposit_transactions'] / df3_eth['deposit_transactions'].sum()) * 100
    df3_eth.drop('index', axis=1, inplace=True)

    df3_eth['diff_count_sum'] = df3_eth.deposit_transactions - df3_eth.hitbtc

    # CHANGE THIS BELOW FOR EVERY VALUE
    # USDT 0.3 USDC 1 ETH
    df3_eth.drop(df3_eth[df3_eth.qty > x].index, inplace=True)
    df3_eth.sort_values(by=['qty'], inplace=True)

    ###############################################################################################
    statement_usdc = '''SELECT qty,Count(*) as deposit_transactions FROM "deposit_transactions" WHERE match_usdt_1_0 
    =1 GROUP BY qty HAVING
                   count(*) >0; '''
    df_usdc = pds.read_sql(statement_usdc, conn)
    statement2_usdc = '''SELECT qty, COUNT(*) as hitbtc FROM "hitbtc_trans_usdt" WHERE side= 'sell' GROUP BY qty 
    HAVING 
    COUNT(*)>0'''
    df2_usdc = pds.read_sql(statement2_usdc, conn)

    merged_df_usdc = df2_usdc.merge(df_usdc, how='outer', on=['qty'])

    df3_usdc = merged_df_usdc[merged_df_usdc['deposit_transactions'].notna()]
    df3_usdc = df3_usdc.reset_index()

    df3_usdc['hitbtc'] = (df3_usdc['hitbtc'] / df3_usdc['hitbtc'].sum()) * 100
    df3_usdc['deposit_transactions'] = (df3_usdc['deposit_transactions'] / df3_usdc['deposit_transactions'].sum()) * 100
    df3_usdc.drop('index', axis=1, inplace=True)

    df3_usdc['diff_count_sum'] = df3_usdc.deposit_transactions - df3_usdc.hitbtc

    # CHANGE THIS BELOW FOR EVERY VALUE
    # USDT 0.3 USDC 1 ETH
    df3_usdc.drop(df3_usdc[df3_usdc.qty > x].index, inplace=True)
    df3_usdc.qty = df3_usdc.qty.astype(float)
    df3_usdc.sort_values(by=['qty'], inplace=True)
    ###############################################################################################


    print(df3_usdc)
    print(sum(df3_usdc['hitbtc']))
    print(sum(df3_usdc['deposit_transactions']))
    print(sum(df3_usdc['diff_count_sum']))


    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, sharey=True)
    fig.set_size_inches(15, 7.5)

    ax1.plot(df3_usdt['qty'], df3_usdt['diff_count_sum'])
    ax1.axhline(y=0, color='r', linewidth=0.7)
    ax1.set_title(r'$\tau_{dep}$')
    ax1.set_ylabel('Percentage Difference')
    ax1.set_xlabel('qty')

    ax2.plot(df3_eth['qty'], df3_eth['diff_count_sum'])
    ax2.axhline(y=0, color='r', linewidth=0.7)
    ax2.set_title(r'$\tau_{dep} (m>0)$')
    ax2.set_xlabel('qty')

    ax3.plot(df3_usdc['qty'], df3_usdc['diff_count_sum'])
    ax3.axhline(y=0, color='r', linewidth=0.7)
    ax3.set_title(r'$\tau_{dep} (m=1)$')
    ax3.set_xlabel('qty')

    # CHANGE threshhold
    df3_usdt.drop(df3_usdt[abs(df3_usdt.diff_count_sum) < 2.5].index, inplace=True)
    df3_eth.drop(df3_eth[abs(df3_eth.diff_count_sum) < 2.5].index, inplace=True)
    df3_usdc.drop(df3_usdc[abs(df3_usdc.diff_count_sum) < 2.5].index, inplace=True)
    df3_usdt = df3_usdt.reset_index()
    df3_eth = df3_eth.reset_index()
    df3_usdc = df3_usdc.reset_index()

    for i in range(len(df3_usdt.index)):
        if (df3_usdt.at[i, 'qty'] < 0.0001):
            ax1.text(df3_usdt.at[i, 'qty'], df3_usdt.at[i, 'diff_count_sum'], format(df3_usdt.at[i, 'qty'], ".5f"),
                     size=12)
        else:
            ax1.text(df3_usdt.at[i, 'qty'], df3_usdt.at[i, 'diff_count_sum'], df3_usdt.at[i, 'qty'], size=12)
    for i in range(len(df3_eth.index)):
        if (df3_eth.at[i, 'qty'] < 0.0001):
            ax2.text(df3_eth.at[i, 'qty'], df3_eth.at[i, 'diff_count_sum'], format(df3_eth.at[i, 'qty'], ".5f"),
                     size=12)
        else:
            ax2.text(df3_eth.at[i, 'qty'], df3_eth.at[i, 'diff_count_sum'], df3_eth.at[i, 'qty'], size=12)
    for i in range(len(df3_usdc.index)):
        if (df3_usdc.at[i, 'qty'] < 0.0001):
            ax3.text(df3_usdc.at[i, 'qty'], df3_usdc.at[i, 'diff_count_sum'], format(df3_usdc.at[i, 'qty'], ".5f"),
                     size=12)
        else:
            ax3.text(df3_usdc.at[i, 'qty'], df3_usdc.at[i, 'diff_count_sum'], df3_usdc.at[i, 'qty'],
                     size=12)

    ax1.grid(axis='y')
    ax2.grid(axis='y')
    ax3.grid(axis='y')

    plt.show()



occurance_parameter_2_vs_02()
