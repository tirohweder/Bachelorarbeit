import ssl

import matplotlib
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import sql
import pandas as pds
import dask.dataframe as dd


# Number of matches
def plotmatches():
    labels = ['0', '1', '2', '3', '4', '5']
    x = np.arange(len(labels))

    fig, (ax, ab) = plt.subplots(1, 2)

    nr_match_1_0 = [1927030, 60371, 24318, 14049, 9869, 7285]
    nr_match_2_0 = [1867552, 69265, 31376, 18640, 12453, 9300]
    nr_match_3_0 = [1831956, 71774, 35166, 21304, 14519, 11025]
    nr_match_1_2 = [1114174, 358233, 198593, 112279, 69403, 45792]
    nr_match_2_2 = [826978, 284921, 213332, 153337, 110900, 81872]
    nr_match_3_2 = [706800, 212976, 187772, 151682, 121658, 96476]

    ax.bar(x - 0.3, nr_match_1_0, width=0.3, label='1 hour , 0%', align='center', color='#FC766AFF')
    ax.bar(x, nr_match_2_0, width=0.3, label='2 hours, 0%', align='center', color='#B0B8B4FF')
    ax.bar(x + 0.3, nr_match_3_0, width=0.3, label='3 hours, 0%', align='center', color='#184A45FF')

    ax.set_ylabel('Count of matches')
    ax.set_yscale('log')
    ax.set_xlabel('Numer of matches found')
    ax.legend()

    ab.bar(x - 0.3, nr_match_1_2, width=0.3, label=' 1 hour , 2%', align='center', color='#FC766AFF')
    ab.bar(x, nr_match_2_2, width=0.3, label='2 hours, 2%', align='center', color='#B0B8B4FF')
    ab.bar(x + 0.3, nr_match_3_2, width=0.3, label='3 hours, 2%', align='center', color='#184A45FF')
    ab.legend()

    plt.xlabel('Number of matches found')

    ab.set_yscale('log')
    plt.show()


def plotmatchesbyqty():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement = '''SELECT qty, COUNT(*) FROM "hitbtc_trans_busd" WHERE side= 'sell' GROUP BY qty'''

    dataFrame = pds.read_sql(statement, conn)
    dataFrame.plot(kind='scatter', x='qty', y='count')
    plt.yscale('log')
    plt.show()
    print(dataFrame)


def popop():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement = '''SELECT qty, COUNT(*) FROM "hitbtc_trans_busd" WHERE qty <=1 AND side= 'sell' GROUP BY qty HAVING 
               COUNT(*)> 380'''

    dataFrame = dd.read_sql(statement, conn)
    dataFrame.plot(kind='bar', x='qty', y='count')
    # plt.yscale('log')
    # ax.get_xaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(float(x))))
    # plt.ticklabel_format(style='plain')
    plt.show()

    print(dataFrame)


def plotmatchesbyqty2():
    # engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    # conn = engine.connect()

    statement = '''SELECT id, qty, COUNT(*) FROM "hitbtc_trans_busd" WHERE side= 'sell' GROUP BY qty,id'''

    dataFrame = dd.read_sql('hitbtc_trans_busd', 'postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede',
                            npartitions=10, index_col='id')
    dataFrame.plot(kind='scatter', x='qty', y='count')
    plt.yscale('log')
    plt.show()
    print(dataFrame)


def plotmatchesbyqty3():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement = '''SELECT qty, COUNT(*) FROM "hitbtc_trans_usdt" WHERE side= 'sell' GROUP BY qty HAVING COUNT(
    *)>412455'''

    dataFrame = pds.read_sql(statement, conn, columns=list('12345'))
    ax = dataFrame.plot(kind='bar', x='qty', y='count', figsize=(10, 5))
    # ax.margins(0.2)
    plt.subplots_adjust(bottom=0.2)
    plt.yscale('log')
    plt.show()
    print(dataFrame)


def plotmatchesbyqty4():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement = '''SELECT qty,SUM(nr_match_usdt3_2) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING SUM(nr_match_usdt3_2) >
                                                                                               178640 '''

    dataFrame = pds.read_sql(statement, conn)
    ax = dataFrame.plot(kind='bar', x='qty', y='sum_e', figsize=(10, 5))
    ax.margins(0.2)
    plt.subplots_adjust(bottom=0.2)
    plt.yscale('log')
    plt.show()
    print(dataFrame)


# Showing
def plotmatchesbyqty5():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement = '''SELECT qty,SUM(nr_match_usdt3_0) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
    SUM(nr_match_usdt3_2) >178640 '''
    dataFrame = pds.read_sql(statement, conn)

    statement2 = '''SELECT qty, COUNT(*) FROM "hitbtc_trans_usdt" WHERE side= 'sell' GROUP BY qty HAVING COUNT(
                    *)>412455'''
    dataFrame2 = pds.read_sql(statement2, conn)

    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(10, 5))
    dataFrame2.plot(kind='bar', x='qty', y='count', ax=ax1, subplots=True)
    dataFrame.plot(kind='bar', x='qty', y='sum_e', ax=ax2, subplots=True)

    plt.subplots_adjust(bottom=0.2)
    plt.yscale('log')
    plt.show()
    print(dataFrame)


# For the qty x, there are y transactions in hitbtc_trans_x and in real_deposit_transaction total matches found
def df_of_tran_sum():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement = '''SELECT qty,SUM(nr_match_usdt1_0) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
                   SUM(nr_match_usdt1_0) >0 '''
    dataFrame = pds.read_sql(statement, conn)
    statement2 = '''SELECT qty, COUNT(*) FROM "hitbtc_trans_usdt" WHERE side= 'sell' GROUP BY qty HAVING COUNT(
                    *)>0'''
    dataFrame2 = pds.read_sql(statement2, conn)

    merged_df = dataFrame2.merge(dataFrame, how='left', on=['qty'])

    less_df = merged_df[merged_df['sum_e'].notna()]
    less_df = less_df.reset_index()
    print("There are ", len(merged_df.index), " different transaction quantities, ", len(less_df.index))
    print(less_df)


# For the qty x, there are y transactions in hitbtc_trans_x and in real_deposit_transaction total matches found GRAPH
def df_tran_sum_graph():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement = '''SELECT qty,SUM(nr_match_usdt1_0) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
                   SUM(nr_match_usdt1_0) >0 '''
    dataFrame = pds.read_sql(statement, conn)
    statement2 = '''SELECT qty, COUNT(*) FROM "hitbtc_trans_usdt" WHERE side= 'sell' GROUP BY qty HAVING COUNT(
                    *)>0'''
    dataFrame2 = pds.read_sql(statement2, conn)

    merged_df = dataFrame2.merge(dataFrame, how='left', on=['qty'])

    less_df = merged_df[merged_df['sum_e'].notna()]
    less_df = less_df.reset_index()

    sum_count = less_df['count'].sum()
    sum_sum_e = less_df['sum_e'].sum()

    less_df['count'] = (less_df['count'] / sum_count) * 100
    less_df['sum_e'] = (less_df['sum_e'] / sum_sum_e) * 100
    print(less_df)
    print(less_df.sort_values(by=['count']))
    print(less_df.sort_values(by=['sum_e']))

    less_df.drop('index', axis=1, inplace=True)

    # CHANGE THIS BELOW FOR EVERY VALUE
    less_df.drop(less_df[less_df.qty > 2.5].index, inplace=True)

    less_df.plot(x='qty')
    plt.yscale('log')
    plt.show()


def table_printer():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    print("Sum Matches")
    statement = '''SELECT COUNT(*) FROM matches_eth '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 2 : ", f"{w['count']:,}")
    statement = '''SELECT COUNT(*) FROM matches_eth WHERE time_diff < 120'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 2 : ", f"{w['count']:,}")
    statement = '''SELECT COUNT(*) FROM matches_eth WHERE time_diff < 60'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 2 : ", f"{w['count']:,}")

    statement = '''SELECT COUNT(*) FROM matches_eth WHERE tran_qty = dep_qty'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 0 : ", f"{w['count']:,}")
    statement = '''SELECT COUNT(*) FROM matches_eth WHERE time_diff < 120 AND  tran_qty = dep_qty'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 0 : ", f"{w['count']:,}")
    statement = '''SELECT COUNT(*) FROM matches_eth WHERE time_diff < 60 AND  tran_qty = dep_qty'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 0 : ", f"{w['count']:,}")

    print("#Matches(>0)")
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_eth_3_2 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 2 : ", f"{w['count']:,}")
        m32 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_eth_2_2 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 2 : ", f"{w['count']:,}")
        m22 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_eth_1_2 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 2 : ", f"{w['count']:,}")
        m12 = w['count']

    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_eth_3_0 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 0 : ", f"{w['count']:,}")
        m30 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions  WHERE match_eth_2_0 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 0 : ", f"{w['count']:,}")
        m20 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_eth_1_0 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 0 : ", f"{w['count']:,}")
        m10 = w['count']

    print("%")
    # USDT/ETH 2117028
    # USDC 1457894
    total = 2117028
    print("3 2 : ", round((m32 / total) * 100, 2))
    print("2 2 : ", round((m22 / total) * 100, 2))
    print("1 2 : ", round((m12 / total) * 100, 2))
    print("3 0 : ", round((m30 / total) * 100, 2))
    print("2 0 : ", round((m20 / total) * 100, 2))
    print("1 0 : ", round((m10 / total) * 100, 2))

    print("Avg. Matches")
    statement = '''SELECT AVG(match_eth_3_2) FROM deposit_transactions WHERE match_eth_3_2 !=0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 2 : ", round(float(w['avg']),2))
    statement = '''SELECT AVG(match_eth_2_2) FROM deposit_transactions WHERE match_eth_2_2 !=0 '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 2 : ",round(float(w['avg']),2))
    statement = '''SELECT AVG(match_eth_1_2) FROM deposit_transactions WHERE match_eth_1_2 !=0 '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 2 : ", round(float(w['avg']),2))

    statement = '''SELECT AVG(match_eth_3_0) FROM deposit_transactions WHERE match_eth_3_0 !=0 '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 0 : ", round(float(w['avg']),2))
    statement = '''SELECT AVG(match_eth_2_0) FROM deposit_transactions WHERE match_eth_2_0 != 0 '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 0 : ", round(float(w['avg']),2))
    statement = '''SELECT AVG(match_eth_1_0) FROM deposit_transactions WHERE match_eth_1_0 !=0 '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 0 : ", round(float(w['avg']),2))

    print("Matches = 1")
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_eth_3_2 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 2 : ", f"{w['count']:,}")
        mm32 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_eth_2_2 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 2 : ", f"{w['count']:,}")
        mm22 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_eth_1_2 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 2 : ", f"{w['count']:,}")
        mm12 = w['count']

    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_eth_3_0 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 0 : ", f"{w['count']:,}")
        mm30 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions  WHERE match_eth_2_0 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 0 : ", f"{w['count']:,}")
        mm20 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_eth_1_0 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 0 : ", f"{w['count']:,}")
        mm10 = w['count']


    print("% x1")
    print("3 2 : ", round((mm32 / m32) * 100, 2))
    print("2 2 : ", round((mm22 / m22) * 100, 2))
    print("1 2 : ", round((mm12 / m12) * 100, 2))
    print("3 0 : ", round((mm30 / m30) * 100, 2))
    print("2 0 : ", round((mm20 / m20) * 100, 2))
    print("1 0 : ", round((mm10 / m10) * 100, 2))

    print("% x2")
    print("3 2 : ", round((mm32 / total) * 100, 2))
    print("2 2 : ", round((mm22 / total) * 100, 2))
    print("1 2 : ", round((mm12 / total) * 100, 2))
    print("3 0 : ", round((mm30 / total) * 100, 2))
    print("2 0 : ", round((mm20 / total) * 100, 2))
    print("1 0 : ", round((mm10 / total) * 100, 2))

# df_tran_sum_graph()
# plotmatches()
# plotmatchesbyqty5()
table_printer()
