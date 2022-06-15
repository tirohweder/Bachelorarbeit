import ssl

import matplotlib
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import sql
import pandas as pds
import dask.dataframe as dd


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

    statement = '''SELECT qty,SUM(nr_match_usdt3_2) as sum_e FROM "real_deposit_transactions" GROUP BY qty HAVING SUM(nr_match_usdt3_2) >
                                                                                               178640 '''

    dataFrame = pds.read_sql(statement, conn)
    ax = dataFrame.plot(kind='bar', x='qty', y='sum_e', figsize=(10, 5))
    ax.margins(0.2)
    plt.subplots_adjust(bottom=0.2)
    plt.yscale('log')
    plt.show()
    print(dataFrame)




#Mapping
def plotmatchesbyqty5():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()



    statement = '''SELECT qty,SUM(nr_match_usdt3_0) as sum_e FROM "real_deposit_transactions" GROUP BY qty HAVING 
    SUM(nr_match_usdt3_2) >
                                                                                               178640 '''
    dataFrame = pds.read_sql(statement, conn)

    statement2 = '''SELECT qty, COUNT(*) FROM "hitbtc_trans_usdt" WHERE side= 'sell' GROUP BY qty HAVING COUNT(
                    *)>412455'''
    dataFrame2 = pds.read_sql(statement2, conn)


    fig, (ax1,ax2) = plt.subplots(nrows=1, ncols=2, figsize=(10,5))
    dataFrame2.plot(kind='bar', x='qty', y='count',ax=ax1,subplots=True)
    dataFrame.plot(kind='bar', x='qty', y='sum_e',ax=ax2,subplots=True)




    plt.subplots_adjust(bottom=0.2)
    plt.yscale('log')
    plt.show()
    print(dataFrame)


#merging the 2 dataframes together
def plotmatchesbyqty6():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()



    statement = '''SELECT qty,SUM(nr_match_usdt1_2) as sum_e FROM "real_deposit_transactions" GROUP BY qty HAVING 
                   SUM(nr_match_usdt1_2) >0 '''
    dataFrame = pds.read_sql(statement, conn)

    statement2 = '''SELECT qty, COUNT(*) FROM "hitbtc_trans_usdt" WHERE side= 'sell' GROUP BY qty HAVING COUNT(
                    *)>0'''
    dataFrame2 = pds.read_sql(statement2, conn)

    merged_df = dataFrame2.merge(dataFrame, how= 'left', on =['qty'])

    less_df = merged_df[merged_df['sum_e'].notna()]
    less_df = less_df.reset_index()
    print("There are ",len(merged_df.index)," different transaction quantities, ", len(less_df.index))
    print(less_df)

plotmatchesbyqty6()
