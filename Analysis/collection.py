import decimal
import ssl
import matplotlib
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import sql
import pandas as pds
import dask.dataframe as dd
from scipy import stats
from scipy.stats import chisquare
from scipy.stats import chi2_contingency
from scipy.stats import chi2
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
import math
from bioinfokit.analys import stat, get_data

#Prints Table 4.x
def table_printer():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    # USDT/ETH 2117028 USDC 1457894 and to select and replace for USDT/ETH/USDC
    total = 1457894

    print("Sum Matches")
    statement = '''SELECT COUNT(*) FROM matches_usdc '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 2 : ", f"{w['count']:,}")
    statement = '''SELECT COUNT(*) FROM matches_usdc WHERE time_diff < 120'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 2 : ", f"{w['count']:,}")
    statement = '''SELECT COUNT(*) FROM matches_usdc WHERE time_diff < 60'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 2 : ", f"{w['count']:,}")

    statement = '''SELECT COUNT(*) FROM matches_usdc WHERE tran_qty = dep_qty'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 0 : ", f"{w['count']:,}")
    statement = '''SELECT COUNT(*) FROM matches_usdc WHERE time_diff < 120 AND  tran_qty = dep_qty'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 0 : ", f"{w['count']:,}")
    statement = '''SELECT COUNT(*) FROM matches_usdc WHERE time_diff < 60 AND  tran_qty = dep_qty'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 0 : ", f"{w['count']:,}")

    print("#Matches(>0)")
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_usdc_3_2 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 2 : ", f"{w['count']:,}")
        m32 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_usdc_2_2 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 2 : ", f"{w['count']:,}")
        m22 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_usdc_1_2 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 2 : ", f"{w['count']:,}")
        m12 = w['count']

    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_usdc_3_0 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 0 : ", f"{w['count']:,}")
        m30 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions  WHERE match_usdc_2_0 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 0 : ", f"{w['count']:,}")
        m20 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_usdc_1_0 != 0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 0 : ", f"{w['count']:,}")
        m10 = w['count']

    print("%")
    print("3 2 : ", round((m32 / total) * 100, 2))
    print("2 2 : ", round((m22 / total) * 100, 2))
    print("1 2 : ", round((m12 / total) * 100, 2))
    print("3 0 : ", round((m30 / total) * 100, 2))
    print("2 0 : ", round((m20 / total) * 100, 2))
    print("1 0 : ", round((m10 / total) * 100, 2))

    print("Avg. Matches")
    statement = '''SELECT AVG(match_usdc_3_2) FROM deposit_transactions WHERE match_usdc_3_2 !=0'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 2 : ", round(float(w['avg']),2))
    statement = '''SELECT AVG(match_usdc_2_2) FROM deposit_transactions WHERE match_usdc_2_2 !=0 '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 2 : ",round(float(w['avg']),2))
    statement = '''SELECT AVG(match_usdc_1_2) FROM deposit_transactions WHERE match_usdc_1_2 !=0 '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 2 : ", round(float(w['avg']),2))

    statement = '''SELECT AVG(match_usdc_3_0) FROM deposit_transactions WHERE match_usdc_3_0 !=0 '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 0 : ", round(float(w['avg']),2))
    statement = '''SELECT AVG(match_usdc_2_0) FROM deposit_transactions WHERE match_usdc_2_0 != 0 '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 0 : ", round(float(w['avg']),2))
    statement = '''SELECT AVG(match_usdc_1_0) FROM deposit_transactions WHERE match_usdc_1_0 !=0 '''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 0 : ", round(float(w['avg']),2))

    print("Matches = 1")
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_usdc_3_2 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 2 : ", f"{w['count']:,}")
        mm32 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_usdc_2_2 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 2 : ", f"{w['count']:,}")
        mm22 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_usdc_1_2 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("1 2 : ", f"{w['count']:,}")
        mm12 = w['count']

    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_usdc_3_0 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("3 0 : ", f"{w['count']:,}")
        mm30 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions  WHERE match_usdc_2_0 = 1'''
    nr_matches = conn.execute(statement)
    for w in nr_matches:
        print("2 0 : ", f"{w['count']:,}")
        mm20 = w['count']
    statement = '''SELECT COUNT(*) FROM deposit_transactions WHERE match_usdc_1_0 = 1'''
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

def table_avg_time():
    list1 = ['usdt', 'eth', 'usdc']
    list2 = ['3_2', '2_2', '1_2', '3_0', '2_0', '1_0']
    list3 = [180,120,60,180,120,60]
    list4 = [[1,0,0],[0,1,0],[0,0,1]]
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    for id,i in enumerate(list1):
        print(i, ": ")
        for idx, j in enumerate(list2):

            # For some reason with beta = 0, the select gives double entries, fixed by grouping unique and selecting
            # everything then. i dont know why and i dont care its fixed
            statement = '''
            SELECT AVG(te), COUNT(*)FROM(
            SELECT AVG(time_diff) as te, COUNT(*)
            FROM matches_{0}
            INNER JOIN deposit_transactions
            on matches_{0}.inc_address = deposit_transactions.inc_address
            AND matches_{0}.txid = deposit_transactions.txid
            AND deposit_transactions.match_usdt_{1} = {3}
            AND deposit_transactions.match_eth_{1} = {4}
            AND deposit_transactions.match_usdc_{1} = {5}
            AND
            time_diff <= {2}
            GROUP BY matches_{0}.inc_address,matches_{0}.txid) as cw
            ;
            '''.format(i,j,list3[idx], list4[id][0], list4[id][1],list4[id][2])


            df = pds.read_sql(statement, conn)
            print("           ", j, f"{df['count'][0]:,}", round((df['avg'][0]), 2))

def table_avg_time2():
    list1 = ['usdt', 'eth', 'usdc']
    list2 = ['3_2', '2_2', '1_2', '3_0', '2_0', '1_0']
    list3 = [180,120,60,180,120,60]
    list4 = [[1,0,0],[0,1,0],[0,0,1]]
    list5 = [95,65,35,95,65,35]
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    for_test= []
    expected=[95,65,35,95,65,35,95,65,35,95,65,35,95,65,35,95,65,35]

    for id,i in enumerate(list1):
        print(i, ": ")
        for idx, j in enumerate(list2):

            # For some reason with beta = 0, the select gives double entries, fixed by grouping unique and selecting
            # everything then. i dont know why and i dont care its fixed
            statement = '''
            SELECT stddev(te) as dev, AVG(te) as avg_time_diff, COUNT(*) as unique_hits , AVG(tw) as avg_usd_total,
            AVG(tx) as avg_qty FROM(
            SELECT AVG(time_diff) as te, COUNT(*), AVG(usd_total) as tw, AVG(hitbtc_trans_{0}.qty) as tx
            FROM matches_{0}
            INNER JOIN deposit_transactions
            on matches_{0}.inc_address = deposit_transactions.inc_address
            AND matches_{0}.txid = deposit_transactions.txid
            AND deposit_transactions.match_usdt_{1} = {3}
            AND deposit_transactions.match_eth_{1} = {4}
            AND deposit_transactions.match_usdc_{1} = {5}
            AND time_diff <= {2}
            INNER JOIN hitbtc_trans_{0}
            on matches_{0}.tran_id = hitbtc_trans_{0}.id
            GROUP BY matches_{0}.inc_address,matches_{0}.txid) as cw
            ;
            '''.format(i,j,list3[idx], list4[id][0], list4[id][1],list4[id][2])

            #print(statement)
            df = pds.read_sql(statement, conn)
            print("           ", j, f"{df['unique_hits'][0]:,}", round((df['avg_time_diff'][0]), 2),
                  round(df['avg_usd_total'][0],2),f"{df['avg_qty'][0]:.4f}",round(df['dev'][0],2))


            statement2= '''
            SELECT AVG(time_diff) as time_diff, COUNT(*)
            FROM matches_{0}
            INNER JOIN deposit_transactions
            on matches_{0}.inc_address = deposit_transactions.inc_address
            AND matches_{0}.txid = deposit_transactions.txid
            AND deposit_transactions.match_usdt_{1} = {3}
            AND deposit_transactions.match_eth_{1} = {4}
            AND deposit_transactions.match_usdc_{1} = {5}
            AND time_diff <= {2}
            GROUP BY matches_{0}.inc_address,matches_{0}.txid
            '''.format(i,j,list3[idx], list4[id][0], list4[id][1],list4[id][2])
            df2 = pds.read_sql(statement2, conn)
            #print(df2['time_diff'].mean())
            result= stats.ttest_1samp(a=df2, popmean=list5[idx])
            p_value= result[1][0]
            if( p_value<=0.05):
                print("Difference: YES", p_value)
            else:
                print("Difference: NO",p_value)


    print(sum(expected)-sum(for_test))


#Prints Figure x
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

    merged_df_usdt = df2_usdt.merge(df_usdt, how='left', on=['qty'])

    df3_usdt = merged_df_usdt[merged_df_usdt['sum_e'].notna()]
    df3_usdt  = df3_usdt.reset_index()

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
    statement2_eth = '''SELECT qty, COUNT(*) as count_e FROM "hitbtc_trans_eth" WHERE side= 'buy' GROUP BY qty HAVING 
    COUNT(*)>0'''
    df2_eth = pds.read_sql(statement2_eth, conn)

    merged_df_eth = df2_eth.merge(df_eth, how='left', on=['qty'])

    df3_eth = merged_df_eth[merged_df_eth['sum_e'].notna()]
    df3_eth = df3_eth.reset_index()

    df3_eth['count_e'] = (df3_eth['count_e'] / df3_eth['count_e'].sum()) * 100
    df3_eth['sum_e'] = (df3_eth['sum_e'] / df3_eth['sum_e'].sum()) * 100
    df3_eth.drop('index', axis=1, inplace= True)

    df3_eth['diff_count_sum'] = df3_eth.sum_e - df3_eth.count_e

    # CHANGE THIS BELOW FOR EVERY VALUE
    # USDT 0.3 USDC 1 ETH
    df3_eth.drop(df3_eth[df3_eth.qty > x].index, inplace=True)
    df3_eth.sort_values(by=['qty'], inplace= True)

    ###############################################################################################
    statement_usdc = '''SELECT qty,SUM(match_usdc_1_2) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
                   SUM(match_usdc_1_2) >0 '''
    df_usdc = pds.read_sql(statement_usdc, conn)
    statement2_usdc = '''SELECT qty, COUNT(*) as count_e FROM "hitbtc_trans_usdc" WHERE side= 'sell' GROUP BY qty HAVING 
    COUNT(*)>0'''
    df2_usdc = pds.read_sql(statement2_usdc, conn)

    merged_df_usdc = df2_usdc.merge(df_usdc, how='left', on=['qty'])

    df3_usdc = merged_df_usdc[merged_df_usdc['sum_e'].notna()]
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



    fig, (ax1, ax2,ax3) = plt.subplots(1,3, sharey=True)
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
        if(df3_usdt.at[i, 'qty'] <0.0001):
            ax1.text(df3_usdt.at[i,  'qty'], df3_usdt.at[i, 'diff_count_sum'], format(df3_usdt.at[i, 'qty'],".5f"),
                     size=12)
        else:
            ax1.text(df3_usdt.at[i, 'qty'], df3_usdt.at[i, 'diff_count_sum'], df3_usdt.at[i, 'qty'], size=12)
    for i in range(len(df3_eth.index)):
        if(df3_eth.at[i, 'qty'] <0.0001):
            ax2.text(df3_eth.at[i, 'qty'], df3_eth.at[i, 'diff_count_sum'], format(df3_eth.at[i, 'qty'],".5f"), size=12)
        else:
            ax2.text(df3_eth.at[i, 'qty'], df3_eth.at[i, 'diff_count_sum'], df3_eth.at[i, 'qty'], size=12)
    for i in range(len(df3_usdc.index)):
        if(df3_usdc.at[i, 'qty'] <0.0001):
            ax3.text(df3_usdc.at[i, 'qty'], df3_usdc.at[i, 'diff_count_sum'], format(df3_usdc.at[i, 'qty'],".5f"),
                 size=12)
        else:
            ax3.text(df3_usdc.at[i, 'qty'], df3_usdc.at[i, 'diff_count_sum'], df3_usdc.at[i, 'qty'],
                 size=12)
    plt.show()

#Prints Figure x
def plotNrOfMatches():
    labels = ['0', '1', '2', '3', '4', '5']
    x = np.arange(len(labels))

    fig, (ax1, ax2) = plt.subplots(1, 2,sharey=True)

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

#Looking at the quanties where matches are found and looking into it
def qty_match_ana():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    pd.options.mode.chained_assignment = None
    list1= {'usdt', 'eth', 'usdc'}
    list2 = {'3_2', '2_2', '1_2', '3_0', '2_0', '1_0'}

    for i in list1:
        for j in list2:
            statement = '''SELECT qty,SUM(match_{0}_{1}) as deposit_match_total FROM "deposit_transactions" GROUP BY 
            qty 
            HAVING 
                           SUM(match_{0}_{1}) >0 '''.format(i,j)
            df = pds.read_sql(statement, conn)
            statement2 = '''SELECT qty, COUNT(*) as hitbtc_qty_count FROM "hitbtc_trans_{0}" WHERE side= 'sell' GROUP BY qty 
            HAVING COUNT(*)>0'''.format(i)
            df2 = pds.read_sql(statement2, conn)

            merged_df = df2.merge(df, how='left', on=['qty'])

            less_df = merged_df[merged_df['deposit_match_total'].notna()]

            less_df.sort_values(by=['qty'], inplace= True)
            less_df = less_df.reset_index()
            less_df.drop('index', axis=1, inplace=True)

            less_df2=less_df.drop(less_df[less_df.qty > 1].index)

            print("This is ", i, j)

            print(f"{len(df2.index):,}"," ")
            print(f"{len(df.index):,}"," ")
            print(f"{len(less_df.index):,}"," ")
            print(f"{len(less_df2.index):,}",round(len(less_df2.index)/len(less_df.index)*100,2), " ")
            print()

#FIG that
def tran_usd_size():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    list1= ['usdt', 'eth', 'usdc']

    #######################################
    statement_usdt = '''SELECT usd_total FROM "hitbtc_trans_usdt" WHERE side='sell' '''.format(list1[2])
    df_usdt = pds.read_sql(statement_usdt, conn)

    df_usdt['usdt_buckets'] = pds.cut(df_usdt.usd_total, bins=[0,1000,5000,20000,50000,200000,1000000000000],
                                     right=False)
    df2_usdt= df_usdt['usdt_buckets'].value_counts()
    df2_usdt = (df2_usdt / df2_usdt.sum()) * 100
    #######################################
    statement_eth = '''SELECT usd_total FROM "hitbtc_trans_eth" WHERE side='buy' '''.format(list1[2])
    df_eth = pds.read_sql(statement_eth, conn)

    df_eth['usdt_buckets'] = pds.cut(df_eth.usd_total, bins=[0,1000,5000,20000,50000,200000,1000000000000],
                                     right=False)
    df2_eth= df_eth['usdt_buckets'].value_counts()
    df2_eth = (df2_eth/ df2_eth.sum()) * 100
    #######################################
    statement_usdc = '''SELECT usd_total FROM "hitbtc_trans_usdc" WHERE side='sell' '''.format(list1[2])
    df_usdc = pds.read_sql(statement_usdc, conn)

    df_usdc['usdt_buckets'] = pds.cut(df_usdc.usd_total, bins=[0,1000,5000,20000,50000,200000,1000000000000],
                                     right=False)
    df2_usdc= df_usdc['usdt_buckets'].value_counts()
    df2_usdc = (df2_usdc / df2_usdc.sum()) * 100

    #fig, (ax1, ax2) = plt.subplots(1,2, figsize=(10, 5))

    xlabel= ['0 to $1k','$1k to $5k','5$ to $1k','$20k to $50k','$50 to $200k', 'Over $200k']
    x= np.arange(len(xlabel))

    ax = plt.subplot(111)

    ax.bar(x - 0.2,df2_usdt.values,width=0.2)
    ax.bar(x , df2_eth.values,width=0.2)
    ax.bar(x+0.2, df2_usdc.values,width=0.2)
    ax.set_xticks(x,xlabel)
    ax.autoscale(tight=True)
    #plt.subplots_adjust(bottom=0.2)
    #plt.yscale('log')
    #ax.set_xticks(rotation='vertical')
    plt.show()
    print("Done")

#Want bubble
def bubble_x_x():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement_usdt = '''SELECT qty, usd_total, price FROM "hitbtc_trans_usdt" WHERE side='sell' '''

    df = pds.read_sql(statement_usdt, conn)

    #np.random.seed(42)
    #N = 100
    #x = np.random.normal(170, 20, N)
    #y = x + np.random.normal(5, 25, N)
    #colors = np.random.rand(N)
    #area = (25 * np.random.rand(N)) ** 2

    #df = pds.DataFrame({
    #    'X': x,
    #    'Y': y,
    #    'Colors': colors,
     #   "bubble_size": area})

    #df.head(n=3)

    print(df)

    # scatter plot with scatter() function
    plt.scatter('usd_total', 'price', s='qty',data=df)


    plt.xlabel("X", size=16)
    plt.ylabel("y", size=16)
    plt.xscale('log')
    plt.title("Scatter Plot with Matplotlib", size=18)

    # scatter plot with scatter() function
    # transparency with "alpha"
    # bubble size with "s"
    #plt.scatter('X', 'Y',
    #            s='bubble_size',
    #            alpha=0.5,
    #            data=df)
    #plt.xlabel("X", size=16)
    #plt.ylabel("y", size=16)
    #plt.title("Bubble Plot with Matplotlib", size=18)

    # scatter plot with scatter() function
    # transparency with "alpha"
    # bubble size with "s"
    # color the bubbles with "c"
    #plt.scatter('X', 'Y',
    #            s='bubble_size',
    #            c='Colors',
    #            alpha=0.5, data=df)
    #plt.xlabel("X", size=16)
    #plt.ylabel("y", size=16)
    #plt.title("Bubble Plot with Colors: Matplotlib", size=18)

    plt.show()


# Showing how often a value appears in depositing transaction, showing how often a qty is transacted on hitbtc
def redundand():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement = '''SELECT qty,SUM(match_usdt_3_0) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
    SUM(match_usdt_3_2) >178640 '''
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

# Showing qty, then how many matches found, then how often qty is transacted with
def df_of_tran_sum():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()


    statement = '''SELECT qty,SUM(match_usdc_1_2) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
                   SUM(match_usdc_1_2) >0 '''
    dataFrame = pds.read_sql(statement, conn)
    statement2 = '''SELECT qty, COUNT(*) FROM "hitbtc_trans_usdc" WHERE side= 'sell' GROUP BY qty HAVING COUNT(
                    *)>0'''
    dataFrame2 = pds.read_sql(statement2, conn)

    merged_df = dataFrame2.merge(dataFrame, how='left', on=['qty'])

    #less_df = merged_df[merged_df['sum_e'].notna()]
    #less_df = less_df.reset_index()
    less_df = merged_df

    #less_df.drop('index', axis=1, inplace=True)
    less_df.sort_values(by=['qty'], inplace= True)

    #fig, (ax1, ax2) = plt.subplots(1, 1,sharey=True)


    plt.plot(less_df['qty'], less_df['count'])
    #ax1.axhline(y=0, color='r', linewidth=0.7)
    plt.title('USDC')
    plt.ylabel('Count')
    plt.xlabel('qty')

    print("There are ", len(merged_df.index), " different transaction quantities, ", len(less_df.index))

    plt.yscale('log')
    plt.show()


# For the qty x, there are y transactions in hitbtc_trans_x and in real_deposit_transaction total matches found GRAPH
def df_tran_sum_graph_single():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    statement = '''SELECT qty,SUM(match_usdt_1_2) as sum_e FROM "deposit_transactions" GROUP BY qty HAVING 
                   SUM(match_usdt_1_2) >0 '''
    dataFrame = pds.read_sql(statement, conn)
    statement2 = '''SELECT qty, COUNT(*) as count_e FROM "hitbtc_trans_usdt" WHERE side= 'sell' GROUP BY qty HAVING 
    COUNT(*)>0'''
    dataFrame2 = pds.read_sql(statement2, conn)

    merged_df = dataFrame2.merge(dataFrame, how='left', on=['qty'])

    less_df = merged_df[merged_df['sum_e'].notna()]
    less_df = less_df.reset_index()

    less_df['count_e'] = (less_df['count_e'] / less_df['count_e'].sum()) * 100
    less_df['sum_e'] = (less_df['sum_e'] / less_df['sum_e'].sum()) * 100
    less_df.drop('index', axis=1, inplace=True)
    print(less_df)
    print(less_df.sort_values(by=['count_e']))
    print(less_df.sort_values(by=['sum_e']))

    less_df['diff_count_sum'] = less_df.sum_e - less_df.count_e

    print(less_df)
    print(less_df.sort_values(by=['count_e']))
    print(less_df.sort_values(by=['sum_e']))
    print(less_df.sort_values(by=['diff_count_sum']))

    # CHANGE Only QTY difference below
    less_df.drop(less_df[less_df.qty > 1].index, inplace=True)

    #CHANGE threshhold Show Only Divergence after x
    less_df.drop(less_df[abs(less_df.diff_count_sum) < 2.5].index, inplace=True)
    less_df = less_df.reset_index()

    #print(less_df)

def benfords_law():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    #statement = '''SELECT qty FROM "hitbtc_trans_usdt" WHERE side='sell' '''
    #statement = '''SELECT qty FROM "deposit_transactions" '''
    statement = '''SELECT dep_qty as qty FROM "matches_usdc" '''
    df = pds.read_sql(statement, conn)
    df['qty'] = (df['qty']) * 1000000
    df['leading_digit'] = df['qty'].astype(str).str[0].astype(int)
    df_f= df['leading_digit'].value_counts(normalize=True)*100
    df_f.sort_index(inplace=True)
    print(df_f)

    ax= df_f.plot.bar(color='#4d6910')
    df_b = pd.DataFrame({'Benford´s Law':[30.1,17.6,12.5,9.7,7.9,6.7,5.8,5.1,4.6]})
    df_b.plot(ax=ax, marker='o', color='#ffa600', label='')
    plt.ylabel("Probability")
    plt.xlabel("Leading Digit")
    plt.show()

    print(chisquare(df_f.values, f_exp=[30.1,17.6,12.5,9.7,7.9,6.7,5.8,5.1,4.6]) )

def benfords_law2():
    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()
    statement = '''SELECT qty FROM hitbtc_trans_usdc WHERE side='sell' '''


    statement2 = '''SELECT qty FROM deposit_transactions WHERE match_usdc_1_2 != 0''' # Matches > 0

    statement3 = '''SELECT dep_qty as qty FROM "matches_usdc" WHERE time_diff < 60 '''

    statement4 = '''SELECT txid, inc_address,dep_qty as qty, COUNT(*)  FROM "matches_usdc" GROUP BY dep_qty, 
        inc_address, txid HAVING COUNT(*)=1 ''' #Matches = 1

    df = pds.read_sql(statement, conn)
    df['qty'] = (df['qty']) * 1000000
    df['leading_digit'] = df['qty'].astype(str).str[0].astype(int)
    df_f= df['leading_digit'].value_counts(normalize=True)*100
    df_f.sort_index(inplace=True)


    df2 = pds.read_sql(statement2, conn)
    df2['qty'] = (df2['qty']) * 1000000
    df2['leading_digit'] = df2['qty'].astype(str).str[0].astype(int)
    df_f2= df2['leading_digit'].value_counts(normalize=True)*100
    df_f2.sort_index(inplace=True)

    df3 = pds.read_sql(statement3, conn)
    df3['qty'] = (df3['qty']) * 1000000
    df3['leading_digit'] = df3['qty'].astype(str).str[0].astype(int)
    df_f3= df3['leading_digit'].value_counts(normalize=True)*100
    df_f3.sort_index(inplace=True)

    df_b = pd.DataFrame({'Benford´s Law': [30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]})

    fig, (ax1,ax2,ax3)= plt.subplots(1, 3,sharey=True)
    fig.set_size_inches(15, 7.5)

    ax1.bar(df_f.index.values, df_f.values, color='#4d6910')
    ax1.plot(df_f.index.values,df_b.values, marker='o', color='#ffa600', label='')
    ax1.set_xlabel("Leading Digit")
    ax1.set_ylabel("Probability")
    ax1.set_title("HitBTC USDC Transactions")

    #print(df_f2.index.values)
    ax2.bar(df_f2.index.values, df_f2.values, color='#4d6910')
    ax2.plot(df_f2.index.values,df_b.values, marker='o', color='#ffa600', label='')
    ax2.set_xlabel("Leading Digit")
    ax2.set_title("Matches USDC - > 0")

    ax3.bar(df_f3.index.values, df_f3.values, color='#4d6910')
    ax3.plot(df_f2.index.values,df_b.values, marker='o', color='#ffa600', label='')
    ax3.set_xlabel("Leading Digit")
    ax3.set_title("Matches USDC - All")



    df4 = pds.read_sql(statement4, conn)
    df4['qty'] = (df4['qty']) * 1000000
    df4['leading_digit'] = df4['qty'].astype(str).str[0].astype(int)
    df_f4= df4['leading_digit'].value_counts(normalize=True)*100
    df_f4.sort_index(inplace=True)

    print(chisquare(df_f.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    print(chisquare(df_f2.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    print(chisquare(df_f3.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    print(chisquare(df_f4.values, f_exp=[30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]))
    plt.show()

    #print(chisquare(df_f.values, f_exp=[30.1,17.6,12.5,9.7,7.9,6.7,5.8,5.1,4.6]) )

#table_avg_time2()
#bubble_x_x()
#tran_usd_size()
#df_of_tran_sum()
#df_tran_sum_graph_single()
benfords_law2()
#df_tran_sum_graph_all()
#plotNrOfMatches()
#plotmatchesbyqty5(
#table_printer()
#plotmatchesbyqty5()