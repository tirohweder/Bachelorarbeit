import ssl

import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine
import pandas as pds

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
    #ssl_args = {'ssl': {'cert': '/path/to/client-cert',
                        #'key': 'C:/Users/rohwe/Desktop/privatekeyssh2',
                        #'ca': '/path/to/ca-cert'}}
    #ssl_context = ssl.create_default_context(cafile='C/Users/rohwe/Desktop/privatekeyssh.ppk')
    #ssl_context.verify_mode = ssl.CERT_REQUIRED

    #ssl_args = {'ssl': {'cert': 'C:/Users/rohwe/Desktop/privatekeyssh2'}}



    #ssl_args= {"ssl": 'C/Users/rohwe/Desktop/privatekeyssh.ppk'}

    engine = create_engine('postgresql+psycopg2://trohwede:hallo123@localhost:8877/trohwede')
    conn = engine.connect()

    dataFrame = pds.read_sql("SELECT * FROM \"hitbtc_trans_busd\"", conn)
    print(dataFrame)

def main():
    con= ""
    try:
        con = psycopg2.connect(user="trohwede",
                               password="1687885@uma",
                               host="localhost",
                               port="8877",
                               database="trohwede")

        cur = con.cursor()
        cur2= con.cursor()
        cur3= con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        cur.execute('''SELECT * FROM hitbtc_trans_busd''')

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            cur2.close()
            cur3.close()
            con.close()
            print("PostgreSQL connection is closed")


plotmatchesbyqty()
