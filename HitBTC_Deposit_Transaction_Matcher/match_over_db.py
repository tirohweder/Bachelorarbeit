import psycopg2
import datetime

def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="admin",
                               host="localhost",
                               port="5434",
                               database="Bachelorarbeit_Rohweder")
        cur = con.cursor()
        cur2= con.cursor()
        cur3 = con.cursor()
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

    selection ='''
    SELECT time, qty, txid FROM incoming_transactions 
    WHERE qty IS NOT NULL AND time > '2019-04-01 
    00:00:00.000000' 
    '''

    #print(selection)
    cur.execute(selection)
    test= cur.fetchall()
    print(len(test))
    for row in test:

        timebordertemp = row[0]
        timediff = datetime.timedelta(hours= 2)
        timeborder = timebordertemp +timediff
        #print("here")
        statement= \
            'SELECT id, qty, timestamp ' \
            'FROM hitbtc_trans ' \
            'WHERE timestamp BETWEEN '+"'"+str(row[0]) +"'"+'AND '+"'"+ str(timeborder) +"'"

        #print(statement)
        cur2.execute(statement)
        tem2 = cur2.fetchall()
        #print(len(tem2))
        #row -> is max value because it comes from the transaction
        for row2 in tem2:
            #print("t")
            if row2[1]<= row[1] and row2[1] >= row[1]-(row[1]/100)*2:
                #print("Match between: ",row[2]," and: ",row2[0]," with DepositQTY : TransactionQTY ",row[1]," : ",
                #      row2[1])

                diff= round((row2[2] -row[0]).total_seconds()/60,2)
                #print ("The transaction happend: ",round(diff.total_seconds()/60,2), " minuites after initial deposit")
                statement2 = \
                '''
                INSERT INTO matches (txid, time_diff, tran_qty,dep_qty, pair, tran_id)
                VALUES ('{0}','{1}',{2},{3},'{4}',{5})'''.format(str(row[2]), str(diff), str(row2[1]), str(row[1]),
                                                                 "USDT",
                                                                                        str(row2[0]))
                #print(statement2)
                cur3.execute(statement2)
                con.commit()

        #print (statement)

main()