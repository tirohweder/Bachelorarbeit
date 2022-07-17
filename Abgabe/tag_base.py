import yaml
from pathlib import Path

pathlist = Path(r"C:\Users\rohwe\PycharmProjects\Bachelorarbeit\packs").glob('**/*.yaml')
import psycopg2
# for path in pathlist:
#     print (str(path))
#     with open(str(path), "r") as stream:
#         try:
#             data= (yaml.safe_load(stream))
#             single = False
#             for type in data:
#
#                 if type == 'label':
#                     single= True
#
#
#
#             if single:
#                 print("test")
#
#             else:
#                 for row in data['tags']:
#                    print(row['address'], row['label'], row['source'])
#         except yaml.YAMLError as exc:
#             print(exc)
def main():

    try:
        con = psycopg2.connect(user="trohwede",
                               password="hallo123",
                               host="localhost",
                               port="8877",
                               database="trohwede")
        cur = con.cursor()
        cur2 = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        #moreInfo(cur, con, cur2)

        #connectionWithHostDoeOnlyOnce(cur, con)
        #connectionWithHost(cur, con, cur2)
        #getRealOutDegree(cur,con,cur2)
        #getRealInDegree(cur,con,cur2)
        #doOnlyOnce(cur,con)
        #getUSDValueForETHtran(cur,con,cur2)3
        #originChecker(cur,con,cur2)
        #densityChecker(cur,con,cur2)
        info(cur,con,cur2)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            cur2.close()
            con.close()
            print("PostgreSQL connection is closed")

def info(cur, con, cur2):
    for path in pathlist:
        print (str(path))
        with open(str(path), "r") as stream:
            try:
                data= (yaml.safe_load(stream))

                for row in data['tags']:
                    #print(row['address'])

                    statement = ''' 
                            INSERT INTO tags (address)
                            VALUES ('{0}')
                    '''.format(row['address'])

                    cur2.execute(statement)
                    con.commit()

            except yaml.YAMLError as exc:
                print(exc)

main()