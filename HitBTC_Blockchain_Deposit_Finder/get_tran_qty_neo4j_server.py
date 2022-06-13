import psycopg2
from neo4j import GraphDatabase


def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="hallo123",
                               host="localhost",
                               port="5432",
                               database="trohwede")
        cur = con.cursor()
        cur2 = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        find_qty(cur, con, cur2)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error[0])
        print("Error Type", type(error))

    finally:
        if (con):
            cur.close()
            cur2.close()
            con.close()
            print("PostgreSQL connection is closed")


class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None

        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver: ", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None

        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


def find_qty(cur, con, cur2):
    conn = Neo4jConnection(uri='bolt://localhost:7687', user='trohwede', pwd='1687885@uma')

    selection = 'SELECT * FROM incoming_transactions ' \
                "WHERE qty IS NULL"

    cur.execute(selection)

    for row in cur:
        query = '''
                MATCH (t:Transaction)-[r:RECEIVES]->(a:Address)
                WHERE t.txid = '{0}' AND a.address = '{1}'
                RETURN r.value AS qty
                '''.format(row[0], row[3])
        #print(query)

        result = conn.query(query)

        #print(query)
        #more than 1 recieves relationship can exsist for one txid and address
        total_sum= 0
        for i in result:
            total_sum= total_sum+i["qty"]


        statement = "UPDATE depositing_transactions " \
                    "SET qty = " + str(total_sum / 100000000) + \
                    " WHERE txid= " + "'" + row[0] + "'" + " AND inc_address= " + "'" + row[3] + "'"

        #print(statement)
        cur2.execute(statement)
        con.commit()


main()
