import psycopg2
import datetime

from neo4j import GraphDatabase


def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="1687885@uma",
                               host="localhost",
                               port="5432",
                               database="trohwede")
        cur = con.cursor()
        cur2 = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        #moreInfo(cur, con, cur2)
        #connectionWithHostDoeOnlyOnce(cur, con)
        connectionWithHost(cur, con, cur2)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

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


def moreInfo(cur, con, cur2):
    conn = Neo4jConnection(uri='bolt://localhost:7687', user='trohwede', pwd='1687885@uma')

    selection = 'SELECT address FROM unique_address ' \
                'WHERE in_degree IS NULL'

    # print(selection)
    cur.execute(selection)
    for row in cur:
        # nimmt addresse und guckt welche transactions zu der wallet führen
        query2 = '''
        MATCH (a:Address)
        WHERE a.address='{0}'
        RETURN a.inDegree AS inDegree, a.outDegree AS outDegree
        '''.format(row[0])

        result2 = conn.query(query2)
        # print(result2[0]["inDegree"],result2[0]["outDegree"])

        statement = "UPDATE unique_address " \
                    "SET in_degree = " + str(result2[0]["inDegree"]) + ", out_degree = " + \
                    str(result2[0]["outDegree"]) + \
                    " WHERE address = " + "'" + row[0] + "'"

        # print(statement)
        cur2.execute(statement)
        con.commit()


def connectionWithHostDoeOnlyOnce(cur, con):
    statement = "UPDATE unique_address " \
                "SET connections_with_host = 1" \
                " WHERE in_degree = 1"

    cur.execute(statement)
    con.commit()


def connectionWithHost(cur, con, cur2):
    statement1 = 'SELECT address FROM unique_address ' \
                 'WHERE in_degree IS NOT NULL AND condition IS NULL'

    # print(selection)
    cur.execute(statement1)

    for row in cur:
        statement2 = '''
                        SELECT  COUNT(*)
                        FROM incoming_transactions
                        WHERE inc_address = '{0}'
                        '''.format(row[0])
        # print(statement2)
        cur2.execute(statement2)
        result = cur2.fetchall()
        # print(result[0][0])
        statement3 = "UPDATE unique_address " \
                     "SET connections_with_host= " + str(result[0][0]) + \
                     " WHERE address = " + "'" + row[0] + "'"

        # print(statement3)
        cur2.execute(statement3)
        con.commit()


main()
