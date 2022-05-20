import datetime

import psycopg2
import requests
import json


def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="admin",
                               host="localhost",
                               port="5434",
                               database="Bachelorarbeit_Rohweder")
        cur = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        apiAccess(cur, con)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            con.close()
            print("PostgreSQL connection is closed")

def apiAccess(cur, con):
    parameters = {"from": "2022-01-16 00:00:00", "till": "2022-01-17 00:00:00"}
    traidingPair = ("BTCUSDT")
    try:
        response = requests.get("https://api.hitbtc.com/api/3/public/trades/" + traidingPair, params=parameters)
        # print(json.dumps(response.text))
        responseformat = (json.loads(response.text))

        for item in responseformat:
            statementPart2 = ""
            for attribute, value in item.items():
                if attribute == "timestamp":
                    statementPart2 = statementPart2 + "'" + str(value).replace("T", " ")
                    statementPart2 = statementPart2[:-1] + "'"
                elif attribute == "side":
                    statementPart2 = statementPart2 + "'" + value + "',"
                else:
                    statementPart2 = statementPart2 + str(value) + ", "

            statementPart1 = "INSERT INTO transactions(id, price, qty, side, timestamp) VALUES ("
            statement = statementPart1 + statementPart2 + ")"
            #print(statement)
            cur.execute(statement)
            con.commit()



    except ConnectionError:
        return ("ERROR - " + str(response.status_code))


def main2():
    parameters = {"till": "2022-01-16 00:00:00.000", "limit": 1000}

    responseformat = list()
    startingDate = datetime.datetime(int(parameters['till'][:4]), int(parameters['till'][5:7]), int(parameters['till'][
                                                                                                    8:10]),
                                     int(parameters['till'][11:13]), int(parameters['till'][14:16]),
                                     int(parameters['till'][17:19]),
                                     int(parameters['till'][20:23]))
    endingDate = startingDate - datetime.timedelta(days=1)

    currentStartDate = startingDate
    print("CurrentStartDate: ", currentStartDate, "  When it will end:  ", endingDate)

    while (currentStartDate > endingDate):
        try:
            response = requests.get("https://api.hitbtc.com/api/3/public/trades/" + "BTCUSDT", params=parameters)
            # print(json.dumps(response.text))
            responseformat = responseformat + json.loads(response.text)

            counter = 0
            for item in responseformat:
                counter = counter + 1;
                for attribute, value in item.items():
                    if (attribute == 'timestamp'):
                        print(attribute, value)

            print(counter)
            print(" LastTimeStamp ", responseformat[-1]['timestamp'])
            parameters = {"till": responseformat[-1]['timestamp'], "limit": 1000}
            currentStartDate = datetime.datetime(int(parameters['till'][:4]), int(parameters['till'][5:7]),
                                                 int(parameters['till'][
                                                     8:10]), int(parameters['till'][11:13]),
                                                 int(parameters['till'][14:16]), int(parameters['till'][17:19]),
                                                 int(parameters['till'][20:23]))
            print(currentStartDate, "    ", endingDate)
        except ConnectionError:
            return ("ERROR - " + str(response.status_code))

main2()