import datetime
import psycopg2
import requests
import json

def main():
    try:
        con = psycopg2.connect(user="trohwede",
                               password="1687885@uma",
                               host="localhost",
                               port="5432",
                               database="trohwede")
        cur = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        finalCreate(cur, con)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            con.close()
            print("PostgreSQL connection is closed")


def finalCreate(cur, con):
    parameters = {"till": "2022-03-30 23:59:58.412000", "limit": 1000}

    startingDate = datetime.datetime(int(parameters['till'][:4]), int(parameters['till'][5:7]), int(parameters['till'][                                                                                                    8:10]),
                                     int(parameters['till'][11:13]), int(parameters['till'][14:16]),
                                     int(parameters['till'][17:19]),
                                     int(parameters['till'][20:23]))
    endingDate = startingDate - datetime.timedelta(weeks=70)

    currentStartDate = startingDate
    traidingPair = ("BTCBUSD")
    count = 0;

    #set variable data abitrary
    prevStartDate = currentStartDate + datetime.timedelta(days=1)
    while (currentStartDate > endingDate):
        responseformat = list()
        # print(currentStartDate)
        # print(currentStartDate, prevStartDate, currentStartDate== prevStartDate)

        prevStartDate = currentStartDate
        try:
            count = count + 1;
            response = requests.get("https://api.hitbtc.com/api/3/public/trades/" + traidingPair, params=parameters)
            responseformat = responseformat + json.loads(response.text)

            counter = 0
            for item in responseformat:
                counter = counter + 1;
                statementPart2 = ""
                for attribute, value in item.items():
                    if attribute == "timestamp":
                        statementPart2 = statementPart2 + "'" + str(value).replace("T", " ")
                        statementPart2 = statementPart2[:-1] + "'"
                    elif attribute == "side":
                        statementPart2 = statementPart2 + "'" + value + "',"
                    else:
                        statementPart2 = statementPart2 + str(value) + ", "

                statementPart1 = "INSERT INTO hitbtc_trans_busd(id, price, qty, side, timestamp) VALUES ("
                statement = statementPart1 + statementPart2 + ")"
                # print(statement)
                cur.execute(statement)
                con.commit()
            if (count % 100 == 0):
                print(str(count * 100000) + " transactions parsed")

            parameters = {"till": responseformat[-1]['timestamp'], "limit": 1000}
            currentStartDate = datetime.datetime(int(parameters['till'][:4]), int(parameters['till'][5:7]),
                                                 int(parameters['till'][
                                                     8:10]), int(parameters['till'][11:13]),
                                                 int(parameters['till'][14:16]), int(parameters['till'][17:19]),
                                                 int(parameters['till'][20:23]) * 1000)

            # print(currentStartDate, prevStartDate)
            if (currentStartDate == prevStartDate):
                currentStartDate = currentStartDate - datetime.timedelta(milliseconds=10)
                parameters = {"till": currentStartDate, "limit": 1000}

            #print(parameters)

        except ConnectionError:
            return ("ERROR - " + str(response.status_code))


main()
