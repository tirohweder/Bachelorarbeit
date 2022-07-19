import datetime
import requests
import json
import settings


currency = settings.currency

if currency == 'usdt':
    trading_pair = 'btcusdt'
elif currency == 'eth':
    trading_pair = 'ethbtc'
else:
    trading_pair= 'btcusdc'

def main():
    try:
        con = settings.con
        cur = con.cursor()
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

        collect_hitbtc_transactions(cur, con)
        deleting_duplicates(cur, con, currency)

    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (con):
            cur.close()
            con.close()
            print("PostgreSQL connection is closed")


def collect_hitbtc_transactions(cur, con):
    # Set start date in parameter till
    # Change trading pair as wanted

    parameters = settings.parameters
    time_frame = settings.time_frame

    starting_date = datetime.datetime(int(parameters['till'][:4]), int(parameters['till'][5:7]),
                                      int(parameters['till'][8:10]),
                                      int(parameters['till'][11:13]), int(parameters['till'][14:16]),
                                      int(parameters['till'][17:19]),
                                      int(parameters['till'][20:23]))
    # Set time_frame to how far back data should be added, (minutes, days, weeks)
    ending_date = starting_date - datetime.timedelta(weeks=time_frame)
    current_start_date = starting_date

    count = 0;

    while current_start_date > ending_date:
        response_format = []
        prev_start_date = current_start_date
        try:
            count = count + 1;
            response = requests.get("https://api.hitbtc.com/api/3/public/trades/" + trading_pair, params=parameters)
            response_format = response_format + json.loads(response.text)

            counter = 0
            for item in response_format:
                counter = counter + 1;
                statement_part2 = ""
                for attribute, value in item.items():
                    if attribute == "timestamp":
                        statement_part2 = statement_part2 + "'" + str(value).replace("T", " ")
                        statement_part2 = statement_part2[:-1] + "'"
                    elif attribute == "side":
                        statement_part2 = statement_part2 + "'" + value + "',"
                    else:
                        statement_part2 = statement_part2 + str(value) + ", "

                statement= '''
                                    INSERT INTO hitbtc_trans_{1}(id, price, qty, side, timestamp) VALUES (
                                    {0})
                                    '''.format(statement_part2, currency)

                #print(statement)
                cur.execute(statement)
                con.commit()
            if count % 100 == 0:
                print(str(count * 100000) + " transactions parsed")

            parameters = {"till": response_format[-1]['timestamp'], "limit": 1000}
            current_start_date = datetime.datetime(int(parameters['till'][:4]), int(parameters['till'][5:7]),
                                                   int(parameters['till'][
                                                       8:10]), int(parameters['till'][11:13]),
                                                   int(parameters['till'][14:16]), int(parameters['till'][17:19]),
                                                   int(parameters['till'][20:23]) * 1000)

            # Triggers if more than 1000 transactions at the same timespan
            if current_start_date == prev_start_date:
                current_start_date = current_start_date - datetime.timedelta(milliseconds=10)
                parameters = {"till": current_start_date, "limit": 1000}

        except ConnectionError:
            return ("ERROR - " + str(response.status_code))


# Deletes duplicates
def deleting_duplicates(cur, con, currency):
    statement = '''
                alter table hitbtc_trans_{0}
                add uid serial
                '''.format(currency)
    cur.execute(statement)
    con.commit()

    statement = '''
            DELETE FROM hitbtc_trans_{} a
            USING hitbtc_trans_{} b
            WHERE a.uid < b.uid
            AND a.id = b.id
            '''.format(currency)
    cur.execute(statement)
    con.commit()

    statement = '''
                alter table hitbtc_trans_{0}
                drop column uid;
                '''.format(currency)

    cur.execute(statement)
    con.commit()


main()
