import datetime

import requests
import json


def hitBtc_api_request(marketDataFrom, marketDataTill, tradingPair):
    parameters = {"from": marketDataFrom, "till": marketDataTill}
    try:
        response = requests.get("https://api.hitbtc.com/api/3/public/trades/" + tradingPair, params=parameters)
        # print(json.dumps(response.text))
        return json.loads(response.text)
    except ConnectionError:
        return ("ERROR - " + str(response.status_code))


# Insert Value= valueYouAreLookingFor, then transaction time happend, and how long you want to look for in mins
def naiv_value_matching(value, year, month, day, hour, min, timespan, tradingPair):
    print("Looking for matches in: " + tradingPair)
    marketDataFrom = datetime.datetime(year, month, day, hour, min, 0, 0)
    timespan = datetime.timedelta(minutes=timespan)

    marketDataTill = marketDataFrom + timespan

    try:
        match_return = list(
            filter(lambda x: x["qty"] == value, hitBtc_api_request(marketDataFrom, marketDataTill, tradingPair)))
        if not match_return:
            print("No matches found!")
            return 0
        else:
            return match_return
    except:
        print("     ERROR - CANT FIND PAIR: " + tradingPair)
        return 0


def multiple_matches():
    interestingPairsBTC = ["ADA", "ETH", "BNB", "SOL", "MATIC", "LTC", "NEAR", "LUNA", "ATOM", "FTM", "AAVE", "DOT",
                           "LRC", "ONE", "UNI",
                           "AVAX", "MANA", "ROSE", "XMR", "XRP", "LINK", "SAND", "EGLD", "TRX", "THETA", "VET", "FTT",
                           "CRV", "AXS", "ZEC",
                           "GALA", "ALGO", "EOS", "OMG", "XTZ", "DUSK", "ENJ", "DASH", "ICP", "XLM", "CELR", "SUSHI",
                           "ANT", "RVN", "WAVES", "ZIL",
                           "IOST", "RUNE", "FIL", "HBAR", "MITH", "KSM", "BAT", "HEX", "KNC", "COMP", "SNX", "UTK",
                           "REP", "BSV", "CHSB", "DYDX", ]

    interestingPairsBTC2 = {"BTCUSDT", "BTCBUSD", "BTCUSDC", "BTCTUSD", "qweqwe"}

    matches = 0
    matchingList= []

    for pair in interestingPairsBTC:
        naiv_matching_result = json.dumps(naiv_value_matching("0.00150732", 2022, 1, 15, 0, 0, 1440, pair + "BTC"), indent=4,
                                          sort_keys=True)
        if (naiv_matching_result != "0"):
            matches += 1
            matchingList.append(pair)
            print(naiv_matching_result)
        else:
            print("")

    for pair2 in interestingPairsBTC2:
        naiv_matching_result = json.dumps(naiv_value_matching("0.00150732", 2022, 1, 15, 0, 0, 1440, pair2), indent=4,
                                          sort_keys=True)

        if (naiv_matching_result != "0"):
            matches += 1
            matchingList.append(pair2)
            print(naiv_matching_result)
        else:
            print("")

    if matches==1:
        print("We found 1 Match! The trade was found in: ")
        print(matchingList)
    elif matches>1:
        print("We found "+matches+" matches the belonging tradingpairs are: ")
        print(matchingList)

    else:
        print("We found no Match. Try again.")



multiple_matches()

# parameters2 = {"from": "2017-11-22T02:30", "till": "2017-11-23T02:30"}
# response2 = requests.get("https://api.hitbtc.com/api/3/public/trades/" + "BTCUSDT", params=parameters2)
# print(json.dumps(response2.text))
