import requests
import json

parameters= {'till': '2019-04-10T05:16:53.940Z', 'limit': 1000}
response = requests.get("https://api.hitbtc.com/api/3/public/trades/BTCUSDT" , params=parameters)

responseformat = json.loads(response.text)

print(json.dumps(responseformat, indent=4, sort_keys=True))