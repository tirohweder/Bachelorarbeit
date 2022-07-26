import psycopg2
import neo4jconn

global currency
global con
global conn
global master_address


con = psycopg2.connect(user="trohwede",
                       password="hallo123",
                       host="localhost",
                       port="5432", #server 5432 / 8877
                       database="testing")

conn = neo4jconn.Neo4jConnection(uri='bolt://localhost:7687', user='trohwede', pwd='1687885@uma')



master_address = '1EEqRvnS7XqMoXDcaGL7bLS3hzZi1qUZm1'
currency = 'eth' #eth, usdc


# For hitbtc_trans_data
parameters = {"till": "2022-03-31 23:59:59.000000", "limit": 1000} #select starting date
time_frame = 500 # this is in weeks, to change to smaller go change in function


path_tagpacks = r"C:\Users\rohwe\PycharmProjects\Bachelorarbeit\Abgabe\Additonal_Data\tagpacks"
graphsense_api_key = r'/trlZnh9014X8kosj4mNW6ZaAep+e8+1'
