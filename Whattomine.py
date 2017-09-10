# collect data from whattomine website (https://whattomine.com/coins.json)
# WHATS NEW:
#          - in SQL use the float or real data types only if the precision provided by decimal
#            (up to 38 digits) is insufficient.
#          - in case of a server request (access) is forbiden, try to Replace the default header "python urllib/3.3.0" (the serveur might filter the python bots)
#            for more info see: https://stackoverflow.com/questions/16627227/http-error-403-in-python-3-web-scraping
#          - No Bolean type in SQL,  "BIT" instead (1/0)
#          - date in a nice format, see below "s_time" variable

import urllib.parse
import json   #json library.
from urllib.request import Request, urlopen

#fname = "coins.json"
#try :
#    fh = open(fname)
#except :
#    print(fname,"does not exist in the current folder.")
#    quit()


# 1. Fetch for updated data -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
# Replace the default header "python urllib/3.3.0" to get access to the Json (the serveur filter the python bots)
# for more info see: https://stackoverflow.com/questions/16627227/http-error-403-in-python-3-web-scraping
req = Request('http://whattomine.com/coins.json', headers={'User-Agent': 'Mozilla/5.0'})
fh = urlopen(req)

s_json = fh.read().decode() # one string format
tree = json.loads(s_json) # tree object


# 2. Update local DB -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
import sqlite3
conn = sqlite3.connect('whattomine.sqlite')
cur = conn.cursor()

from time import localtime, strftime
s_time = strftime("%Y-%m-%d %H:%M:%S", localtime())

for currency in tree["coins"]:
    currency_ = currency.replace("-","_")  # "-" not accepted in SQL table name
    currency_ = currency_.replace("(","_")
    currency_ = currency_.replace(")","_")
    currency_ = currency_.replace(" ","_")
    print("-------------------------------------------")
    print("currency_:", currency_)

    # Create sql table if needed (in case of new currency)
    Command = """CREATE TABLE IF NOT EXISTS """ + currency_ + """
                    (id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                    date_time TIMESTAMP,
                    block_time	INTEGER,
                    block_reward	NUMERIC,
                    block_reward24	NUMERIC,
                    last_block	INTEGER,
                    difficulty	INTEGER,
                    difficulty24	NUMERIC,
                    nethash	NUMERIC,
                    exchange_rate	NUMERIC,
                    exchange_rate24	NUMERIC,
                    exchange_rate_vol	NUMERIC,
                    exchange_rate_curr	TEXT,
                    market_cap	NUMERIC,
                    estimated_rewards	NUMERIC,
                    estimated_rewards24	NUMERIC,
                    btc_revenue	NUMERIC,
                    btc_revenue24	NUMERIC,
                    profitability	INTEGER,
                    profitability24	INTEGER,
                    lagging	BIT,
                    timestamp INTEGER)"""
    cur.execute(Command)

    # Add new row in with last updated values
    Command = """INSERT INTO """ + currency_ + """(date_time,block_time,block_reward,
    block_reward24,last_block,difficulty,difficulty24,nethash,exchange_rate,
    exchange_rate24,exchange_rate_vol,exchange_rate_curr,market_cap,estimated_rewards,
    estimated_rewards24,btc_revenue,btc_revenue24,profitability,profitability24,lagging,
    timestamp) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

    cur.execute(Command,(
            s_time,
            tree["coins"][currency]["block_time"],
            tree["coins"][currency]["block_reward"],
            tree["coins"][currency]["block_reward24"],
            tree["coins"][currency]["last_block"],
            tree["coins"][currency]["difficulty"],
            tree["coins"][currency]["difficulty24"],
            tree["coins"][currency]["nethash"],
            tree["coins"][currency]["exchange_rate"],
            tree["coins"][currency]["exchange_rate24"],
            tree["coins"][currency]["exchange_rate_vol"],
            tree["coins"][currency]["exchange_rate_curr"],
            tree["coins"][currency]["market_cap"],
            tree["coins"][currency]["estimated_rewards"],
            tree["coins"][currency]["estimated_rewards24"],
            tree["coins"][currency]["btc_revenue"],
            tree["coins"][currency]["btc_revenue24"],
            tree["coins"][currency]["profitability"],
            tree["coins"][currency]["profitability24"],
            tree["coins"][currency]["lagging"],
            tree["coins"][currency]["timestamp"],
            ))

    conn.commit()
conn.close()
