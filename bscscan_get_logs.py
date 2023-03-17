# THIS IS THE SCRIPT TO PULL BSCSCAN ONE BY ONE AND DUMP TO WAREHOUSE
# purpose is to get the complete log transaction of zmt on bsc
#script adjustment to make capital to small letter

# bsc_mainnet = 'https://api.bscscan.com/'
# documentation = 'https://docs.bscscan.com/'
#limitation

#note:
# first zmt transact on BSC block = 11364869
#BUSD_contract = '0xe9e7cea3dedca5984780bafc599bd69add087d56'
#Pancakeswap_v2_zmt = '0x030576e072ad56c593116d5e0781d589dcf3abc9'

#for transaction use topic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
#for minting use topic = '0x2fe5be0146f74c5bce36c0b80911af6c7d86ff27e89d5cfa61fc681327954e5d'
#for burn use topic = '0xa78a9be3a7b862d26933ad85fb11d80ef66b8f972d7cbba06621d583943a4098'
#in burn what this topic do? 0x4599e9bf0d45c505e011d0e11f473510f083a4fdc45e3f795d58bb5379dbad68
### in burn first hex is burn amount
### in burn 5th and 6th hex is address (called operator)

from requests import get
import pprint
from datetime import datetime
import pandas as pd
import json
import math
# pd.io.json._json.loads = lambda s, *a, **kw: json.loads(s)
import psycopg2
from minh_cre import secrets
from csv import writer

API_KEY = secrets.get('bscscan_api')
zmt_address = "0xDE960267B9AabFb5404D9A566C1ED6DB9dB09522" #"0xaa602dE53347579f86b996D2Add74bb6F79462b2"
BASE_URL = "https://api.bscscan.com/api"
ETHER_VALUE = 10 ** 18

connect = psycopg2.connect(
    host=secrets.get('host'),
    database=secrets.get('database'),
    user=secrets.get('user'),
    password=secrets.get('password'),
    port=secrets.get('port')
    )

# print('---filling latest block into depart block------------')
INIT_FACTOR = 400000
DATA_LIMIT = 950
#
# arrival_block = latest_block() + INIT_FACTOR

def make_api_url(module, action, address, fromBlock, toBlock, **kwargs):
    url = BASE_URL + f"?module={module}&action={action}&address={address}&fromBlock={fromBlock}&toBlock={toBlock}&apikey={API_KEY}"

    for key, value in kwargs.items():
        url += f"&{key}={value}"

    return url

from ast import literal_eval

def get_logs(depart_block, INIT_FACTOR):
    arrival_block = depart_block + INIT_FACTOR
    get_log_url = make_api_url("logs", "getLogs", address=zmt_address, fromBlock=depart_block, toBlock=arrival_block)
    response = get(get_log_url)
    data = response.json()["result"]

    created_at = []
    zmt_k_address = []
    length = []
    topic = []
    from_address = []
    to_address = []
    zmt_amt = []
    transactionHash = []
    blockNumber = []
    logIndex = []
    transactionIndex =[]

    get_log_dict = {'created_at': created_at
        ,'zmt_k_address': zmt_k_address
        ,'topic': topic
        ,'length': length
        ,'from_address': from_address
        ,'to_address': to_address
        ,'zmt_amount': zmt_amt
        ,'transactionHash': transactionHash
        ,'blockNumber': blockNumber
        ,'logIndex': logIndex
        ,'transactionIndex': transactionIndex
                    }

    for lg in data:
        created_a = datetime.utcfromtimestamp(int(lg["timeStamp"], 16))
        # time= datetime.fromtimestamp(lg['timeStamp'])
        zmt_k_a = lg["address"]
        topi = lg["topics"][0]
        l1 = len(lg["topics"][0])

        if lg["topics"][0] == '0xa78a9be3a7b862d26933ad85fb11d80ef66b8f972d7cbba06621d583943a4098': #for burn
            from_add = lg["topics"][1]
            to_add = lg["topics"][2]
        elif lg["topics"][0] == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef': #for transaction
            from_add = lg["topics"][1]
            to_add = lg["topics"][2]
        elif lg["topics"][0] == '0x4599e9bf0d45c505e011d0e11f473510f083a4fdc45e3f795d58bb5379dbad68': #for mint >>assume. do not know for real
            from_add = lg["topics"][1]
            to_add = '099999999999999noaddress'
        else:
            from_add = '09999999999999999999999999999999999999999999999999999999996699999f'
            to_add = '09999999999999999999999999999999999999999999999999999999996699999f'

        if lg["topics"][0] == '0xa78a9be3a7b862d26933ad85fb11d80ef66b8f972d7cbba06621d583943a4098':
            zmt_a = int(lg["data"],16) #literal_eval(lg["data"]) / (10**320)
        elif lg["topics"][0] == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef':
            zmt_a = literal_eval(lg["data"]) / (10 ** 18)
        elif lg["topics"][0] == '0x4599e9bf0d45c505e011d0e11f473510f083a4fdc45e3f795d58bb5379dbad68':
            zmt_a = literal_eval(lg["data"]) / (10 ** 320)
        else:
            zmt_a = 0
        if lg["data"] == '0x':
            zmt_a = 0.000000000000000001
        elif literal_eval(lg["data"]) > 200000000000000000000000000:
            zmt_a = 0.000000000000000001
        else:
            zmt_a = literal_eval(lg["data"]) / ETHER_VALUE

        transactionH = lg["transactionHash"]
        blockN = lg["blockNumber"]
        logind = lg["logIndex"]
        transactionInd = lg["transactionIndex"]

        created_at.append(created_a)
        zmt_k_address.append(zmt_k_a)
        topic.append(topi)
        length.append(l1)
        from_address.append(from_add)
        to_address.append(to_add)
        zmt_amt.append(zmt_a)
        transactionHash.append(transactionH)
        blockNumber.append(blockN)
        logIndex.append(logind)
        transactionIndex.append(transactionInd)

    y = json.dumps(get_log_dict, indent=4, sort_keys=False, default=str)

    return y

# print(get_logs(11364869, 100000))

def get_etherscan_api_logs(**kwargs):
    def latest_block():
        conn = connect
        cur = conn.cursor()
        cur.execute('select '
                    'min("blocknumber"::dec(65,0))'
                    'from warehouse.data_team_staging.zmt_bscscan')
        max_block = cur.fetchone()[0]

        return max_block

    max_block = latest_block()
    api_data = get_logs(max_block, INIT_FACTOR)

    while (len(json.loads(api_data)['created_at']) > DATA_LIMIT):
            factor = math.floor(INIT_FACTOR * 0.5)
            print ('block incremental reduced from= ' + str(INIT_FACTOR), 'to= ' + str(factor))
            api_data = get_logs(max_block, factor)
            print('new data length= ' +str(len(json.loads(get_logs(factor))['created_at'])))

    logs_df = pd.read_json(api_data)

    print('---individual column formatting in pandas-------------')
    logs_df['length_from_address'] = logs_df['from_address'].str.len()
    logs_df['length_to_address'] = logs_df['to_address'].str.len()
    logs_df['from_address_use'] = '0x' + logs_df['from_address'].str[-40:].astype(str)
    logs_df['to_address_use'] = '0x' + logs_df['to_address'].str[-40:].astype(str)
    logs_df['blockNumber'] = logs_df['blockNumber'].apply(int, base=16)
    logs_df.drop(['length'], inplace=True, axis=1)
    return logs_df

# # print(get_etherscan_api_logs())
# get_etherscan_api_logs().to_csv('bsc_load.csv')
