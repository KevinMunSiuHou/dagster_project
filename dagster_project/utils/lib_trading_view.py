import json
import pandas as pd
from websocket import create_connection
import pytz
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

socket = 'wss://data.tradingview.com/socket.io/websocket'

def create_msg(ws, func, arg):
    ms = json.dumps({"m":func,"p":arg})
    msg = "~m~"+str(len(ms))+"~m~"+ms
    ws.send(msg)

def format_data(data):
    start = data.find('"s":[')
    end = data.find(',"ns":')
    list_data = json.loads(data[start+4:end])
    final_list = []
    for item in list_data:
        final_list.append(item['v'])
    final_df = pd.DataFrame(final_list)
    if not final_df.empty:
        final_df['created_at'] = final_df[0].apply(
            lambda ts: datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        )
    return final_df

def extract_data(symbol:str):
    ws = create_connection(socket)
    create_msg(ws, func="chart_create_session", arg= [os.getenv('token_trading_view'), ""])
    create_msg(ws, func="resolve_symbol", arg=["cs_OjFXwReC3y0p", "sds_sym_1", f"={{\"adjustment\":\"splits\",\"currency-id\":\"USD\",\"session\":\"regular\",\"symbol\":\"{symbol}\"}}"])
    create_msg(ws, func="create_series", arg= [os.getenv('token_trading_view'),"sds_1","s1","sds_sym_1","1",10000,""])
    create_msg(ws, func="create_study", arg=[os.getenv('token_trading_view'),"st9","st1","sds_1","BarSetContinuousRollDates@tv-corestudies-28",{"currenttime":"now"}])
    df = pd.DataFrame()
    while True:
        res = ws.recv()
        if "series_completed" in res:
            df = format_data(res)
            break
    return df
    