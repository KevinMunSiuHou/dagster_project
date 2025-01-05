from dagster import asset, AssetExecutionContext
import sys
sys.path.append('././dagster_project')
from utils.lib_trading_view import extract_data
from utils.lib_sql import pg_connect
from utils.lib_shared import read_sql_file
import pandas as pd

@asset 
def extract_trading_view_xauusd(context: AssetExecutionContext):
    df = extract_data(symbol='OANDA:XAUUSD')
    if not df.empty:
        context.log.info(df.tail())
        return df
    return pd.DataFrame()

@asset 
def compare_trading_view_xauusd(context: AssetExecutionContext, extract_trading_view_xauusd):
    df_now = extract_trading_view_xauusd
    df_pre = pg_connect().read_data(read_sql_file('query',{'columns':'max(created_at)','tableName':'trading_view.market_data','conditions':"WHERE symbol = 'XAUUSD'"}))
    max_date = df_pre['max'][0].strftime('%Y-%m-%d %H:%M:%S')
    if not df_now.empty:
        df_now = df_now[df_now['created_at']>=max_date]
        df_now.columns = df_now.columns.astype(str)
        df_now = df_now.rename(columns={'0':'event_timestamp','1':'open','2':'high','3':'low','4':'close','5':'volume'})
        df_now['symbol'] = 'XAUUSD'
        context.log.info(df_now.tail())
        pg_connect().execute(read_sql_file('delete',{'tableName':'trading_view.market_data','conditions':f"WHERE created_at = '{max_date}' and symbol = 'XAUUSD'"}))
        pg_connect().bulk_update(df_now, **{'tableName':'market_data','schema':'trading_view'})
        return True
    return False

@asset 
def extract_trading_view_btcusd(context: AssetExecutionContext):
    df = extract_data(symbol='BITSTAMP:BTCUSD')
    if not df.empty:
        context.log.info(df.tail())
        return df
    return pd.DataFrame()

@asset 
def compare_trading_view_btcusd(context: AssetExecutionContext, extract_trading_view_btcusd):
    df_now = extract_trading_view_btcusd
    df_pre = pg_connect().read_data(read_sql_file('query',{'columns':'max(created_at)','tableName':'trading_view.market_data','conditions':"WHERE symbol = 'BTCUSD'"}))
    max_date = df_pre['max'][0].strftime('%Y-%m-%d %H:%M:%S')
    if not df_now.empty:
        df_now = df_now[df_now['created_at']>=max_date]
        df_now.columns = df_now.columns.astype(str)
        df_now = df_now.rename(columns={'0':'event_timestamp','1':'open','2':'high','3':'low','4':'close','5':'volume'})
        df_now['symbol'] = 'BTCUSD'
        context.log.info(df_now.tail())
        pg_connect().execute(read_sql_file('delete',{'tableName':'trading_view.market_data','conditions':f"WHERE created_at = '{max_date}' and symbol = 'BTCUSD'"}))
        pg_connect().bulk_update(df_now, **{'tableName':'market_data','schema':'trading_view'})
        return True
    return False
