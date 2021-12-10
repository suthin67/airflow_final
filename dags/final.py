from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook

default_args = {
    'start_date': datetime(2021, 1, 1)
}
@dag('python_dag', schedule_interval='@daily', default_args=default_args, catchup=False)
def myTask():
    @task
    def extract_from_binance():
        import pandas as pd
        import requests
        res = requests.get('https://api.binance.com/api/v3/exchangeInfo')
        columns = ['symbol', 'baseAsset', 'quoteAsset']
        data = []
        for symbo in res.json()['symbols']:
            #print(symbo['symbol'], symbo['baseAsset'], symbo['quoteAsset'])
            data.append([symbo['symbol'], symbo['baseAsset'], symbo['quoteAsset']])
        df = pd.DataFrame(data, columns=columns)
        #print(df.describe())
        return df.to_json()
    @task
    def transform_to_crosstab(val):
        import pandas as pd
        df = pd.read_json(val)
        df2 = pd.crosstab(df.baseAsset, df.quoteAsset)
        return df2.to_json()
    @task
    def transform_to_watchlist(val):
        import pandas as pd
        df2 = pd.read_json(val)
        return {'watchlist': list(df2[df2['USDT'] == 0].index)}
    @task
    def load_to_mysql(val):
        #from airflow.providers.mysql.hooks.mysql import MySqlHook
        hook = MySqlHook(mysql_conn_id='mysql_testdb')
        print(hook.get_pandas_df("SELECT NOW()"))
    
    load_to_mysql(transform_to_watchlist(transform_to_crosstab(extract_from_binance())))

dag = myTask()