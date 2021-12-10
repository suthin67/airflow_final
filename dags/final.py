from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pandas.io import sql

default_args = {
    'start_date': datetime(2021, 4, 1)
}
@dag('Final_Exam', schedule_interval='@daily', default_args=default_args, catchup=False)
def myTask():
    @task
    def extract():
        import pandas as pd
        import requests
        import json
        target_url = 'https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all'
        raw_json = requests.get(target_url).text
        data = json.loads(raw_json)
        df = pd.DataFrame(data)
        #print(df.describe())
        return df.to_json()
    @task
    def transform(val):
        import pandas as pd
        import json
        df1 = pd.read_json(val)
        a = round(((df1.new_recovered/df1.total_case)*100),2)
        b = round(((df1.new_case/df1.total_case)*100),2)
        d = {'Date': df1.txn_date ,'recovered': a, 'new_case': b} 
        df2 = pd.DataFrame(d)
        return df2.to_json()
        
    @task
    def load_to_mysql(val):
        import pandas as pd
        import json
        hook = MySqlHook(mysql_conn_id='mysql_testdb')
        conn = hook.get_conn()
        c = conn.cursor()
        df3 = pd.read_json(val)

        c.execute('''
        CREATE TABLE IF NOT EXISTS covid 
        (YYYY_MM_DD DATE NOT NULL, 
        recovered_percent decimal(4,2) NOT NULL, 
        new_case_percent decimal(4,2) NOT NULL);''')
        
        for index, row in df3.iterrows():
            query = """
            INSERT INTO covid (YYYY_MM_DD, recovered_percent, new_case_percent)
            VALUES ('{Date}',{recovered},{new_case})
            """.format(Date = row['Date'], recovered = row['recovered'], new_case = row['new_case'])

            hook.run(sql=query)
        
    load_to_mysql(transform(extract()))

dag = myTask()