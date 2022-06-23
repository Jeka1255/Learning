import pandas as pd
#from CH import Getch
import pandahouse
from datetime import date, timedelta, datetime
import telegram
import io
import numpy as np

from airflow import DAG
from airflow.operators.python_operator import PythonOperator  # Так как мы пишет такси в питоне
from airflow.decorators import dag, task

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-orlova-7',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 2),
}

# Интеравал запуска Даг
schedule_interval = '0 12 * * *'
#dag = DAG('dag_orlova', default_args=default_args, schedule_interval=schedule_interval)
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'}

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_orlova():
    @task(retries=2)
    def foo1():
        q_feed = '''
            select user_id,source ,gender, age, os
                        ,multiIf(age = 0, 'no info',
                     age <=20 , '1-20',
                     age <=30 , '21-30',
                     age <=40 , '31-40',
                     age <=50 , '41-50',
                     '50+') as age_type
                    ,toDate(time) as yesterday
                    ,countIf(post_id,action ='like') as likes
                    ,countIf(post_id,action='view') as views
            from simulator_20220520.feed_actions
            where toDate(time) = today()-1
            group by user_id, source, gender, age, os, toDate(time)
            '''

        feed = pandahouse.read_clickhouse(q_feed, connection=connection)
        return feed
    @task(retries=2)
    def foo2():
        #TASK 2
        q_message = (
        '''
        with s as (select user_id, os,age,gender
                         , toDate(time) as yesterday
                         , count(reciever_id) as messages_sent
                         , count(distinct reciever_id) as users_sent
                    from simulator_20220520.message_actions 
                    where toDate(time) =today()-1
                    group by user_id, os,age,gender,yesterday)
        , r as (select reciever_id
                         , toDate(time) as yesterday
                         , count(user_id) as messages_received
                         , count(distinct user_id) as users_received
                    from simulator_20220520.message_actions 
                    where toDate(time) =today()-1
                    group by reciever_id,yesterday)


        select if(user_id = 0,reciever_id, user_id) as user_id
        ,if(os='','no info',os) as os
        ,age
        ,multiIf(age = 0, 'no info',
             age <=20 , '1-20',
             age <=30 , '21-30',
             age <=40 , '31-40',
             age <=50 , '41-50',
             '50+') as age_type
        ,if(age=0,2,gender) as gender
        ,if(s.yesterday ='1970-01-01',r.yesterday,s.yesterday) as yesterday
        ,messages_sent
        ,users_sent
        ,messages_received
        ,users_received
        from s
        full join r on r.reciever_id = s.user_id
        '''
        )
        message = pandahouse.read_clickhouse(q_message, connection=connection)
        return message
    @task(retries=2)
    def foo3(feed,message):
        df = feed.merge(message, left_on = ['user_id','os','age','age_type','gender','yesterday'], right_on =['user_id','os','age','age_type','gender','yesterday'], how ='outer').fillna(0)
        return df
    @task(retries=2)
    def foo4(df):
        #TASK 4 - пол
        df_gender = df[['gender','yesterday','likes','views','messages_sent','users_sent','messages_received','users_received']].groupby(['yesterday','gender'], as_index=False).agg('sum')
        df_gender['metric'] ='gender'
        df_gender = df_gender.rename(columns = {'gender':'metric_value'})
        df_gender['metric_value']= df_gender['metric_value'].apply(lambda x: 'no info' if x == 2 else x)
        return df_gender
    @task(retries=2)
    def foo5(df):
        #TASK 5 - возраст
        df_age = df[['age_type','yesterday','likes','views','messages_sent','users_sent','messages_received','users_received']].groupby(['yesterday','age_type'], as_index=False).agg('sum')
        df_age['metric'] ='age_type'
        df_age = df_age.rename(columns = {'age_type':'metric_value'})
        return df_age
    @task(retries=2)
    def foo6(df):
        #TASK 6 - os
        df_os = df[['os','yesterday','likes','views','messages_sent','users_sent','messages_received','users_received']].groupby(['yesterday','os'], as_index=False).agg('sum')
        df_os['metric'] ='os'
        df_os = df_os.rename(columns = {'os':'metric_value'})
        return df_os
    @task(retries=2)
    def foo7(df_gender,df_os,df_age):
        #TASK 7
        df_final = pd.concat([df_gender,df_os,df_age])
        df_final = df_final[['yesterday','metric','metric_value','likes','views','messages_sent','users_sent','messages_received','users_received']].rename(columns={'yesterday':'eventdate'})
        df_final[['likes','views','messages_sent','users_sent','messages_received','users_received']] = df_final[['likes','views','messages_sent','users_sent','messages_received','users_received']].astype(int)
        return df_final
    @task(retries=2)
    def into_base(df_final):
        #подключение к новой базе
        connection_ch = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': '656e2b0c9c',
            'user': 'student-rw',
            'database': 'test'}

        q1 = '''
        CREATE TABLE IF NOT EXISTS test.eorlova
        (eventdate Date
        , metric String
        , metric_value String
        , likes UInt64
        , views UInt64
        , messages_sent UInt64
        , users_sent UInt64
        , messages_received UInt64
        , users_received UInt64) 
        ENGINE = MergeTree()
        ORDER BY eventdate
        '''
        pandahouse.execute(query=q1, connection=connection_ch)

        q2 = 'SELECT max(eventdate) as eventdate FROM test.eorlova'
        max_date = pandahouse.read_clickhouse(q2, connection=connection_ch)
        max_date = max_date.iloc[0]

        if (max_date == df_final.eventdate.max())[0]:
            q3 = '''
            ALTER TABLE test.eorlova DELETE WHERE eventdate == today()-1
            '''
            pandahouse.execute(query=q3, connection=connection_ch)
            pandahouse.to_clickhouse(df_final, table='eorlova',index=False, connection=connection_ch)
        else:
            pandahouse.to_clickhouse(df_final, table='eorlova',index=False, connection=connection_ch)

        return 
    print('ok')
    df =foo3(foo1(),foo2())
    into_base(foo7(foo4(df),foo5(df),foo6(df)))

    #with DAG(
    #    'dag_orlova',default_args=default_args, schedule_interval=schedule_interval, catchup=False) as dag:

#t1 = PythonOperator(task_id='foo1',  # Название таска
#                            python_callable=foo1,  # Название функции
#                            dag=dag)  # Параметры DAG

#t2 = PythonOperator(task_id='foo2',  # Название таска
#                            python_callable=foo2,  # Название функции
#                            dag=dag)  # Параметры DAG

#t3 = PythonOperator(task_id='foo3',  # Название таска
#                            python_callable=foo3,  # Название функции
#                            dag=dag)  # Параметры DAG

#t4 = PythonOperator(task_id='foo4',  # Название таска
#                            python_callable=foo4,  # Название функции
#                            dag=dag)  # Параметры DAG

#t5 = PythonOperator(task_id='foo5',  # Название таска
#                            python_callable=foo5,  # Название функции
#                            dag=dag)  # Параметры DAG

#t6 = PythonOperator(task_id='foo6',  # Название таска
#                            python_callable=foo6,  # Название функции
#                            dag=dag)  # Параметры DAG

#t7 = PythonOperator(task_id='foo7',  # Название таска
#                            python_callable=foo7,  # Название функции
#                            dag=dag)  # Параметры DAG

#t8 = PythonOperator(task_id='into_base',  # Название таска
#                            python_callable=into_base,  # Название функции
#                            dag=dag)  # Параметры DAG

#[t1, t2] >> t3 >> [t4, t5, t6] >> t7 >> t8

dag_orlova = dag_orlova()
