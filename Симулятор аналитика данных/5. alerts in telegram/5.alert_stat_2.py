import pandas as pd
from CH import Getch
import pandahouse
from datetime import date,timedelta, datetime
import telegram
import seaborn as sns
import matplotlib.pyplot as plt
import io
sns.set(style ='ticks')

def check_anomaly(df,metric, a1=3,a2=3, n=5, per = 0.1):
    # межквартильный размах
    df['25q'] =df[metric].shift(1).rolling(n).quantile(0.25)
    df['75q'] =df[metric].shift(1).rolling(n).quantile(0.75)
    df['IQR'] =df['75q'] - df['25q']
    df['minq'] = df['25q'] -  a1  * df['IQR']
    df['maxq'] = df['75q'] +  a1 * df['IQR']
    df['minq'] = df['minq'].rolling(n,center =True, min_periods =1).mean()
    df['maxq'] = df['maxq'].rolling(n,center =True, min_periods =1).mean()
   
    mapping = {True:1, False:0}
    df['is_alert_q'] = ((df[metric] < df['minq']) | (df[metric] > df['maxq'])).map(mapping) 
    
    #сигмы
    df['std'] = df[metric].shift(1).rolling(n).std()
    df['mean'] = df[metric].shift(1).rolling(n).mean()
    
    df['mins'] = df['mean'] - a2 * df['std']
    df['maxs'] = df['mean'] + a2 * df['std']
    
    df['mins'] = df['mins'].rolling(n,center =True, min_periods =1).mean()
    df['maxs'] = df['maxs'].rolling(n,center =True, min_periods =1).mean() 
    
    df['is_alert_s'] = ((df[metric] < df['mins']) | (df[metric] > df['maxs'])).map(mapping) 
    
    if (df['is_alert_s'].iloc[-1] == 1) and (df['is_alert_q'].iloc[-1] == 1):          #df[metric].iloc[-1] < df['minq'].iloc[-1]  or df[metric].iloc[-1] > df['maxq'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df


def run_alerts(chat=None):
    #подключаемся к телеге
    chat_id = chat or 400853280
    token = '5368668226:AAGZNDxvxf83GApSiJqi5ahEkIHDXzV52Nw'
    bot = telegram.Bot(token = token)
    #выгружаем данные из базы
    data_from_base = Getch(
            '''
            with f as (
                    select 
                        toStartOfFifteenMinutes(time) as FifteenMinutes
                        ,toDate(FifteenMinutes) as date
                        ,formatDateTime(FifteenMinutes, '%R') as hm
                        ,count(distinct user_id) as dau_news
                        ,countIf(post_id, action = 'view') as view
                        ,countIf(post_id, action = 'like') as like
                        ,countIf(post_id, action = 'like')/countIf(post_id, action = 'view') as CTR
                    from simulator_20220520.feed_actions
                    where time < toStartOfFifteenMinutes(now())
                    group by FifteenMinutes,date,hm
            ),
            m as (select 
                        toStartOfFifteenMinutes(time) as FifteenMinutes
                        ,count(distinct user_id) as dau_message
                        ,count(user_id) as count_message 
                    from simulator_20220520.message_actions
                    where time < toStartOfFifteenMinutes(now())
                    group by FifteenMinutes
            )

            select f.FifteenMinutes as FifteenMinutes
                ,f.date as date
                ,f.hm as hm
                ,f.dau_news as dau_news
                ,f.view as view
                ,f.like as like
                ,f.CTR as CTR
                ,m.dau_message as dau_message
                ,m.count_message as count_message
                ,now() as now
            from f
            --where time < toStartOfFifteenMinutes(now())
            --toDate(time) = today() or toDate(time) = today()-1 or toDate(time) = today() -7
            full join m 
            using FifteenMinutes
            where FifteenMinutes > today() -1 --and FifteenMinutes <= '2022-06-07 11:15:00'
            order by FifteenMinutes
            '''
            ).df    
#маркеры времени для проверки
    voc = pd.DataFrame(
       [['dau_news','http://superset.lab.karpov.courses/r/1241', 'https://superset.lab.karpov.courses/superset/dashboard/988/',3,3,5,0.1], 
       ['view','http://superset.lab.karpov.courses/r/1243',  'https://superset.lab.karpov.courses/superset/dashboard/988/',3,3,5,0.1], 
       ['like','http://superset.lab.karpov.courses/r/1242',  'https://superset.lab.karpov.courses/superset/dashboard/988/',3,3,5,0.1], 
       ['CTR','http://superset.lab.karpov.courses/r/1244',  'https://superset.lab.karpov.courses/superset/dashboard/988/',4,3,5,0.1], 
       ['dau_message','http://superset.lab.karpov.courses/r/1245',  'https://superset.lab.karpov.courses/superset/dashboard/988/',1.5,3,5,0.1], 
       ['count_message','http://superset.lab.karpov.courses/r/1246',  'https://superset.lab.karpov.courses/superset/dashboard/988/',2,3,7,0.1]]\
        , columns=['index','Chart_url','Dash_url','a1','a2','n','thereshold'])

    metrics_list =['dau_news','view','like','CTR','dau_message','count_message']
    for metric in metrics_list:
        df = data_from_base[['FifteenMinutes','date','hm','now',metric]].copy()
        voc_new = voc[voc['index'] == metric]
        chart = voc_new['Chart_url'].iloc[-1]
        dash = voc_new['Dash_url'].iloc[-1]
        a1 = voc_new['a1'].iloc[-1]
        a2 = voc_new['a2'].iloc[-1]        
        n = voc_new['n'].iloc[-1]
        now = data_from_base['FifteenMinutes'].max()
        #df =  df[df.date == today]
        is_alert, df = check_anomaly(df, metric,a1,a2,n)
        
        if is_alert == 1:
            msg = '''
                Показатель {metric} на {time}:
текущее значение = {current_value:.2f}
отклонение от предыдущего значения  {diff:.2%}
<a href='{chart}' >график </a> - <a href='{dash}' >дашборд </a>
@jeka_pe4enka
                '''.format(metric = metric
                            ,time = now
                           ,current_value = df[metric].iloc[-1]
                           ,diff = 1- (df[metric].iloc[-1]/df[metric].iloc[-2])
                           ,chart = chart
                           ,dash = dash
                          )
        #рисуем график
            #metric = df[0]
            plt.figure(figsize=(16, 10))
            plt.tight_layout()
            ax = sns.lineplot(x=df['FifteenMinutes'], y=df[metric],label = 'metric')
            ax = sns.lineplot(x=df['FifteenMinutes'], y=df['minq'],label = 'up')
            ax = sns.lineplot(x=df['FifteenMinutes'], y=df['maxq'],label = 'down')
        # этот цикл нужен чтобы разрядить подписи координат по оси Х
            for ind, label in enumerate(ax.get_xticklabels()): 
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
                ax.set(xlabel='time') # задаем имя оси Х
                ax.set(ylabel=metric) # задаем имя оси У
                ax.set_title('{}'.format(metric)) # задаем заголовок графика
                ax.set(ylim=(0, None)) # задаем лимит для оси У


            # формируем файловый объект
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            # отправляем алерт
            #bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption = msg, parse_mode ='HTML')
try:
    run_alerts()
except Exception as e:
    print(e)  
