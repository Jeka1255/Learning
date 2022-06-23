import pandas as pd
from CH import Getch
import pandahouse
from datetime import date,timedelta, datetime
import telegram
import seaborn as sns
import matplotlib.pyplot as plt
import io
sns.set(style ='ticks')

#маркеры времени
#если алерт запустили дополнительно в другое время, берется последняя "полная" запись
def ceil_dt(dt, delta):
    dt = dt + timedelta(hours =3)
    return dt - (dt - datetime.min) % delta

now = datetime.now()
now_time = ceil_dt(now, timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")

yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
today = date.today().strftime('%Y-%m-%d')
last_week = (date.today() - timedelta(days=7)).strftime('%Y-%m-%d')
period = [today,yesterday,last_week]

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
                    group by FifteenMinutes,date,hm
            ),
            m as (select 
                        toStartOfFifteenMinutes(time) as FifteenMinutes
                        ,count(distinct user_id) as dau_message
                        ,count(user_id) as count_message 
                    from simulator_20220520.message_actions 
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
            from f
            --where toDate(time) = today() or toDate(time) = today()-1 or toDate(time) = today() -7
            full join m 
            using FifteenMinutes
            '''
            ).df
    
    #справочник ссылок
    voc = pd.DataFrame(
       [['dau_news','http://superset.lab.karpov.courses/r/1241', 'https://superset.lab.karpov.courses/superset/dashboard/988/'], 
       ['view','http://superset.lab.karpov.courses/r/1243',  'https://superset.lab.karpov.courses/superset/dashboard/988/'], 
       ['like','http://superset.lab.karpov.courses/r/1242',  'https://superset.lab.karpov.courses/superset/dashboard/988/'], 
       ['CTR','http://superset.lab.karpov.courses/r/1244',  'https://superset.lab.karpov.courses/superset/dashboard/988/'], 
       ['dau_message','http://superset.lab.karpov.courses/r/1245',  'https://superset.lab.karpov.courses/superset/dashboard/988/'], 
       ['count_message','http://superset.lab.karpov.courses/r/1246',  'https://superset.lab.karpov.courses/superset/dashboard/988/']]\
        , columns=['Metrics','Chart_url','Dash_url'])
    #порог
    thereshold = 0.2
    
    #маркеры времени для проверки
    check_time = data_from_base[data_from_base["FifteenMinutes"] < now_time].FifteenMinutes.max()
    check_time_yesterday = check_time - timedelta(days=1)
    check_time_last_week = check_time - timedelta(days=7)
    check_time_last_minutes = check_time - timedelta(minutes=15)
    check_minutes = data_from_base[data_from_base["FifteenMinutes"] ==  check_time].hm.max()
    
    # статистические показатели
    std = data_from_base[['dau_news','view','like','CTR','dau_message','count_message']][(data_from_base["hm"] == check_minutes) & (data_from_base["date"] > last_week)]\
    .agg(['std','mean']).T.reset_index() #.rename(columns={0:'std'})
    std['mins'] = (std['mean'] - 3*std['std']).round(0)
    std['maxs'] = (std['mean'] + 3*std['std']).round(0)
    
    quantile = data_from_base[(data_from_base["hm"] == check_minutes) & (data_from_base["date"] > last_week)].quantile([0.25,0.5,0.75]).T
    quantile['IQR'] =(quantile[0.75] - quantile[0.25])
    quantile = quantile.reset_index().rename(columns = {0.25:'25q',0.75:'75q',0.5:'50q'})
    quantile['minq'] = quantile['25q'] -  quantile['IQR']
    quantile['maxq'] = quantile['75q'] +  quantile['IQR']
    
    
    #обработка данных:оставляем текущее время и время, с которым идет сверка
    data = data_from_base
    data = data[(data["FifteenMinutes"] == check_time) | \
     (data["FifteenMinutes"] == check_time_yesterday) | \
     (data["FifteenMinutes"] == check_time_last_week)| \
     (data["FifteenMinutes"] == check_time_last_minutes)] 
    
    data['type'] = data['FifteenMinutes'].apply(lambda x: "1.today" if x == check_time else\
                                                    ("3.yesteday" if x == check_time_yesterday else \
                                                    ("4.last_week"if x == check_time_last_week else \
                                                    "2.minutes")))
    #разворачиваем таблицу и делаем проверку
    pivot_data = data.drop(columns=['FifteenMinutes','date','hm'])\
                        .sort_values('type').set_index('type').T.reset_index()
    
   #объединяем таблицы
    pivot_data = pivot_data.merge(std).merge(quantile)
    mapping = {True:1, False:0}

    pivot_data['dif_t_m'] = (pivot_data['1.today']/pivot_data['2.minutes']-1)
    pivot_data['dif_t_y'] = (pivot_data['1.today']/pivot_data['3.yesteday']-1)
    pivot_data['dif_t_w'] = (pivot_data['1.today']/pivot_data['4.last_week']-1)
    pivot_data['is_alert_d'] = pivot_data['dif_t_y'].apply(lambda x: 1 if x >= abs(thereshold) else 0) #предыдущий день
    pivot_data['is_alert_w'] = pivot_data['dif_t_w'].apply(lambda x: 1 if x >= abs(thereshold) else 0) #прошлая неделя
    pivot_data['is_alert_q'] = ((pivot_data['1.today'] < pivot_data['minq']) & (pivot_data['1.today'] > pivot_data['maxq'])).map(mapping) #квантили
    pivot_data['is_alert_s'] = ((pivot_data['1.today'] < pivot_data['mins']) & (pivot_data['1.today'] > pivot_data['maxs'])).map(mapping) #сигмы
    pivot_data['is_alert'] = ((pivot_data['is_alert_d']+pivot_data['is_alert_w']+pivot_data['is_alert_q']+pivot_data['is_alert_s']) >=3 ).map(mapping) #сигнализируем, если три и больше проверки показали алерт
    
    #отбираем показатели, по которым нужен алерт
    alert_data = pivot_data[(pivot_data['is_alert'] == 1)]\
                    .reset_index()
    
    #проходим по метрикам и делаем рассылку алертов
    for row in alert_data.index:
        df = alert_data.loc[row]
        
        #if df[8] == 1:
        #    msg = '''
        #        Показатель {metric}:
        #        текущее значение = {current_value:.2f}
        #        отклонение от вчера {diff:.2%}'''.format(metric = df[0]
        #                                                 ,current_value = df[1]
        #                                                 ,diff = df[6])
        #else:    
        #    msg = '''
        #        Показатель {metric}:
        #        текущее значение = {current_value:.2f}
        #        отклонение от прошлой недели {diff:.2%}'''.format(metric = df[0]
        #                                                 ,current_value = df[1]
        #                                                 ,diff = df[7])
        
        msg = '''
        Показатель {metric}:
        текущее значение = {current_value:.2f}
        отклонение от вчера {diff:.2%}
        отклонение от прошлой недели {diff2:.2%}
        '''.format(metric = df[0]
                   ,current_value = df[1]
                   ,diff = df[6]
                   ,diff2 = df[7])
        #рисуем график
        metric = df[0]
        plt.figure(figsize=(16, 10))
        plt.tight_layout()
        ax = sns.lineplot(data=data_from_base[data_from_base.date.isin(period)].sort_values(by=['date', 'hm']), 
                      x="hm", y=metric, hue="date")
        # этот цикл нужен чтобы разрядить подписи координат по оси Х
        for ind, label in enumerate(ax.get_xticklabels()): 
            if ind % 15 == 0:
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
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

try:
    run_alerts()
except Exception as e:
    print(e)
