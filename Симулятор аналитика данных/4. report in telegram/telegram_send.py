import telegram
#import python-telegram-bot
import io
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from read_db.CH import Getch
import os
import pandas as pd
import pandahouse
#import logging


sns.set()


def send_report(chat=None):
    chat_id = chat or 400853280
    token = os.environ.get('REPORT_BOT_TOKEN')
    bot = telegram.Bot(token = token) 
    
    #отправка текста
    msg = 'Hello'
    bot.sendMessage(chat_id = chat_id, text = msg)
    
    #отправка картинки
    x = np.arange(1,10,1)
    y = np.random.choice(5,len(x))
    
    sns.lineplot(x,y)
    plt.title('Тестовый график')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'test_plot.png'
    #Перемещает указатель чтения/записи в файле.
    plot_object.seek(0)
    plt.close

    bot.sendPhoto(chat_id = chat_id, photo = plot_object)
    
    #отправка файла
    data = Getch('select * from simulator_20220520.feed_actions where toDate(time) = today() limit 1000').df
    file_object = io.StringIO()
    data.to_csv(file_object)
    file_object.seek(0)

    file_object.name = 'test_data.csv'
    bot.sendDocument(chat_id=chat_id,document =file_object)
    
    
    
#logging.basicConfig(level = 'INFO',filename = 'logdata.txt')
#now =pd.Timestamp('now')
#logging.info('TEST REPORT START BUILDING {}'.format(now))
try:
    send_report()
    #logging.info('TEST REPORT SENT')

except Exception as e:
    #logging.exception(e)
    print(e)
