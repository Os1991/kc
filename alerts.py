import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import date
import io
from read_db.CH import Getch
import sys
import os
import scipy.stats

def check_anomaly(data, metric, threshold):
    current_ts = data['ts'].max()
    day_ago_ts = current_ts - pd.DateOffset(days=1)
    current_value = data[data['ts'] == current_ts][metric].values[0]
    yesterday_value = data[data['ts'] == day_ago_ts][metric].values[0]
    
    if current_value <= yesterday_value:
        diff = abs(current_value / yesterday_value - 1)
    else:
        diff = abs(yesterday_value / current_value - 1)

    diff = round(diff, 4)
    if diff > threshold:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, current_value, yesterday_value, diff

def check_anomaly_15min(data, metric, threshold_15):
    current_ts = data['ts'].max()
    current_value = data[data['ts'] == current_ts][metric].values[0]
    min15_value = data[metric].values[-2]
    
    if current_value <= min15_value:
        diff_15 = abs(current_value / min15_value - 1)
    else:
        diff_15 = abs(min15_value / current_value - 1)

    diff_15 = round(diff_15, 4)
    if diff_15 > threshold_15:
        is_alert_15 = 1
    else:
        is_alert_15 = 0

    return is_alert_15, min15_value, diff_15

def check_anomaly_qn(data, metric, a_qn=3, n_qn=6):
    data['q25'] = round(data[metric].shift(1).rolling(n_qn).quantile(0.25), 2)
    data['q75'] = round(data[metric].shift(1).rolling(n_qn).quantile(0.75), 2)
    data['iqr'] = data['q75'] - data['q25']
    data['up_qn'] = data['q75']+a_qn*data['iqr']
    data['low_qn'] = data['q25']-a_qn*data['iqr']
    data['up_qn'] = data['up_qn'].rolling(n_qn, center=True, min_periods=1).mean()
    data['low_qn'] = data['low_qn'].rolling(n_qn, center=True, min_periods=1).mean()
    current_value = data[metric].values[-1]
    current_up = data['up'].values[-1]
    current_low = data['low'].values[-1]
    data_qn = data
    if current_value > current_up or current_value < current_low:
        is_alert_qn = 1
    else:
        is_alert_qn = 0
        
    return is_alert_qn, data_qn
        
def check_anomaly_std(data, metric, a=3, n=6):
    data['roll_mean'] = round(data[metric].shift(1).rolling(n, min_periods=1).mean(), 2)
    data['roll_std'] = round(data[metric].shift(1).rolling(n, min_periods=1).std(), 2)
    data['up'] = data['roll_mean']+a*data['roll_std']
    data['low'] = data['roll_mean']-a*data['roll_std']
    data['up'] = data['up'].rolling(8, center=True, min_periods=1).mean()
    data['low'] = data['low'].rolling(8, center=True, min_periods=1).mean()
    current_ts = data['ts'].max()
    current_value = data[data['ts'] == current_ts][metric].values[0]
    current_up = data[data['ts'] == current_ts]['up'].values[0]
    current_low = data[data['ts'] == current_ts]['low'].values[0]
    data_std = data
    if current_value > current_up or current_value < current_low:
        is_alert_std = 1
    else:
        is_alert_std = 0


    return is_alert_std, data_std

def check_anomaly_confidence(data, metric, n_conf=6):
    data['roll_mean'] = round(data[metric].shift(1).rolling(n_conf, min_periods=1).mean(), 2)
    data['roll_sem'] = round(data[metric].shift(1).rolling(n_conf, min_periods=1).sem(), 2)
    data['h'] =  data['roll_sem'] * scipy.stats.t.ppf(1.99 / 2, n_conf-1)
    data['up_conf'] = data['roll_mean'] + (data['h'])
    data['low_conf'] = data['roll_mean'] -  (data['h'])
    data['up_conf'] = data['up_conf'].rolling(6, center=True, min_periods=1).mean()
    data['low_conf'] = data['low_conf'].rolling(6, center=True, min_periods=1).mean()
    current_ts = data['ts'].max()
    conf_up = data[data['ts'] == current_ts]['up_conf'].values[0]
    conf_low = data[data['ts'] == current_ts]['low_conf'].values[0]
    current_value = data[data['ts'] == current_ts][metric].values[0]
    data_conf = data
    if current_value > conf_up or current_value < conf_low:
        is_alert_conf = 1
    else:
        is_alert_conf = 0

    return is_alert_conf, data_conf



def run_alerts(chat=None):
    chat_id = chat or -xxxxxxxxxxxxxxxxx
    bot = telegram.Bot(token='xxxxxxxxxxxxxxxxxxxxxxxxxx')

    data = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , toDate(ts) as date
                        , formatDateTime(ts, '%R') as hm
                        , uniqExact(user_id) as users
                        , (countIf(user_id, action='like')/countIf(user_id, action='view'))*100 CTR
                        , (countIf(DISTINCT user_id, os='Android')/users)*100 android_users
                        , (countIf(DISTINCT user_id, source='ads')/users)*100 ads_users
                        , round(count(user_id)/uniqExact(user_id), 2) interactions
                    FROM simulator_20220320.feed_actions
                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts ''').df

    data_mess = Getch(''' SELECT
                          toStartOfFifteenMinutes(time) as ts
                        , toDate(ts) as date
                        , formatDateTime(ts, '%R') as hm
                        , count(DISTINCT user_id) messages
                    FROM simulator_20220320.message_actions
                    WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm
                    ORDER BY ts
    ''').df
    
    metrics = ['users', 'CTR', 'interactions', 'android_users', 'ads_users', 'messages']
    dashboard = 'http://superset.lab.karpov.courses/r/704'
    
    for metric in metrics:
        if metric == 'users':
            n=6
            n_qn = 6
            n_conf = 4
            threshold = 0.4
            threshold_15 = 0.30
            a=4.5
            a_qn = 4.5
            chart = 'http://superset.lab.karpov.courses/r/692'
        elif metric == 'CTR':
            n=5
            n_qn = 8
            n_conf = 4
            threshold = 0.25
            threshold_15 = 0.25
            a=4
            a_qn = 4
            chart = 'http://superset.lab.karpov.courses/r/693'
        elif metric == 'interactions':
            n=6
            n_qn = 8
            n_conf = 3
            threshold = 0.3
            threshold_15 = 0.3
            a=5
            a_qn = 6
            chart = 'http://superset.lab.karpov.courses/r/694'
        elif metric == 'android_users':
            n=6
            n_qn = 8
            n_conf = 3
            threshold = 0.25
            threshold_15 = 0.25
            a=4
            a_qn = 4
            chart = 'http://superset.lab.karpov.courses/r/701'
        elif metric == 'ads_users':
            n = 6
            n_qn = 8
            n_conf = 3
            threshold = 0.25
            threshold_15 = 0.25
            a=4
            a_qn = 5
            chart = 'http://superset.lab.karpov.courses/r/702'
        else:
            data = data_mess
            n=10
            n_qn = 12
            n_conf = 3
            threshold = 0.4
            threshold_15 = 0.4
            a=5
            a_qn = 4
            chart = 'http://superset.lab.karpov.courses/r/703'
            
        is_alert, current_value, yesterday_value, diff = check_anomaly(data, metric, threshold)
        is_alert_15, min15_value, diff_15 = check_anomaly_15min(data, metric, threshold_15)
        is_alert_std, data_std = check_anomaly_std(data, metric, a, n)
        is_alert_conf, data_conf = check_anomaly_confidence(data, metric, n_conf)
        is_alert_qn, data_qn = check_anomaly_qn(data, metric, a_qn, n_qn)
        alerts = is_alert + is_alert_qn + is_alert_std + is_alert_conf + is_alert_15
        msg = f'Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от вчерашнего {diff:.2%}\nотклонение за последние 15 минут {diff_15:.2%}\n\
Chart {chart}\nDashboard {dashboard}\nКоличество сработавших сигналов {alerts} из 5\nday {is_alert} qn {is_alert_qn} std {is_alert_std} conf {is_alert_conf} 15_min {is_alert_15}'
        
        if (is_alert_qn+is_alert_std+is_alert_conf)>0 and (is_alert_15+is_alert)>0:
            bot.sendMessage(chat_id=chat_id, text=msg)
            if is_alert_conf:
                data = data_conf
                sns.set(rc={'figure.figsize': (12, 8)})
                plt.tight_layout()
                ax = sns.lineplot(x=data['ts'], y=data[metric], label='metric')
                ax = sns.lineplot(x=data['ts'], y=data['up_conf'], label='up_conf')
                ax = sns.lineplot(x=data['ts'], y=data['low_conf'], label='low_conf')

                ax.set(xlabel='time')
                ax.set_title(metric+' alert confidence')

                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                
            elif is_alert_std:
                data = data_std
                sns.set(rc={'figure.figsize': (12, 8)})
                plt.tight_layout()
                ax = sns.lineplot(x=data['ts'], y=data[metric], label='metric')
                ax = sns.lineplot(x=data['ts'], y=data['up'], label='up')
                ax = sns.lineplot(x=data['ts'], y=data['low'], label='low')

                ax.set(xlabel='time')
                ax.set_title(metric + ' alert std')

                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()
                
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
            else:
                data = data_qn
                sns.set(rc={'figure.figsize': (12, 8)})
                plt.tight_layout()
                ax = sns.lineplot(x=data['ts'], y=data[metric], label='metric')
                ax = sns.lineplot(x=data['ts'], y=data['up_qn'], label='up_qn')
                ax = sns.lineplot(x=data['ts'], y=data['low_qn'], label='low_qn')

                ax.set(xlabel='time')
                ax.set_title(metric + ' alert qn')

                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()
                
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                
        if alerts>3:
            msg_sos = f'@os2612 Сработало больше 3 сигналов!'
            bot.sendMessage(chat_id=chat_id, text=msg_sos)

try:
    run_alerts()
except Exception as e:
    print(e)
