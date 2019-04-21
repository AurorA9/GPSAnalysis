import numpy as nu
import pandas as pd
import os
import logging

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s')
logging.root.setLevel(level=logging.INFO)


def get_gps_poi(gps_poi,poi):
    return float(gps_poi.split(',')[poi])


def connect_same_gps(filepath,save_path):
    files = os.listdir(filepath)
    process_index = 1
    for filename in files:
        logging.info('处理文件为：%s,进度为；%d%%' %(filename,process_index/len(files)*100))
        process_index +=1
        df = pd.read_excel(filepath+filename)
        delete_index = []
        for i in range(1, df.shape[0]):
            if df['datetime'].iloc[i] != df['datetime'].iloc[i - 1]:
                continue
            else:
                # 获取上次的终止地址的GPS信息
                last_x = get_gps_poi(df['end_gps_poi'].iloc[i - 1], 0)
                last_y = get_gps_poi(df['end_gps_poi'].iloc[i - 1], 1)
                # 获取当前开始地址的GPS信息
                now_x = get_gps_poi(df['start_gps_poi'].iloc[i], 0)
                now_y = get_gps_poi(df['start_gps_poi'].iloc[i], 1)

                diff_time = df['start_time'].iloc[i] - pd.to_datetime(df['end_time'].iloc[i - 1])
                hours = int(str(diff_time).split(' ')[2].split(':')[0])
                minues = int(str(diff_time).split(' ')[2].split(':')[1])

                # 间隔小于100米 且时间不超过30分钟，则认定是统一次记录
                if pow((now_x - last_x) ** 2 + (now_y - last_y) ** 2, 0.5) < 0.001 and hours == 0 and minues <= 30:
                    df['end_address_name'].iloc[i - 1] = df['end_address_name'].iloc[i]
                    df['end_gps_poi'].iloc[i - 1] = df['end_gps_poi'].iloc[i]
                    df['end_time'].iloc[i - 1] = df['end_time'].iloc[i]
                    df['spend_time'].iloc[i - 1] = df['spend_time'].iloc[i - 1] + df['spend_time'].iloc[i]
                    delete_index.append(i)
        df = df.drop(index=delete_index)
        if not os.path.exists(save_path):
            os.mkdir(save_path)

        df.to_excel(save_path+'/'+ filename)


filepath = 'personalDataPath/'
save_path = 'data_gps_all'
connect_same_gps(filepath,save_path)

