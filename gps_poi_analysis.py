import numpy as nu
import pandas as pd
import os
import logging
import numpy as np
import math
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from scipy.spatial.distance import squareform
from scipy.spatial.distance import pdist
from sklearn.cluster import DBSCAN

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

#根据经纬度求两点之间的距离 单位米
def haversine(lonlat1, lonlat2):
    lon1, lat1  = lonlat1
    lon2, lat2= lonlat2
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r *1000

def get_home_address_gps(df):
    #获取每天起始地址与终止地址
    df = df[df['weekday'].isin(list(range(1, 6)))]
    address_gps = []
    address_gps.append(df.iloc[0]['start_gps_poi'])
    for i in range(1, df.shape[0]):
        if df.iloc[i]['datetime'] != df.iloc[i - 1]['datetime']:
            address_gps.append(df.iloc[i]['start_gps_poi'])
            address_gps.append(df.iloc[i - 1]['end_gps_poi'])
    address_gps.append(df.iloc[df.shape[0]-1]['end_gps_poi'])

    if len(address_gps)<50:
        Minpoints = len(address_gps) // 5
    else:
        Minpoints = 10

    gps_lng = 0.0
    gps_lat = 0.0
    select_probability = 0.0
    if len(address_gps) < 10:
        return gps_lng, gps_lat, select_probability

    #将起始地址与终止地址转为数组，原来是 x,y  转 [x, y]
    pointsList = []
    for item in address_gps:
        point = []
        point.append(float(item.split(',')[0]))
        point.append(float(item.split(',')[1]))
        pointsList.append(point)


    xy = np.array(pointsList)

    X = pd.DataFrame(
        {
            "lng": xy[:, 0],
            "lat": xy[:, 1]
        })
    #获取距离举证
    distance_matrix = squareform(pdist(X, (lambda u, v: haversine(u, v))))
    # 100代表100米
    db = DBSCAN(eps=100, min_samples=15, metric='precomputed')
    y_db = db.fit_predict(distance_matrix)
    X['cluster'] = y_db
    # plt.scatter(X['lng'], X['lat'], c=X['cluster'])
    # plt.show()

    i = 0
    #获取结果最多的簇类型
    cluster = X['cluster'].value_counts().keys()[i]
    #-1为离群点 无用,选择下一个
    no_cluster_flag = False
    if cluster == -1:
        i +=1
        if i >= len(X['cluster'].value_counts()):
            no_cluster_flag = True
        else:
            cluster = X['cluster'].value_counts().keys()[i]


    if no_cluster_flag :
        return gps_lng, gps_lat, select_probability
    # 求此类型的均值
    gps_lng = X[X['cluster'] == cluster]['lng'].mean()
    gps_lat = X[X['cluster'] == cluster]['lat'].mean()

    noise = len(X[X['cluster'] == -1])
    select_probability = X['cluster'].value_counts().values[i] / (X.shape[0]-noise)

    # 使用一下语句可查看核密度图像
    #sns.kdeplot(X['lng'], shade=True)
    # sns.kdeplot(X['lat'], shade=True)

    return gps_lng, gps_lat, select_probability

def get_work_address_gps(df):

    df = df[df['weekday'].isin(list(range(1, 6)))]

    end_address_gps = []
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
            all_minues = hours * 60 + minues
            # 间隔小于100米 且时间超过200分钟，进行记录
            if pow((now_x - last_x) ** 2 + (now_y - last_y) ** 2, 0.5) < 0.001 and all_minues >= 200:
                end_address_gps.append(df.iloc[i]['start_gps_poi'])

    if len(end_address_gps)<50:
        Minpoints = len(end_address_gps) // 5
    else:
        Minpoints = 10

    gps_lng = 0.0
    gps_lat = 0.0
    select_probability = 0.0
    if len(end_address_gps)<10:
        return gps_lng, gps_lat, select_probability
    pointsList = []
    for item in end_address_gps:
        point = []
        x = get_gps_poi(item, 0)
        y = get_gps_poi(item, 1)
        point.append(x)
        point.append(y)
        pointsList.append(point)

    xy = np.array(pointsList)

    X = pd.DataFrame(
        {
            "lng": xy[:, 0],
            "lat": xy[:, 1]
        })

    distance_matrix = squareform(pdist(X, (lambda u, v: haversine(u, v))))
    # 2代表两公里
    db = DBSCAN(eps=100, min_samples=Minpoints, metric='precomputed')
    y_db = db.fit_predict(distance_matrix)
    X['cluster'] = y_db
    # plt.scatter(X['lng'], X['lat'], c=X['cluster'])
    # plt.show()

    i = 0
    # 获取结果最多的簇类型
    cluster = X['cluster'].value_counts().keys()[i]
    no_cluster_flag = False
    # -1为离群点 无用,选择下一个
    if cluster == -1:
        i += 1
        if i >= len(X['cluster'].value_counts()):
            no_cluster_flag = True
        else:
            cluster = X['cluster'].value_counts().keys()[i]


    if no_cluster_flag:
        return gps_lng, gps_lat, select_probability
    # 求此类型的均值
    gps_lng = X[X['cluster'] == cluster]['lng'].mean()
    gps_lat = X[X['cluster'] == cluster]['lat'].mean()

    noise = len(X[X['cluster'] == -1])
    select_probability = X['cluster'].value_counts().values[i] / (X.shape[0] - noise)

    # 使用一下语句可查看核密度图像
    # sns.kdeplot(X['lng'], shade=True)
    # sns.kdeplot(X['lat'], shade=True)
    return gps_lng, gps_lat, select_probability
# filepath = 'personalDataPath/'
# save_path = 'data_gps_all'
# connect_same_gps(filepath,save_path)


def getdf(filename):
    return pd.read_excel(filename)

# df = getdf('0e947f19f6214fd2a5ef99f6b9a56c41.xls')
# print(get_home_address_gps(df))
#
# print(get_work_address_gps(df))
