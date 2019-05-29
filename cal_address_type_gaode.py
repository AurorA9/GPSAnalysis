#!/usr/bin/env python
# coding: utf-8

# In[1]:

import os
import pandas as pd
import personal_info
import requests
import threading
import time
from tqdm import tqdm
AK = '20222d44e5f1d4a9b879eedee30d9340'

poicode_path = 'map_poicode.xlsx'



def get_poi_num_to_name(path):

    df_poi_type = pd.read_excel(path)

    num2name ={}
    for i in range(df_poi_type.shape[0]):
        num2name[df_poi_type['NEW_TYPE'].iloc[i]] = ';'.join([df_poi_type['大类'].iloc[i],df_poi_type['中类'].iloc[i],df_poi_type['小类'].iloc[i]])
    return num2name


#高德 获取地址的JSON   根据经度和纬度 顺序不要错
def get_request_json_by_gaode(lat,lng):
    location =','.join([str(lat),str(lng)])
    url = 'https://restapi.amap.com/v3/geocode/regeo?'
    params = {'key':AK, 'location':location, 'extensions':'all', 'radius':0}
    res = requests.get(url,params=params)
    #time.sleep(0.01)
    return res.json()



#根据经纬度获取相关信息
def get_address_info_by_lat_lng(lat,lng,full_info=True):
    num2name = get_poi_num_to_name(poicode_path)

    a = get_request_json_by_gaode(lat,lng)

    count_recall = 0
    while a['status']!= '1':
        print('再次请求!!')
        a = get_request_json_by_gaode(lat, lng)
        count_recall +=1
        time.sleep(0.05)
        if count_recall >=10:
            print('请求超时')
            return

    print(a['status'],a['info'])
    a = a['regeocode']
    formatted_address = a['formatted_address']

    if a['aois'] != []:
        address_name = a['aois'][0]['name']
        # 以数字编号的形式 例如10010   10010|100012
        address_type_num = a['aois'][0]['type'].split('|')[0]
        address_type = num2name[int(address_type_num)]
    else:
        if a['pois'] != []:
            address_name = a['pois'][0]['name']
            address_type = a['pois'][0]['type']
        else:
            address_name =formatted_address
            address_type = 'null'

    if full_info ==False:
        return address_name,address_type
    else:

        city = a['addressComponent']['city']
        if city == []:
            city = ''
        province = a['addressComponent']['province']
        district = a['addressComponent']['district']
        township = a['addressComponent']['township']

        return city,province,district,township,address_name,address_type


#使用series的方式
def get_info_of_data(df_origin):
    '''

    :param df_origin:  原始数据
    :param start:      开始下标
    :param end:        结束下标
    :param save_num:   每隔num个保存一个文件
    :return:
    '''
    df_series = []

    for i in tqdm(range(df_origin.shape[0])):
        print('当前进程开始')
        # try:
        user_id = df_origin['user_id'].iloc[i]
        print('%d----------%s' % (i, user_id))
        a = time.time()
        car_id = df_origin['car_id'].iloc[i]
        start_time = df_origin['start_time'].iloc[i]
        end_time = df_origin['end_time'].iloc[i]
        start_gps_poi = df_origin['start_gps_poi'].iloc[i]
        end_gps_poi = df_origin['end_gps_poi'].iloc[i]
        datetime = df_origin['datetime'].iloc[i]
        weekday = df_origin['weekday'].iloc[i]
        # 这里使用的是开始地址
        start_lat = df_origin['start_gps_poi'].iloc[i].split(',')[0]
        start_lng = df_origin['start_gps_poi'].iloc[i].split(',')[1]

        end_lat = df_origin['end_gps_poi'].iloc[i].split(',')[0]
        end_lng = df_origin['end_gps_poi'].iloc[i].split(',')[1]

        city, province, district, township, start_address_name, start_address_type = get_address_info_by_lat_lng(float(start_lat),
                                                                                                     float(start_lng))
        time.sleep(0.01)
        end_address_name,end_address_type = get_address_info_by_lat_lng(float(end_lat),float(end_lng),full_info=False)



        df_series.append([user_id, car_id, start_time, end_time, start_gps_poi, end_gps_poi, datetime, weekday,
                               city, province, district, township, start_address_name, start_address_type,end_address_name, end_address_type])


        print('保存数据完成，用时:%f' % (time.time() - a))

    columns = ['user_id', 'car_id', 'start_time', 'end_time', 'start_gps_poi', 'end_gps_poi', 'datetime',
               'weekday',
               'city', 'province', 'district', 'township', 'start_address_name', 'start_address_type',
               'end_address_name', 'end_address_type']

    df = pd.DataFrame(df_series)
    df.columns = columns

    return df

#另一种存的方式，但是可能稍微慢点
def get_info_of_data11(df_origin,df_new,start, end):
    '''

    :param df_origin:  原始数据
    :param start:      开始下标
    :param end:        结束下标
    :param save_num:   每隔num个保存一个文件
    :return:
    '''
    start_time_now = time.time()

    lock = threading.Lock()
    for i in tqdm(range(start, end)):


        user_id = df_origin['user_id'].iloc[i]
        print('%d----------%s' % (i, user_id))
        a = time.time()
        car_id = df_origin['car_id'].iloc[i]
        start_time = df_origin['start_time'].iloc[i]
        end_time = df_origin['end_time'].iloc[i]
        start_gps_poi = df_origin['start_gps_poi'].iloc[i]
        end_gps_poi = df_origin['end_gps_poi'].iloc[i]
        datetime = df_origin['datetime'].iloc[i]
        weekday = df_origin['weekday'].iloc[i]
        # 这里使用的是开始地址
        start_lat = df_origin['start_gps_poi'].iloc[i].split(',')[0]
        start_lng = df_origin['start_gps_poi'].iloc[i].split(',')[1]

        end_lat = df_origin['end_gps_poi'].iloc[i].split(',')[0]
        end_lng = df_origin['end_gps_poi'].iloc[i].split(',')[1]

        city, province, district, township, start_address_name, start_address_type = get_address_info_by_lat_lng(
            float(start_lat),
            float(start_lng))
        end_address_name, end_address_type = get_address_info_by_lat_lng(float(end_lat), float(end_lng),
                                                                         full_info=False)

        lock.acquire()
        df_new.loc[i] = [user_id, car_id, start_time, end_time, start_gps_poi, end_gps_poi,datetime,weekday,
                       city, province, district, township, start_address_name, start_address_type,
                       end_address_name, end_address_type]
        print('写入数据为：',user_id, car_id, start_time, end_time, start_gps_poi, end_gps_poi,datetime,weekday,
                       city, province, district, township, start_address_name, start_address_type,
                       end_address_name, end_address_type)
        # if index >= save_num:
        #
        #     df.to_csv('temp/data_' + str(i) + '.csv', encoding='utf-8-sig')
        #     df = pd.DataFrame(columns=columns)
        #     index = 0
        #

        lock.release()
        print('用时:%f' % (time.time() - a))
    # if len(df) > 0:
    #     df.to_csv('temp/data_' + str(i) + '.csv', encoding='utf-8-sig')



def get_data(path):
    df = pd.read_csv(path)
    classa = personal_info.Personal_analysis()

    df = df.dropna(subset=['user_id', 'start_gps_poi', 'end_gps_poi', 'start_time', 'end_time'])

    #只取前500人的记录  数据太大  并且只去周天

    #下次执行
    #df = df[~df['user_id'].isin(in_list)]

    df = classa.process_data(df)
    # 取周末数据
    # df_weekend = df[df['weekday'].isin([6,7])]
    # # 太多 删除一部分
    # user_id_in = list(set(df_weekend.user_id))[:-100]
    # df_weekend_a = df_weekend[df_weekend.user_id.isin(user_id_in)]

    #df = df[df['weekday']==7]
    print('aa:',df.shape)
    return df
#len(list(set(df_weekend.user_id))[:-100])


#可对index重新排序
# columns = ['user_id']
# df_weekend_c = pd.DataFrame(columns=columns)
# df_weekend_c.head()
# df_weekend_c.loc[1] =['1']
# df_weekend_c.sort_index(axis=0)




#获取每一条爬取后的数据
def get_new_data(origin_data,save_path,num_threading):
    '''
    :param origin_data:  原始数据文件地址 a800_1000cars.csv
    :param save_path:    保存数据文件地址 temp/
    :return:
    '''
    df_origin = get_data(origin_data)

    user_list = list(set(df_origin['user_id']))

    #不写入之前已经存在的文件
    exists_list = [i.split('.')[0] for i in os.listdir(save_path) if i.endswith('.csv')]

    if len(exists_list) == len(user_list):
       print('已经全部完成！')
       return

    for index in range(len(user_list)):
        if len(exists_list)!=0:
            if user_list[index] in exists_list:
                continue
        #每次一个人的数据
        df = df_origin[df_origin['user_id']==user_list[index]]

        columns = ['user_id', 'car_id', 'start_time', 'end_time', 'start_gps_poi', 'end_gps_poi', 'datetime',
                   'weekday',
                   'city', 'province', 'district', 'township', 'start_address_name', 'start_address_type',
                   'end_address_name', 'end_address_type']

        df_new = pd.DataFrame(columns=columns)


        print('当前user_id:%s，记录个数：%d，进程度:%d/%d' %(user_list[index],df.shape[0],index+1,len(user_list)))
        start_time = time.time()
        #save_num = 1000
        threads = []
        #total = df_weekend_a.shape[0]
        #total = df_origin.shape[0]
        total = df.shape[0]
        #num_threading = 3
        # if total % num_threading != 0:
        #     yushu = total % num_threading

        each_threading_num = total // num_threading
        for i in range(num_threading):
            if i == num_threading-1:
                t = threading.Thread(target=get_info_of_data11,args=(df,df_new,i*each_threading_num,total))
            else:
                t = threading.Thread(target=get_info_of_data11,args=(df,df_new,i*each_threading_num,(i+1)*each_threading_num))
            threads.append(t)
        for t in threads:
            t.setDaemon(True)
            t.start()
        for t in threads:
            t.join()

        # df.to_csv('temp/data_' + str(i) + '.csv', encoding='utf-8-sig')
        print('写入当前数据！！')
        df_new.to_csv('temp/'+user_list[index]+'.csv',encoding='utf-8-sig')

        print('当前写入用时：',time.time()-start_time)
    #print('记录：%d  总：%d' %(df_new.shape[0],total))
    #排序
    #df_new = df_new.sort_index(axis=0)
    #df_new.to_csv(save_path)


def get_new_data_no_threading(origin_data,save_path):
    '''
    :param origin_data:  原始数据文件地址 a800_1000cars.csv
    :param save_path:    保存数据文件地址 temp/
    :return:
    '''
    df_origin = get_data(origin_data)

    user_list = list(set(df_origin['user_id']))

    #不写入之前已经存在的文件
    exists_list = [i.split('.')[0] for i in os.listdir(save_path) if i.endswith('.csv')]

    if len(exists_list) == len(user_list):
       print('已经全部完成！')
       return

    for index in range(len(user_list)):
        if len(exists_list)!=0:
            if user_list[index] in exists_list:
                continue
        #每次一个人的数据
        df = df_origin[df_origin['user_id']==user_list[index]]

        # columns = ['user_id', 'car_id', 'start_time', 'end_time', 'start_gps_poi', 'end_gps_poi', 'datetime',
        #            'weekday',
        #            'city', 'province', 'district', 'township', 'start_address_name', 'start_address_type',
        #            'end_address_name', 'end_address_type']
        #
        # df_new = pd.DataFrame(columns=columns)

        print('当前user_id:%s，记录个数：%d，进程度:%d/%d' %(user_list[index],df.shape[0],index+1,len(user_list)))


        df_new =  get_info_of_data(df)



        # df.to_csv('temp/data_' + str(i) + '.csv', encoding='utf-8-sig')
        print('写入当前数据！！')
        df_new.to_csv('temp/'+user_list[index]+'.csv',encoding='utf-8-sig')


    #print('记录：%d  总：%d' %(df_new.shape[0],total))
    #排序
    #df_new = df_new.sort_index(axis=0)
    #df_new.to_csv(save_path)



get_new_data_no_threading('a800_1000cars.csv','temp/')


#速度测试  15w条数据 22小时  只是请求 还不算上处理
# for i in tqdm(range(150000)):
#
#     city, province, district, township, address_name, address_type  = get_address_info_by_lat_lng(102.151215,35.125484)
#     print(city,province,district,township,address_name,address_type)
#     city, province, district, township, address_name, address_type = get_address_info_by_lat_lng(102.151215, 35.125484)
#     print(city, province, district, township, address_name, address_type)



