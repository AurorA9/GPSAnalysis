import pandas as pd
import os,sys
import re
import datetime
import numpy as np
import os,sys
import matplotlib.pyplot as plt
import math
from sklearn.cluster import KMeans
from sklearn.cluster import Birch

import logging
import time
from sklearn.preprocessing import StandardScaler
from pylab import *

logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)



class Personal_analysis:
    def __init__(self):
        pass

    def load_data(self, filePath):
        logger.info("加载数据............. ")
        return pd.read_csv(filePath)

    # 处理地址显示问题 去除方向及数值(米)
    def preprocess_address(self, row):
        row = str(row).strip()
        return re.sub('(附近|东|南|西|北|东南|东西|东北|西南|西北|南北)\d+米', '', row)

    # 求星期 1-7 分别代表星期一到星期天
    def weekday(self, row):
        row = str(row)
        # try:
        #     int('')
        # except ValueError:
        #     pass
        # if row ==' ':
        #     print(11)
        year =int(row[0:4])
        mon = int(row[5:7])
        day = int(row[8:10])

        return datetime.date(year, mon, day).weekday() + 1

    def process_data(self, df):
        # 统一 开始地址与结束地址的格式
        df = df.fillna('null')
        df['start_address_name'] = df['start_address_name'].apply(self.preprocess_address)
        df['end_address_name'] = df['end_address_name'].apply(self.preprocess_address)
        #print(df['end_address_name'][0])
        df[~df['start_address_name'].isnull()]
        logger.info("地址格式化完成.... ")
        # 新建一列用来保存 日期 格式：年 - 月 - 日
        #predict_dataset.Date_received = pd.to_datetime(predict_dataset.Date_received, format='%Y-%m-%d')
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['datetime'] = df['start_time'].dt.strftime('%Y-%m-%d')
        # predict_dataset.Date_received = pd.to_datetime(predict_dataset.Date_received, format='%Y-%m-%d')
        # predict_dataset.Date_received = predict_dataset.Date_received.dt.strftime('%Y%m%d')

        #df['datetime'] = df['start_time'].dt.floor('d')

        # 保存 星期
        df['weekday'] = df['datetime'].apply(self.weekday)

        # 根据日期将数据排序
        df = df.sort_values(by='start_time', ascending=True, inplace=False)


        return df

    # 将每个人的数据分别保存至excel 文件地址为：personalDataPath
    def save_personal_data(self, df, personalDataPath):
        if not os.path.exists(personalDataPath):
            os.makedirs(personalDataPath)
        nameList = list(df['user_id'].unique())
        n_ = len(nameList)
        i_ = 1
        for name in nameList:
            if name == 'null':
                continue
            else:
                logger.info("将个人信息存到excel中...进度 %.2f" % (i_ / n_))
                i_ += 1
                result = df[df['user_id'] == name]
                #print(result)
                result.to_excel(personalDataPath + name + '.xls', index=False, encoding='utf-8')
        logger.info("保存完成，共%d个数据" % i_)

    # 起始地址排序 终止地址排序
    def cal_satrt_end_address(self, df, num=None, starttime=None, endtime=None, which_day=0, precess=False):
        """
            num : 显示排序后前几条数据
            starttime ：开始时间
            endtime: 截止时间
            形式 ：2018 ：2019   2018-01 ：2019-03    2018-06-08 ：2019-07-03 三种形式
            which_day : 0:全部  1：工作日  2：周末
        """
        start_address = {}
        end_address = {}

        # 由日期进行筛选
        if starttime != None and endtime != None:
            df = df.set_index('datetime')
            df = df[starttime:endtime]

        # 筛选 哪天
        if which_day == 1:
            df = df[df['weekday'].isin(list(range(1, 6)))]
        elif which_day == 2:
            df = df[df['weekday'].isin([6, 7])]

        sortBy_start_Address = df.groupby('start_address_name').sum().sort_values(by='count', ascending=False)
        # 按照start地址分组后统计出现个数，使用reset_index()将其转为DataFrame格式
        sortBy_start_Address = sortBy_start_Address.reset_index()

        if num == None or num > len(sortBy_start_Address):
            numa = len(sortBy_start_Address)
        else:
            numa = num
        for i in range(numa):
            start_address[sortBy_start_Address['start_address_name'][i]] = \
            sortBy_start_Address['count'][i]

        start_address_ = []
        for key in start_address.keys():
            start_address_.append(key)

        sortBy_end_Address = df.groupby('end_address_name').sum().sort_values(by='count', ascending=False)
        # 按照start地址分组后统计出现个数，使用reset_index()将其转为DataFrame格式
        sortBy_end_Address = sortBy_end_Address.reset_index()

        if num == None or num > len(sortBy_end_Address):
            numb = len(sortBy_end_Address)
        else:
            numb = num
        for i in range(numb):
            end_address[sortBy_end_Address['end_address_name'][i]] = sortBy_end_Address['count'][i]

        end_address_ = []
        for key in start_address.keys():
            end_address_.append(key)

        if precess:
            start_address_ =self.sub_address(start_address_)
            end_address_ = self.sub_address(end_address_)

        return start_address_, end_address_

    # 起始地址到终止地址 排序
    def get_start_to_end_address(self, df, num=None, starttime=None, endtime=None, which_day=0):
        """
        返回起始地址到终止地址 排序
        input:df
        num : 显示排序后前几条数据
        starttime ：开始时间
        endtime: 截止时间
        形式 ：2018 ：2019   2018-01 ：2019-03    2018-06-08 ：2019-07-03 三种形式
        which_day : 0:全部  1：工作日  2：周末
        output: df格式的数据
        """
        start_to_end_address = {}
        start_to_end_address_df = pd.DataFrame()

        if starttime != None:
            df = df.set_index('datetime')
            df = df[starttime:endtime]

        #筛选 哪天
        if which_day ==1:
            df = df[df['weekday'].isin(list(range(1,6)))]
        elif which_day == 2:
            df = df[df['weekday'].isin([6,7])]

        tmp_df = df.groupby(['start_address_name','end_address_name']).sum().sort_values(by='count',ascending =False).reset_index()
        # for i in range(len(start_to_end_address_df)):
        #     start_to_end_address_df[]
        start_to_end_address_df['start_address_name'] = tmp_df['start_address_name']
        start_to_end_address_df['end_address_name'] = tmp_df['end_address_name']
        start_to_end_address_df['count'] = tmp_df['count']

        if num == None or num >len(start_to_end_address_df):
            num = len(start_to_end_address_df)
        return start_to_end_address_df[:num]

    # 取前四个路线较多的
    def start_to_end_list(self, df,num=None, starttime=None, endtime=None, which_day=0):
        '''
        取前四个路线较多的
        :param df:
        :return: list start_to_end_address_list
        '''
        start_to_end_address_df = self.get_start_to_end_address(df, num, starttime, endtime, which_day)
        start_to_end_address_list = []
        for item in start_to_end_address_df.values[:4]:
            # if item[0] != item[1]:
            stra = item[0] + '-' + item[1] + '-' + str(item[2])
            start_to_end_address_list.append(stra)
        return start_to_end_address_list

    # 统计每天起始地址
    def count_everyday_first_address(self, df,num=None,starttime=None,endtime=None,which_day=0,precess=False):
        '''
        计算每天起始地址排序
        保存每天日期，保证不重复  因为数据是按照时间排序，即每天第一条是起始数据
        input :df  数据表
        num : 显示排序后前几条数据
        starttime ：开始时间
        endtime: 截止时间
        形式 ：2018 ：2019   2018-01 ：2019-03    2018-06-08 ：2019-07-03 三种形式
        which_day : 0:全部  1：工作日  2：周末
        precess : 是否处理地址信息 比如删除前缀
        output:dict 每天起始地址排序
        '''
        everyday_tmp = []   # 存已记录的数据
        everyday_start_address={}  # 保存地址 以及出现次数
        cnt = 0  # 统计总个数 应该和数据量相同

        if starttime != None:
            df['date'] = df['datetime']
            df = df.set_index('date')
            df = df[starttime:endtime]

        #筛选 哪天
        if which_day == 1:
            df = df[df['weekday'].isin(list(range(1,6)))]
        elif which_day == 2:
            df = df[df['weekday'].isin([6,7])]



        # 遍历所有数据
        for i in range(len(df)):
            # 如果之前保存过，则不考虑
            if str(df['datetime'].iloc[i]).split(' ')[0] in everyday_tmp:
                continue
            else:     # 仅保存每天第一次出行的记录
                everyday_tmp.append(str(df['datetime'].iloc[i]).split(' ')[0])   #
                if df['start_address_name'].iloc[i] in everyday_start_address:
                    everyday_start_address[df['start_address_name'].iloc[i]] += 1
                    cnt += 1
                else:
                    everyday_start_address[df['start_address_name'].iloc[i]] = 1
                    cnt += 1
        everyday_start_address = sorted(everyday_start_address.items(),key=lambda s:s[1],reverse=True)

        # 判断取出数据的个数
        if num == None or num > len(everyday_start_address):
            num = len(everyday_start_address)

        # 只记录前num个
        everyday_start_address = everyday_start_address[:num]


        everyday_start_address_ = []
        everyday_start_times = []
        temp_ = {}
        for i in everyday_start_address:
            if i[0] is nan:
                continue
            else:
                temp_[i[0]] = i[1]
                everyday_start_address_.append(i[0])

        # 对地址数据进行处理
        if precess:
            everyday_start_address_ = self.sub_address(everyday_start_address_)


        # 如果进行了格式化，则去除了前缀后没有记录，因此遍历，然后用in
        for i in everyday_start_address_:
            for j in temp_.keys():
                if i in j:
                    everyday_start_times.append(temp_[j])
                    break

        return everyday_start_address_, everyday_start_times, cnt
    # 统计每天第一次停止地点
    def count_everyday_first_stop_address(self, df,num=None,starttime=None,endtime=None,which_day=0,precess=False):
        '''
        计算每天起始地址排序
        input :df  数据表
        num : 显示排序后前几条数据
        starttime ：开始时间
        endtime: 截止时间
        形式 ：2018 ：2019   2018-01 ：2019-03    2018-06-08 ：2019-07-03 三种形式
        which_day : 0:全部  1：工作日  2：周末
        precess : 是否处理地址信息 比如删除前缀
        output:dict 每天起始地址排序
        '''
        everyday_tmp = []   # 存已记录的数据
        everyday_first_stop_address={}  # 保存地址 以及出现次数
        cnt = 0  # 统计总个数 应该和数据量相同

        if starttime != None:
            df['date'] = df['datetime']
            df = df.set_index('date')
            df = df[starttime:endtime]

        #筛选 哪天
        if which_day == 1:
            df = df[df['weekday'].isin(list(range(1,6)))]
        elif which_day == 2:
            df = df[df['weekday'].isin([6,7])]

        # 遍历所有数据
        for i in range(len(df)):
            # 如果之前保存过，则不考虑
            if str(df['datetime'].iloc[i]).split(' ')[0] in everyday_tmp:
                continue
            else:     # 仅保存每天第一次出行的记录
                everyday_tmp.append(str(df['datetime'].iloc[i]).split(' ')[0])   #
                if df['end_address_name'].iloc[i] in everyday_first_stop_address:
                    everyday_first_stop_address[df['end_address_name'].iloc[i]] += 1
                else:
                    everyday_first_stop_address[df['end_address_name'].iloc[i]] = 1
                cnt += 1
        everyday_first_stop_address = sorted(everyday_first_stop_address.items(), key=lambda s:s[1], reverse=True)

        print(everyday_first_stop_address)
        print(cnt)
        # 判断取出数据的个数
        if num == None or num > len(everyday_first_stop_address):
            num = len(everyday_first_stop_address)

        # 只记录前num个
        everyday_first_stop_address = everyday_first_stop_address[:num]

        everyday_first_stop_address_ = []
        everyday_first_stop_times = []
        temp_ = {}
        for i in everyday_first_stop_address:
            if i[0] is nan:
                continue
            else:
                temp_[i[0]] = i[1]
                everyday_first_stop_address_.append(i[0])

        # 对地址数据进行处理
        if precess:
            everyday_first_stop_address_ = self.sub_address(everyday_first_stop_address_)


        # 如果进行了格式化，则去除了前缀后没有记录，因此遍历，然后用in
        for i in everyday_first_stop_address_:
            for j in temp_.keys():
                if i in j:
                    everyday_first_stop_times.append(temp_[j])
                    break

        return everyday_first_stop_address_, everyday_first_stop_times, cnt

    # 统计每天最后地址
    def count_everyday_last_address(self, df,num=None,starttime=None,endtime=None,which_day=0,precess=False):
        """计算每天终止地址排序
        input :df
        num : 显示排序后前几条数据
        starttime ：开始时间
        endtime: 截止时间
        形式 ：2018 ：2019   2018-01 ：2019-03    2018-06-08 ：2019-07-03 三种形式
        which_day : 0:全部  1：工作日  2：周末
        output:list
        """
        everyday_tmp = {}
        everyday_end_address={}   # 统计终止地址及出现次数

        if starttime != None:
            df['date'] = df['datetime']
            df = df.set_index('date')
            df = df[starttime:endtime]

        #筛选 哪天
        if which_day == 1:
            df = df[df['weekday'].isin(list(range(1,6)))]
        elif which_day == 2:
            df = df[df['weekday'].isin([6,7])]

        for i in range(len(df)):
            everyday_tmp[str(df['datetime'].iloc[i]).split(' ')[0]] = df['end_address_name'].iloc[i]

        cnt = 0
        #everyday_tmp
        for key,value in everyday_tmp.items():
            if value in everyday_end_address:
                everyday_end_address[value] += 1

            else:
                everyday_end_address[value] = 1
            cnt += 1
        everyday_end_address = sorted(everyday_end_address.items(),key=lambda s:s[-1],reverse=True)

        if num == None or num > len(everyday_end_address):
            num = len(everyday_end_address)

        everyday_end_address = everyday_end_address[:num]

        everyday_end_address_ = []
        everyday_end_times = []
        temp_ = {}
        for i in everyday_end_address:   # everyday_end_address 是一个键值对 地址：次数
            if i[0] is nan:  # 避免出现nan值
                continue
            else:
                temp_[i[0]] = i[1]
            everyday_end_address_.append(i[0])

        if precess:
            everyday_end_address_ = self.sub_address(everyday_end_address_)
            # 如果进行了格式化，则去除了前缀后没有记录，因此遍历，然后用in
        for i in everyday_end_address_:
            for j in temp_.keys():
                if i in j:
                    everyday_end_times.append(temp_[j])
                    break

        return everyday_end_address_, everyday_end_times, cnt

    # 统计每天停车最长的地址
    def count_everyday_stay_long_address(self, df, num=None,starttime=None,endtime=None,which_day=0,precess=False):
        '''
        统计每天停车最长的地址
        input :df  数据表
        num : 显示排序后前几条数据
        starttime ：开始时间
        endtime: 截止时间
        形式 ：2018 ：2019   2018-01 ：2019-03    2018-06-08 ：2019-07-03 三种形式
        which_day : 0:全部  1：工作日  2：周末
        precess : 是否处理地址信息 比如删除前缀
        output:dict 每天起始地址排序
        '''

        cnt = 1  # 统计总个数 应该和数据量相同

        if starttime != None:
            df['date'] = df['datetime']
            df = df.set_index('date')
            df = df[starttime:endtime]

        #筛选 哪天
        if which_day == 1:
            df = df[df['weekday'].isin(list(range(1,6)))]
        elif which_day == 2:
            df = df[df['weekday'].isin([6,7])]

        everyday_tmp = []  # 存已记录的数据
        everyday_tmp.append(str(df['datetime'].iloc[0]).split(' ')[0])   #初始化为第一天的记录
        everyday_stay_long_address = {}  # 保存地址 以及出现次数
        maxStayTime = -10000
        everyday_long_address = ''  # 保存每天停车时长最长的地址
        #   从第二条开始遍历所有数据
        for i in range(1,len(df)):
            # 如果是同一天 计算 时间差
            # print(df['datetime'].iloc[i])
            if str(df['datetime'].iloc[i]).split(' ')[0] in everyday_tmp:
                # 如果上次停止地址与当前开始地址不匹配，则不计算
                if df['end_address_name'].iloc[i-1] != df['start_address_name'].iloc[i]:
                    continue
                else:
                    last_time = str(df['end_time'].iloc[i-1]).split(' ')[1]   #上一次停车时间结果时:分:秒
                    last_hours = last_time.split(':')[0]
                    last_mins = last_time.split(':')[1]

                    now_time = str(df['start_time'].iloc[i]).split(' ')[1]  # 结果时:分:秒
                    now_hours = now_time.split(':')[0]
                    now_mins = now_time.split(':')[1]
                    stayTime = (int(now_hours) - int(last_hours)) * 60 + (int(now_mins) - int(last_mins))
                    # print("%d:%d --- %d:%d   ----last:%d" % (
                    #int(last_hours), int(last_mins), int(now_hours), int(now_mins), stayTime))
                    if stayTime > maxStayTime:
                        maxStayTime = stayTime
                        everyday_long_address = df['end_address_name'].iloc[i-1]
            else:     # 新的一天 则记录上次保存的结果
                # print("newday")
                maxStayTime = -10000
                everyday_tmp.append(str(df['datetime'].iloc[i]).split(' ')[0])   #如果是每天第一次记录，则不计算，只保存
                # 保存结果
                if everyday_long_address == '':
                    continue
                if everyday_long_address in everyday_stay_long_address.keys():
                    everyday_stay_long_address[everyday_long_address] +=1
                else:
                    everyday_stay_long_address[everyday_long_address] = 1
                cnt += 1
                everyday_long_address = ''
        everyday_stay_long_address = sorted(everyday_stay_long_address.items(),key=lambda s:s[1],reverse=True)
        # print(everyday_stay_long_address)

        # 判断取出数据的个数
        if num == None or num > len(everyday_stay_long_address):
            num = len(everyday_stay_long_address)

        # 只记录前num个
        everyday_stay_long_address = everyday_stay_long_address[:num]

        everyday_stay_long_address_ = []
        everyday_stay_long_times = []
        # 用于单独罗列出地址
        temp_ = {}
        for i in everyday_stay_long_address:
            if i[0] is nan:
                continue
            else:
                temp_[i[0]] = i[1]
                everyday_stay_long_address_.append(i[0])

        # 对地址数据进行处理
        if precess:
            everyday_stay_long_address_ = self.sub_address(everyday_stay_long_address_)

        # 如果进行了格式化，则去除了前缀后查找之前的记录不存在，因此遍历，然后用in
        for i in everyday_stay_long_address_:
            for j in temp_.keys():
                if i in j:
                    everyday_stay_long_times.append(temp_[j])
                    break
        # everyday_stay_long_address_ 罗列地址 如：['sss','aaa','sss','fff']
        # everyday_stay_long_times 罗列地址对应次数 如：[4,2,1,1]
        return everyday_stay_long_address_, everyday_stay_long_times, cnt

    # 时间段停车时长  一天中比如 这里的早6到晚9
    def count_everyday_stay_long_address_6_21(self, df, num=None,starttime=None,endtime=None,which_day=0,precess=False, overMintues=60):
        '''
        统计每天停车最长的地址
        input :df  数据表
        num : 显示排序后前几条数据
        starttime ：开始时间
        endtime: 截止时间
        形式 ：2018 ：2019   2018-01 ：2019-03    2018-06-08 ：2019-07-03 三种形式
        which_day : 0:全部  1：工作日  2：周末
        precess : 是否处理地址信息 比如删除前缀
        output:dict 每天起始地址排序
        '''

        cnt = 1  # 统计总个数 应该和数据量相同

        if starttime != None:
            df['date'] = df['datetime']
            df = df.set_index('date')
            df = df[starttime:endtime]

        #筛选 哪天
        if which_day == 1:
            df = df[df['weekday'].isin(list(range(1,6)))]
        elif which_day == 2:
            df = df[df['weekday'].isin([6,7])]

        everyday_tmp = []  # 存已记录的数据

        everyday_tmp.append(str(df['datetime'].iloc[0]).split(' ')[0])   #初始化为第一天的记录
        everyday_stay_long_address = {}  # 保存地址 以及出现次数 以及总停留时间 格式 {address : [counts,saty_values]}
        maxStayTime = -10000
        everyday_long_address = ''  # 保存每天停车时长最长的地址
        #   从第二条开始遍历所有数据
        for i in range(1, len(df)):
            # 如果是同一天 计算 时间差
            #print(df['datetime'].iloc[i])
            if str(df['datetime'].iloc[i]).split(' ')[0] in everyday_tmp:

                # 如果上次停止地址与当前开始地址不匹配，则不计算
                if df['end_address_name'].iloc[i-1] != df['start_address_name'].iloc[i]:
                    #print("起始地址与终止地址不同")
                    continue
                else:
                    last_time = str(df['end_time'].iloc[i-1]).split(' ')[1]   #上一次停车时间结果时:分:秒
                    last_hours = last_time.split(':')[0]
                    last_mins = last_time.split(':')[1]

                    now_time = str(df['start_time'].iloc[i]).split(' ')[1]  # 结果时:分:秒
                    now_hours = now_time.split(':')[0]
                    now_mins = now_time.split(':')[1]
                    #print("%d:%d --- %d:%d max:" % (
                    #int(last_hours), int(last_mins), int(now_hours), int(now_mins)),maxStayTime)
                    if int(last_hours) < 6 or int(last_hours) > 21 or int(now_hours) < 6 or int(now_hours) > 21:
                        #print("超出时间范围")
                        continue
                    else:
                        stayTime = (int(now_hours) - int(last_hours)) * 60 + (int(now_mins) - int(last_mins))
                        #print("%d:%d --- %d:%d   ----last:%d" % (int(last_hours), int(last_mins), int(now_hours),int(now_mins), stayTime))
                        if stayTime > maxStayTime and stayTime > overMintues:
                            # print("time:%d --- %s" %(stayTime,df['end_address_name'].iloc[i-1]))
                            maxStayTime = stayTime
                            everyday_long_address = df['end_address_name'].iloc[i-1]
            else:     # 新的一天 则记录上次保存的结果
                #print("newDay")

                everyday_tmp.append(str(df['datetime'].iloc[i]).split(' ')[0])   #如果是每天第一次记录，则不计算，只保存
                # 保存结果
                #print('地址%s' % everyday_long_address)
                if everyday_long_address == '':
                    continue
                cnt += 1
                t__ = everyday_stay_long_address.get(everyday_long_address,[0,0])
                # [t__[0]+1,t__[1]+maxStayTime] 分别为计次数+1  最大时间累积和
                everyday_stay_long_address[everyday_long_address] = [t__[0]+1,t__[1]+maxStayTime]
                #print("地址：%s --新增停留时间%d" %(everyday_long_address,maxStayTime))
                everyday_long_address = ''
                maxStayTime = -10000

        everyday_stay_long_address = sorted(everyday_stay_long_address.items(),key=lambda s:s[1][0],reverse=True)
        #print(everyday_stay_long_address)

        # 判断取出数据的个数
        if num == None or num > len(everyday_stay_long_address):
            num = len(everyday_stay_long_address)

        # 只记录前num个
        everyday_stay_long_address = everyday_stay_long_address[:num]

        everyday_stay_long_address_ = []  # 记录停车时长最长的地址列表
        everyday_stay_long_times = [] # 记录停车时长最长的地址列表中每个地址的次数
        everyday_stay_long_mean_stay_time = [] # 记录停车时长最长的地址列表中每个地址平均停止次数
        # 用于单独罗列出地址
        temp_ = {}
        for i in everyday_stay_long_address:
            if i[0] is nan:
                continue
            else:
                temp_[i[0]] = i[1]
                everyday_stay_long_address_.append(i[0])

        everyday_stay_long_gps_data_dic = {}  # 保存地址：GPS对应
        everyday_stay_long_gps_data = []  # 记录前几个的GPS
        for itemAddress in everyday_stay_long_address_:
            df_ = df[df['end_address_name'] == itemAddress]
            loc_gps = df_['end_gps_poi'].iloc[0]
            #print("地址%s:GPS:%s" % (itemAddress, loc_gps))
            everyday_stay_long_gps_data_dic[itemAddress] = loc_gps

        # 对地址数据进行处理
        if precess:
            everyday_stay_long_address_ = self.sub_address(everyday_stay_long_address_)

        # 如果进行了格式化，则去除了前缀后查找之前的记录不存在，因此遍历，然后用in
        for i in everyday_stay_long_address_:
            for j in temp_.keys():
                if i in j:
                    everyday_stay_long_times.append(temp_[j][0])
                    everyday_stay_long_gps_data.append(everyday_stay_long_gps_data_dic[j])
                    everyday_stay_long_mean_stay_time.append(temp_[j][1]/temp_[j][0])
                    break
        # everyday_stay_long_address_ 罗列地址 如：['sss','aaa','sss','fff']
        # everyday_stay_long_times 罗列地址对应次数 如：[4,2,1,1]
        return everyday_stay_long_address_, everyday_stay_long_times, cnt,everyday_stay_long_mean_stay_time, everyday_stay_long_gps_data

    def count_everyday_stay_long_address_17_9(self, df, num=None,starttime=None,endtime=None,which_day=0,precess=False, overMintues=60):
        '''
        统计每天停车最长的地址
        input :df  数据表
        num : 显示排序后前几条数据
        starttime ：开始时间
        endtime: 截止时间
        形式 ：2018 ：2019   2018-01 ：2019-03    2018-06-08 ：2019-07-03 三种形式
        which_day : 0:全部  1：工作日  2：周末
        precess : 是否处理地址信息 比如删除前缀
        overMintues  计算停留时间  > overMintues的地点
        output:dict 每天起始地址排序
        '''
        #print(df.shape)
        cnt = 1  # 统计总个数 应该和数据量相同
        sum_rec = 0
        if starttime != None:
            df['date'] = df['datetime']
            df = df.set_index('date')
            df = df[starttime:endtime]

        #筛选 哪天
        if which_day == 1:
            df = df[df['weekday'].isin(list(range(1,6)))]
        elif which_day == 2:
            df = df[df['weekday'].isin([6,7])]

        #everyday_tmp = []  # 存已记录的数据

        #everyday_tmp.append(str(df['datetime'].iloc[0]).split(' ')[0])   #初始化为第一天的记录
        everyday_stay_long_address = {}  # 保存地址 以及出现次数
        maxStayTime = -10000
        everyday_long_address = ''  # 保存每天停车时长最长的地址

        # 第一条记录开始  计算当前的终止时间与下一条的开始时间
        for i in range(0, df.shape[0]-1):
            sum_rec +=  1
            # print("当前开始地址:%s -- 结束地址%s---时间范围%s --%s" %(df['start_address_name'].iloc[i],df['end_address_name'].iloc[i],df['start_time'].iloc[i],df['end_time'].iloc[i]))
            if df['end_address_name'].iloc[i] != df['start_address_name'].iloc[i+1]:
                # print("起始地址与终止地址不同")
                continue

            last_time = str(df['end_time'].iloc[i]).split(' ')[1]  # 上一次停车时间结果时:分:秒
            last_hours = int(last_time.split(':')[0])
            last_mins = int(last_time.split(':')[1])
            last_date = pd.to_datetime(df['datetime'].iloc[i])

            now_time = str(df['start_time'].iloc[i+1]).split(' ')[1]  # 结果时:分:秒
            now_hours = int(now_time.split(':')[0])
            now_mins = int(now_time.split(':')[1])
            now_date = pd.to_datetime(df['datetime'].iloc[i+1])

            day_diff = getattr(now_date-last_date,"days")

            # print("%s --%s 每条记录时间差:%d" % (df['datetime'].iloc[i],df['datetime'].iloc[i+1],day_diff))
            # print("%d:%d --- %d:%d max:" % (
            minitues = overMintues
            # int(last_hours), int(last_mins), int(now_hours), int(now_mins)),maxStayTime)
            if 17 <= last_hours <= 24:
                if 17 <= now_hours <= 24 and day_diff == 0:
                    stayTime = (now_hours - last_hours) * 60 + (now_mins - last_mins)
                    #print("17-24-----差值%d--最大:%d" %(stayTime,maxStayTime))
                    if stayTime > maxStayTime:
                        # print("%d:%d --- %d:%d max:%d" % (last_hours, last_mins, now_hours, now_mins, stayTime))
                        # print("time:%d --- %s" %(stayTime,df['end_address_name'].iloc[i-1]))
                        maxStayTime = stayTime
                        everyday_long_address = df['end_address_name'].iloc[i]
                elif 0 <= now_hours <= 9 and day_diff == 1:
                    stayTime = (now_hours + 24 - last_hours) * 60 + (now_mins - last_mins)
                    if stayTime > maxStayTime:
                        # print("%d:%d --- %d:%d max:%d" % (last_hours, last_mins, now_hours, now_mins, stayTime))
                        # print("time:%d --- %s" %(stayTime,df['end_address_name'].iloc[i-1]))
                        maxStayTime = stayTime
                        everyday_long_address = df['end_address_name'].iloc[i]
                else:
                    if everyday_long_address != '' and maxStayTime > minitues:
                        #print("%s--最终结果%s--时间间隔：%d" % (df['datetime'].iloc[i], everyday_long_address, maxStayTime))
                        cnt += 1

                        if maxStayTime < 0:
                            logger.error("计算17-9点间最大值出错---------------------------------")
                            return
                        t__ = everyday_stay_long_address.get(everyday_long_address, [0, 0])
                        # [t__[0]+1,t__[1]+maxStayTime] 分别为计次数+1  最大时间累积和
                        everyday_stay_long_address[everyday_long_address] = [t__[0] + 1, t__[1] + maxStayTime]
                        #print("地址：%s --新增停留时间%d" % (everyday_long_address, maxStayTime))

                        everyday_long_address = ''
                        maxStayTime = -1000
                    else:
                        everyday_long_address = ''
                        maxStayTime = -1000
                    continue
            elif 0 <= last_hours <= 9:
                if 0 <= now_hours <= 9 and day_diff == 0:
                    stayTime = (now_hours - last_hours) * 60 + (now_mins - last_mins)
                    if stayTime > maxStayTime:
                        #print("%d:%d --- %d:%d max:%d" % (last_hours, last_mins, now_hours, now_mins, stayTime))
                        # print("time:%d --- %s" %(stayTime,df['end_address_name'].iloc[i-1]))
                        maxStayTime = stayTime
                        everyday_long_address = df['end_address_name'].iloc[i]
                else:
                    if everyday_long_address != '' and maxStayTime > minitues:
                        #print("%s--最终结果%s--时间间隔：%d" % (df['datetime'].iloc[i], everyday_long_address, maxStayTime))
                        cnt += 1
                        if maxStayTime < 0:
                            logger.error("计算17-9点间最大值出错---------------------------------")
                            return
                        t__ = everyday_stay_long_address.get(everyday_long_address, [0, 0])
                        # [t__[0]+1,t__[1]+maxStayTime] 分别为计次数+1  最大时间累积和
                        everyday_stay_long_address[everyday_long_address] = [t__[0] + 1, t__[1] + maxStayTime]
                        #print("地址：%s --新增停留时间%d" % (everyday_long_address, maxStayTime))

                        everyday_long_address = ''
                        maxStayTime = -1000
                    else:
                        everyday_long_address = ''
                        maxStayTime = -1000
            else:
                if everyday_long_address != '' and maxStayTime > minitues:
                    #print("%s--最终结果%s--时间间隔：%d" % (df['datetime'].iloc[i], everyday_long_address, maxStayTime))
                    cnt += 1
                    if maxStayTime < 0:
                        logger.error("计算17-9点间最大值出错---------------------------------")
                        return
                    t__ = everyday_stay_long_address.get(everyday_long_address, [0, 0])
                    # [t__[0]+1,t__[1]+maxStayTime] 分别为计次数+1  最大时间累积和
                    everyday_stay_long_address[everyday_long_address] = [t__[0] + 1, t__[1] + maxStayTime]
                    # print("地址：%s --新增停留时间%d" % (everyday_long_address, maxStayTime))

                    everyday_long_address = ''
                    maxStayTime = -1000
                else:
                    everyday_long_address = ''
                    maxStayTime = -1000
                continue
        everyday_stay_long_address = sorted(everyday_stay_long_address.items(),key=lambda s:s[1][0],reverse=True)

        # 判断取出数据的个数
        if num == None or num > len(everyday_stay_long_address):
            num = len(everyday_stay_long_address)

        # 只记录前num个
        everyday_stay_long_address = everyday_stay_long_address[:num]

        everyday_stay_long_address_ = []  # 记录停车时长最长的地址列表
        everyday_stay_long_times = []  # 记录停车时长最长的地址列表中每个地址的次数
        everyday_stay_long_mean_stay_time = []  # 记录停车时长最长的地址列表中每个地址平均停止次数

        # 用于单独罗列出地址
        temp_ = {}
        for i in everyday_stay_long_address:
            if i[0] is nan:
                continue
            else:
                temp_[i[0]] = i[1]
                everyday_stay_long_address_.append(i[0])

        everyday_stay_long_gps_data_dic = {}  #保存地址：GPS对应
        everyday_stay_long_gps_data = []  # 记录前几个的GPS
        for itemAddress in everyday_stay_long_address_:
            df_ = df[df['end_address_name'] == itemAddress]
            loc_gps = df_['end_gps_poi'].iloc[0]
            # print("地址%s:GPS:%s" %(itemAddress, loc_gps))
            everyday_stay_long_gps_data_dic[itemAddress] = loc_gps

        # 对地址数据进行处理
        if precess:
            everyday_stay_long_address_ = self.sub_address(everyday_stay_long_address_)

        # 如果进行了格式化，则去除了前缀后查找之前的记录不存在，因此遍历，然后用in
        for i in everyday_stay_long_address_:
            for j in temp_.keys():
                if i in j:
                    everyday_stay_long_times.append(temp_[j][0])
                    everyday_stay_long_gps_data.append(everyday_stay_long_gps_data_dic[j])
                    everyday_stay_long_mean_stay_time.append(temp_[j][1]/temp_[j][0])
                    break
        # everyday_stay_long_address_ 罗列地址 如：['sss','aaa','sss1','fff']
        # everyday_stay_long_times 罗列地址对应次数 如：[4,2,1,1]
        return everyday_stay_long_address_, everyday_stay_long_times, cnt,everyday_stay_long_mean_stay_time,everyday_stay_long_gps_data


    def cal_weekday_count_figure(self, df,num =None,starttime=None,endtime=None,which_day=0):
        """

         num : 显示排序后前几条数据
        starttime ：开始时间
        endtime: 截止时间
        形式 ：2018 ：2019   2018-01 ：2019-03    2018-06-08 ：2019-07-03 三种形式
        which_day : 0:全部  1：工作日  2：周末
        """
        if starttime != None:
            df['date'] = df['datetime']
            df = df.set_index('date')
            df = df[starttime:endtime]

        #筛选 哪天
        if which_day == 1:
            df = df[df['weekday'].isin(list(range(1,6)))]
        elif which_day == 2:
            df = df[df['weekday'].isin([6,7])]

        #取出每日开始行使时间数据
        workday_df = df['start_time']

        nowday = 0  # 当前天，判断是否是同一天
        daycount = 0  # 统计共计多少天

        time_count_dic = {}  # 建立 {时间点：次数} 的字典
        hour = -1   # 每一小时仅记录一次
        for i in workday_df:
            if i.day != nowday:
                nowday = i.day
                hour = -1
                daycount +=1

            if i.hour == hour: #同一时间段只记录一次
                continue
            else:
                hour =i.hour
                if hour not in time_count_dic:
                    time_count_dic[hour] = 1
                else:
                    time_count_dic[hour] += 1
        # 有些时间点未出现 将其置“0”
        for i in range(0,24):
            if i not in time_count_dic:
                time_count_dic[i] = 0


        # 按照时间点排序 1-24
        sort_time_list = sorted(time_count_dic.items(), key=lambda x: x[0], reverse=False)
        # 按次数排序，用来取出最多的两次
        sort_count_list = sorted(time_count_dic.items(), key=lambda x: x[-1], reverse=True)


        # 按时间排序 用来绘制图形
        xs = []
        ys = []
        for item in sort_time_list:
            xs.append(item[0] + 0.5)
            ys.append(item[1])
        # print(ys)
        # print(type(ys))
        # ys_ = np.array(ys).reshape(-1, 1)
        # ys_standard = StandardScaler().fit_transform(X=ys_)
        #
        # ys_standard_list = []
        # for i in ys_standard:
        #     ys_standard_list.append(i[0])
        # plt.plot(xs, ys_standard_list, marker='o')
        # plt.xticks([i for i in range(1, 25)])
        # plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        # plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
        # plt.xlabel("行使时间")
        # plt.ylabel("总计次数,共" + str(daycount) + '天')
        # # 绘制前三的点标记
        # # for i in range(3):
        # #     plt.text(sort_count_list[i][0], sort_count_list[i][1], sort_count_list[i][0] + 0.5, ha='center',
        # #              va='bottom', fontsize=10)
        # plt.show()

        if num == None or num > len(sort_count_list):
            num = len(sort_count_list)

        # each_hour_driving_time_standard = ys_standard_list
        #print("len:%d" %len(each_hour_driving_time_standard))
        # return sort_count_list[:num], each_hour_driving_time_standard
        return sort_count_list[:num], ys



    def getProvince(self,start_address, num=None):
        '''
        input : start_address  由 cal_satrt_end_address(df)获得 是一个list
        获取省，市，区，县，镇等信息，这里是使用所有行车记录开始地址中最多的10进行筛选，暂时未出现不存在的情况
        :return: province, city, region, county, town
        '''
        province = ''  # 省
        city = ''  # 市
        region = ''  # 区
        county = ''  # 县
        town = ''  # 镇
        addresses = start_address

        if num == None or num > len(start_address):
            num = len(start_address)

        for address in addresses[:num]:
            if ('省' in address) and province == '':
                province = address[0:address.index('省') + 1]

            if ('市' in address) and city == '':
                index = address.index('市')
                while address[index] != '省':
                    if index > 0:
                        index -= 1
                    else:
                        index = -1
                        break
                city = address[index + 1:address.index('市') + 1]
                # address = address[address.index('市')+1:]

            if ('区' in address) and region == '':
                index = address.index('区')
                while address[index] not in ['省', '市']:
                    if index > 0:
                        index -= 1
                    else:
                        index = -1
                        break
                region = address[index + 1:address.index('区') + 1]

            if ('县' in address) and county == '':
                index = address.index('县')
                while address[index] not in ['省', '市', '区']:
                    if index > 0:
                        index -= 1
                    else:
                        index = -1
                        break
                county = address[index + 1:address.index('县') + 1]
            if ('镇' in address) and town == '':
                index = address.index('镇')
                while address[index] not in ['省', '市', '区', '县']:
                    if index > 0:
                        index -= 1
                    else:
                        index = -1
                        break
                town = address[index + 1:address.index('镇') + 1]
        return province, city, region, county, town

    # 求周末天数  NoworkdayNum   工作日  workdayNum
    def getWorkdayNum(self,df):
        '''
        返回工作日天数与非工作日天数
        :return:workdayNum,NoworkdayNum
        '''
        dayGroup = df.groupby(['datetime', 'weekday']).count().reset_index()  # 按照日期和星期分组
        NoworkdayData = pd.concat([dayGroup[dayGroup['weekday'] == 6], dayGroup[dayGroup['weekday'] == 7]], axis=0)
        NoworkdayNum = len(NoworkdayData)
        workdayNum = dayGroup.shape[0] - NoworkdayNum
        return workdayNum, NoworkdayNum

    #将地址为..路，..街..道的删除前缀
    def sub_address(self,alist):
        lens = len(alist)
        lisb = []
        for i in alist:
            if i is nan:
                i = ''
            while '(' in i:
                index1 = i.index('(')
                if ')' in i:
                    index2 = i.index(')')
                elif '）'in i:
                    index2 = i.index('）')
                else:
                    index2 = len(i)-1
                i = i[0:index1] + i[index2 + 1:len(i)]
            delete_lists = ['街', '道', '路', '县', '镇']
            # 清两次
            for j in delete_lists:
                if j in i:
                    i = i[i.index(j) + 1:len(i)]
            for j in delete_lists:
                if j in i:
                    i = i[i.index(j) + 1:len(i)]

            # if '街' in i:
            #     i = i[i.index('街')+1:len(i)]
            # if '道' in i:
            #     i = i[i.index('道') + 1:len(i)]
            # if '路' in i:
            #     i = i[i.index('路') + 1:len(i)]
            # if '县' in i:
            #     i = i[i.index('县') + 1:len(i)]
            # if '镇' in i:
            #     i = i[i.index('镇') + 1:len(i)]
            # if '市' in i:
            #     i = i[i.index('市') + 1:len(i)]
            if '号' in i and i.index('号') == len(i)-1:
                i = re.sub('\d+号', '', i)
            if i != '' and i != None and ~i.endswith('线'):
                if i not in lisb:
                    lisb.append(i)

        return lisb

    # 获取家庭地址
    def get_home_address(self,lista_, listb_, not_in_list = []):
        """
        在每天起始地址 和终止地址中查找住宅地址
        :param lista:
        :param listb:
        :return: address str  ''为未找到
        """
        # 去除不需要的值
        lista =[]
        listb =[]
        for i in lista_:
            if i not in not_in_list:
                lista.append(i)
        for i in listb_:
            if i not in not_in_list:
                listb.append(i)

        lena = len(lista)
        lenb = len(listb)
        address_flag = False
        address = ''
        address_name_contains = ['苑', '城', '湾', '小区',
                                 '别墅', '公寓', '家居', '佳园', '家园', '花园', '华庭',
                                 '商住楼', '栋', '宿舍楼', '家属楼', '庭院', '水岸',
                                 '庄', '村', '府', '寨']  # 楼 寨 咀 祠
        # 在A中寻找，且也存在B中
        count1 = 1
        for i in lista:
            if address_flag:
                break
            if i in listb:
                for j in address_name_contains:
                    if address_flag:
                        break
                    if j in i:
                        if j != '府' and j != '村' and i.index(j) != 0:
                            address = i
                            address_flag = True
                        elif j == '府' and '政府' not in i and '食府' not in i:
                            address = i
                            address_flag = True
                        elif j == '村' and '农村' not in i:
                            address = i
                            address_flag = True
                        else:
                            pass
                    # if any(j in i for j in address_name_contains):

            count1 +=1
        # 两个中没有共同的 则在A中找
        count2 = 1
        if count1 > lena and address_flag == False:
            for i in lista:
                for j in address_name_contains:
                    if address_flag:
                        break
                    if j in i:
                        if j != '府' and j != '村' and i.index(j) != 0:
                            address = i
                            address_flag = True
                        elif j == '府' and '政府' not in i and '食府' not in i:
                            address = i
                            address_flag = True
                        elif j == '村' and '农村' not in i:
                            address = i
                            address_flag = True
                        else:
                            pass
                if address_flag:
                    break
                count2 += 1
        # A中没有 B中找
        if count2 > lena and address_flag == False:
            for i in listb:
                for j in address_name_contains:
                    if address_flag:
                        break
                    if j in i:
                        if j != '府' and j != '村' and i.index(j) != 0:
                            address = i
                            address_flag = True
                        elif j == '府' and '政府' not in i and '食府' not in i:
                            address = i
                            address_flag = True
                        elif j == '村' and '农村' not in i:
                            address = i
                            address_flag = True
                        else:
                            pass
                if address_flag:
                    break
        return address

    # 获取工作地址  暂时没用
    def get_work_address(self, lista, listb):
        lena = len(lista)
        lenb = len(listb)
        address = ''
        address_flag = False
        address_name_contains = ['站', '局', '驾校', '中心', '政府', '商场', '诊所',
                                 '工业园', '医院', '汽修', '银行', '商会', '公司', '家具', '家居'
                                 '市场', '合作社', '厂', '店', '卫生院', '实验室', '电器',
                                 '物流', '药房', '餐厅', '派出所', '工坊', '馆', '农场', '创业园', '产业园',
                                 '科技园', '银行', '邮政', '信合', '卫生室', '酒楼', '食府', '基地',
                                 '招待所', '餐厅', '牙科', '批发部', '汽贸', '村委会', '委员会', '生活馆',
                                 '景区', '交警', '公安', '机械', '五金', '门诊', '茶业', '广场','零售','商贸',
                                 '大厦']
        count1 = 1
        for i in lista:
            if address_flag:
                break
            if i in listb:
                for j in address_name_contains:
                    if j in i:
                        address = i
                        address_flag = True
                    if address_flag:
                        break
            count1 += 1

        # 两个中没有共同的 则在A中找
        count2 = 1
        if count1 > lena and address_flag == False:
            for i in lista:
                for j in address_name_contains:
                    if address_flag:
                        break
                    if j in i:
                        address = i
                        address_flag = True
                if address_flag:
                    break
                count2 += 1
        # A中没有 B中找
        if count2 > lena and address_flag == False:
            for i in listb:
                for j in address_name_contains:
                    if address_flag:
                        break
                    if j in i:
                        address = i
                        address_flag = True
                if address_flag:
                    break
        return address

    def get_primary_school(self, lista):
        school = ''
        for item in lista :
            if '小学' in item:
                school = item
                break
        return school

    def get_middle_school(self, lista):
        school = ''
        for item in lista :
            if '中学' in item:
                school = item
                break
        return school

    def get_kindergarten(self, lista):
        school = ''
        for item in lista:
            if '幼儿园' in item:
                school = item
                break
        return school

    def judge_is_worker(self,lista):
        if len(lista) == 0:
            return '非上班族'
        if lista[0]>=5 and lista[0]<=10 and lista[1] >=14 and lista[1] <=22:
            return "上班族"
        return "非上班族"

    def get_consumer_address(self, lista):
        consumer_a = []
        for item in lista :
            if '购物' in item:
                consumer_a.append(item)
            if '超市' in item:
                consumer_a.append(item)
            if '商店' in item:
                consumer_a.append(item)
        return consumer_a

    # 将读取的24小时行驶次数 标准化后转为数组形式
    def conver_to_list(self, row):
        arr = row.replace('[', '').replace(']', '').replace(' ', '').split(',')
        arr = list(map(float, arr))
        arr = np.array(arr).reshape(-1, 1)
        ys_standard = StandardScaler().fit_transform(X=arr)
        ys_standard_list = []
        for i in ys_standard:
            ys_standard_list.append(i[0])

        return ys_standard_list

    def get_nums(self, row, hour):
        return row[hour]

    # def get_job_marked(self,filename,n_clusters=5,method='kmeans'):
    #     """
    #     :param method 'kmeans'  或者 'brich'
    #     :param filename:  读取的文件名，为每个人的数据，其中包括列：each_hour_driving_time
    #     :param n_clusters:  聚类个数
    #     :return:  给数据进行打标记  上班类型
    #     """
    #     #filename = "person_info_test1.xlsx"
    #     df = pd.read_excel(filename)
    #
    #     #将str转为list  并且标准化
    #     df['each_hour_driving_time_standard'] = df['each_hour_driving_time'].apply(self.conver_to_list)
    #
    #     # 创建新的列，来保存各个时间数据
    #     for i in range(24):
    #         df['hour_' + str(i)] = df['each_hour_driving_time_standard'].apply(self.get_nums, args=(i,))
    #
    #     # 获取 数据 进行聚类
    #     standardData = df.iloc[:, -24:]
    #
    #     # init='random'
    #     if method == 'kmeans':
    #         kmeans = KMeans(n_clusters=n_clusters, random_state=0).fit(standardData)
    #         markLabels = kmeans.labels_
    #     else:
    #         # 两种聚类方法
    #         brc = Birch(branching_factor=50, n_clusters=n_clusters, threshold=0.5, compute_labels=True).fit(standardData)
    #         markLabels = brc.predict(standardData)
    #
    #     #打标记
    #     df['job_label'] = markLabels
    #
    #     drop_colums = ['hour_' + str(i) for i in range(0, 24)]
    #     df = df.drop(columns=drop_colums)
    #     df = df.drop(columns=['each_hour_driving_time_standard'])
    #     return df
    #     # 作图 同于查看
    #     # fig = plt.figure()
    #     # test = df[df['job_label'] == 4]
    #     # color = ['g', 'r', 'y', 'b']
    #     # fig = plt.figure()
    #     # x = [i for i in range(0, 24)]
    #     # for i in range(test.shape[0]):
    #     #     y1 = test['each_hour_driving_time_standard'].iloc[i]
    #     #     # plt.plot(x,y1,c=color[kmeans.labels_[i]])
    #     #     plt.plot(x, y1)
    #     #     # plt.plot(x,y1,c=color[brc_label[i]])
    #     #     plt.xticks(range(24))
    #     return df

    def get_person_info(self, personalDataPath):
        """
        获取用户信息，并进行保存
        :param personalDataPath:
        :return:
        """
        columns = ['user_id', 'car_id',
                   'car_driving_time','car_driving_day',
                   'car_driving_time_everyday','start_datetime',
                   'end_datetime','day_between_start_end',
                   'usage_rate_day','workdaySum_dring','weekendSum_dring',
                   'usage_rate_workday','usage_rate_weekend',
                   'workdaySum',
                   'weekendSum','rate',
                   'province', 'city',
                   'region',
                    'start_to_end_list',
                    'driving_time_often','startaddress',
                    'startaddress_process','count_start_address_',
                   'sum_start_address_','everyday_end_address',
                   'everyday_end_address_process','count_everyday_end_address_',
                   'sum_everyday_end_address_','everyday_first_stop_address',
                   'everyday_first_stop_address_process','everyday_first_stop_times_',
                   'everyday_first_stop_times_sum_',
                   'everyday_stay_long_address', 'everyday_stay_long_address_process',
                   'everyday_stay_long_times_', 'everyday_stay_long_sum_',
                   'everyday_stay_long_address_6_21', 'everyday_stay_long_address_6_21_process',
                   'everyday_stay_long_times_6_21_', 'everyday_stay_long_sum_6_21_',
                   'everyday_stay_long_mean_stay_time_6_21_',
                   'everyday_stay_long_address_17_9', 'everyday_stay_long_address_17_9_process',
                   'everyday_stay_long_times_17_9_', 'everyday_stay_long_sum_17_9_',
                   'everyday_stay_long_mean_stay_time_17_9_',
                    'everyday_stay_long_sum_17_9_gps', 'everyday_stay_long_sum_17_9_gps_',
                   'first_address', 'home_address_guesss', 'home_address_guesss_process',
                    'work_address_guesss','work_address_guesss_process',
                    'weekendGo', 'weekendGo_process',
                    'workdayGo','workdayGo_process',
                    'having_kindergarten', 'having_primary_school',
                    'having_middle_school', 'is_office_worker',
                    'consumer_address',
                    'each_hour_driving_time'
                   ]

        save_msg = pd.DataFrame(columns=columns)
        row_index = 0

        filenNames = os.listdir(personalDataPath)
        # (filenNames)
        nu_ = len(filenNames)
        for filename in filenNames:
            logger.info("写入个人数据：%s" % (filename))
            df = pd.read_excel(personalDataPath + filename)
            #加一列 用于之后计数
            df['count'] = 1

            user_id = df['user_id'].iloc[0]
            car_id = df['car_id'].iloc[0]
            car_driving_time = df.shape[0]
            car_driving_day = len(set(df['datetime']))


            if car_driving_day < 30:
                print("行使天数少于30天")
                car_driving_time_everyday=None
                start_datetime=None; end_datetime=None; day_between_start_end=None
                usage_rate_day=None;workdaySum_dring=None; weekendSum_dring=None
                usage_rate_workday=None; usage_rate_weekend=None
                workdaySum=None;
                weekendSum=None; rate=None; province=None; city=None; region=None;
                start_to_end_list=None;
                driving_time_often=None; startaddress=None
                startaddress_process=None;
                count_start_address_=None;
                sum_start_address_=None;
                everyday_end_address=None; everyday_end_address_process=None;
                count_everyday_end_address_=None; sum_everyday_end_address_=None;
                everyday_first_stop_address=None; everyday_first_stop_address_process=None;
                everyday_first_stop_times_=None; everyday_first_stop_times_sum_=None;
                everyday_stay_long_address=None; everyday_stay_long_address_process=None
                everyday_stay_long_times_=None; everyday_stay_long_sum_=None
                everyday_stay_long_address_6_21=None; everyday_stay_long_address_6_21_process=None
                everyday_stay_long_times_6_21_=None; everyday_stay_long_sum_6_21_=None
                everyday_stay_long_mean_stay_time_6_21_ = None
                everyday_stay_long_address_17_9=None; everyday_stay_long_address_17_9_process=None
                everyday_stay_long_times_17_9_=None; everyday_stay_long_sum_17_9_=None
                everyday_stay_long_mean_stay_time_17_9_ = None
                everyday_stay_long_sum_17_9_gps=None;everyday_stay_long_sum_17_9_gps_=None
                first_address=None; home_address_guesss=None; home_address_guesss_process =None
                work_address_guesss=None; work_address_guesss_process =None
                weekendGo=None; weekendGo_process=None
                workdayGo=None; workdayGo_process=None
                having_kindergarten=None; having_primary_school=None
                having_middle_school=None; is_office_worker=None
                consumer_address=None
                each_hour_driving_time=None
                save_msg.loc[row_index] = [user_id, car_id, car_driving_time,
                                           car_driving_day, car_driving_time_everyday,
                                           start_datetime, end_datetime, day_between_start_end,
                                           usage_rate_day, workdaySum_dring, weekendSum_dring,
                                           usage_rate_workday, usage_rate_weekend,
                                           workdaySum,
                                           weekendSum, rate, province, city, region,
                                           start_to_end_list,
                                           driving_time_often, startaddress,
                                           startaddress_process,
                                           count_start_address_,
                                           sum_start_address_,
                                           everyday_end_address, everyday_end_address_process,
                                           count_everyday_end_address_, sum_everyday_end_address_,
                                           everyday_first_stop_address, everyday_first_stop_address_process,
                                           everyday_first_stop_times_, everyday_first_stop_times_sum_,
                                           everyday_stay_long_address, everyday_stay_long_address_process,
                                           everyday_stay_long_times_, everyday_stay_long_sum_,
                                           everyday_stay_long_address_6_21, everyday_stay_long_address_6_21_process,
                                           everyday_stay_long_times_6_21_, everyday_stay_long_sum_6_21_,
                                           everyday_stay_long_mean_stay_time_6_21_,
                                           everyday_stay_long_address_17_9, everyday_stay_long_address_17_9_process,
                                           everyday_stay_long_times_17_9_, everyday_stay_long_sum_17_9_,
                                           everyday_stay_long_mean_stay_time_17_9_,
                                           everyday_stay_long_sum_17_9_gps,everyday_stay_long_sum_17_9_gps_,
                                           first_address, home_address_guesss,home_address_guesss_process,
                                           work_address_guesss,work_address_guesss_process,
                                           weekendGo, weekendGo_process,
                                           workdayGo, workdayGo_process,
                                           having_kindergarten, having_primary_school,
                                           having_middle_school, is_office_worker,
                                           consumer_address,
                                           each_hour_driving_time,
                                           ]
                row_index += 1
                logger.info("写入个人数据：%s....进度%0.2f" % (user_id, row_index / nu_))
                continue

            car_driving_time_everyday =round(car_driving_time/car_driving_day, 2)
            start_datetime = df['datetime'][0]
            end_datetime = df['datetime'][df.shape[0]-1]

            start_datetime_ = pd.to_datetime(df['datetime'][0])
            end_datetime_ = pd.to_datetime(df['datetime'][df.shape[0] - 1])
            day_between_start_end = getattr(end_datetime_-start_datetime_,'days')+1
            usage_rate_day = round(car_driving_day/day_between_start_end, 2)

            # 在开始时间与终止期间的工作日天数
            workdaySum_dring = int(day_between_start_end / 7 * 5)+1
            weekendSum_dring = int(day_between_start_end / 7 * 2)+1

            # 统计周中和周末使用天数
            workdaySum, weekendSum = self.getWorkdayNum(df)

            # 周中和周末使用率
            # usage_rate_workday = round(workdaySum/workdaySum_dring, 2)
            # usage_rate_weekend = round(weekendSum / weekendSum_dring, 2)

            if workdaySum != 0 and weekendSum != 0:
                rate = round(workdaySum/weekendSum, 2)
                usage_rate_workday = round(workdaySum / workdaySum_dring, 2)
                usage_rate_weekend = round(weekendSum / weekendSum_dring, 2)
            else :
                usage_rate_workday = 0
                usage_rate_weekend = 0
                rate = 0

            start_address, end_address = self.cal_satrt_end_address(df)

            #市下面分为区
            city_all = ['北京市', '天津市', '上海市', '重庆市']
            # 自治区下面分为市
            region_all = ['内蒙古自治区', '广西壮族自治区', '宁夏回族自治区', '新疆维吾尔自治区', '西藏自治区']
            # 省 市 区
            province_all = ['河北省','山西省','辽宁省','吉林省','黑龙江省','江苏省','浙江省','安徽省',
                        '福建省','江西省','山东省','河南省','湖北省','湖南省','广东省','海南省','四川省','贵州省','云南省','陕西省','甘肃省','青海省','台湾省']
            # 获取地址的总次数在前面的 从中抽取省市区
            address_top_5_df = df['end_address_name'].value_counts()[:5].reset_index()
            address_top_5 = address_top_5_df["index"]

            # 第一列是地址，第二列是次数 index 和end_address_name
            city = ''
            region =''
            province = ''

            flag_cal_address = False
            # 添加省级的  同时四大直辖市 和五大自治区也算
            for address in address_top_5:

                if flag_cal_address:
                    break
                # 五大区的  区下有市
                for i in region_all:
                    if i in address:
                        province = i
                        province_index = address.index('区')
                        if '市' in address:
                            city_index = address.index('市')
                            city = address[province_index + 1:city_index + 1]
                        else:
                            city = ""
                        flag_cal_address = True
                        break
                # 四大市的  市下直接区
                for i in city_all:
                    if i in address:
                        province = i  # 市和省设置相同
                        province_index = address.index('市')
                        if '区' in address:
                            region_index = address.index('区')
                            city = address[province_index + 1:region_index + 1]
                        else:
                            city = ""
                        flag_cal_address = True
                        break
                if flag_cal_address:
                    break
                # 直接是省的
                for i in province_all:
                    if i in address:
                        province = i
                        province_index = address.index('省')
                        if '市' in address:
                            city_index = address.index('市')
                            city = address[province_index+1:city_index+1]
                        if '区' in address and '市' in address:
                            city_index = address.index('市')
                            region_index = address.index('区')
                            region = address[city_index+1:region_index+1]
                        flag_cal_address = True
                        break






            #province, city, region, county, town = self.getProvince(start_address)

            start_to_end_list = self.start_to_end_list(df, 10)
            start_to_end_list = ",".join(start_to_end_list)

            # address_ofen = self.get_address_often(df,10)
            # print(address_ofen)
            # address_ofen = ','.join('%s' %address for address in address_ofen)

            # 每天起始地址计算
            startaddress, count_start_address, sum_start_address = self.count_everyday_first_address(df, num =4, which_day=1, precess=False)
            startaddress_process, count_start_address_, sum_start_address_ = self.count_everyday_first_address(df, num=4, which_day=1,precess=True)
            # 计算每个的概率
            count_start_address_ = [round(i / sum_start_address_, 2) for i in count_start_address_]

            everyday_end_address,count_everyday_end_address, sum_everyday_end_address = self.count_everyday_last_address(df, num=4, which_day=1)
            everyday_end_address_process, count_everyday_end_address_, sum_everyday_end_address_ = self.count_everyday_last_address(df, num=4, which_day=1, precess=True)
            count_start_address_ = [round(i / sum_everyday_end_address_, 2) for i in count_everyday_end_address_]

            everyday_first_stop_address, everyday_first_stop_times, everyday_first_stop_times_sum =self.count_everyday_first_stop_address(df, num=4, which_day=1, precess=False)
            everyday_first_stop_address_process, everyday_first_stop_times_, everyday_first_stop_times_sum_ = self.count_everyday_first_stop_address(df, num=4, which_day=1, precess=True)
            everyday_first_stop_times_ = [round(i / everyday_first_stop_times_sum_, 2) for i in everyday_first_stop_times_]


            everyday_stay_long_address, everyday_stay_long_times, everyday_stay_long_sum = self.count_everyday_stay_long_address(df, num=4, which_day=1, precess=False)
            everyday_stay_long_address_process, everyday_stay_long_times_, everyday_stay_long_sum_ = self.count_everyday_stay_long_address(df, num=4, which_day=1, precess=True)
            everyday_stay_long_times_ = [round(i / everyday_stay_long_sum_, 2) for i in everyday_stay_long_times_]



            everyday_stay_long_address_6_21, everyday_stay_long_times_6_21, everyday_stay_long_sum_6_21,everyday_stay_long_mean_stay_time_6_21,everyday_stay_long_sum_6_21_gps = self.count_everyday_stay_long_address_6_21(
                df, num=4, which_day=1, precess=False,overMintues=180)
            everyday_stay_long_address_6_21_process, everyday_stay_long_times_6_21_, everyday_stay_long_sum_6_21_,everyday_stay_long_mean_stay_time_6_21_, everyday_stay_long_sum_6_21_gps_ = self.count_everyday_stay_long_address_6_21(
                df, num=4, which_day=1, precess=True,overMintues=180)
            everyday_stay_long_times_6_21_ = [round(i / everyday_stay_long_sum_6_21_, 2) for i in everyday_stay_long_times_6_21_]
            '''
                       查看排在第一为的地址出现的概率是否大于第二位的地址概率的threshold,
                        比如超出10% 则认定为第一个为工作地址，否则 为两者间平均停留时间最长的地址
                       家庭地址同理
            '''
            threshold_pro = 0.2
            if len(everyday_stay_long_times_6_21_) <= 0:
                work_address_guesss = None
                work_address_guesss_process = None
            else:
                maxP = everyday_stay_long_times_6_21_[0]
                now_index = 0
                i = 1
                #遍历概率列表，选取最优的地址
                while i < len(everyday_stay_long_times_6_21_):
                    if abs(everyday_stay_long_times_6_21_[i] - maxP) > threshold_pro:
                        # print("超出")
                        break
                    else:  # 如果概率差值不大于threshold,则选择两个中间停车时长最长的地点
                        if everyday_stay_long_mean_stay_time_6_21_[now_index] < everyday_stay_long_mean_stay_time_6_21_[i]:
                            now_index = i
                        i += 1
                # print("目前最大的地址在：%d个" %(now_index+1))
                work_address_guesss_process = everyday_stay_long_address_6_21_process[now_index]

                for i in range(len(everyday_stay_long_address_6_21)):
                    if work_address_guesss_process in everyday_stay_long_address_6_21[i]:
                        work_address_guesss = everyday_stay_long_address_6_21[i]
                        break

            # 计算17点到第二天早上9点间的停车时长最长的地址 ，根据此地址列表计算家庭地址
            everyday_stay_long_address_17_9, everyday_stay_long_times_17_9, everyday_stay_long_sum_17_9,everyday_stay_long_mean_stay_time_17_9,everyday_stay_long_sum_17_9_gps = self.count_everyday_stay_long_address_17_9(
                df, num=4, which_day=1, precess=False, overMintues=180)
            everyday_stay_long_address_17_9_process, everyday_stay_long_times_17_9_, everyday_stay_long_sum_17_9_,everyday_stay_long_mean_stay_time_17_9_, everyday_stay_long_sum_17_9_gps_ = self.count_everyday_stay_long_address_17_9(
                df, num=4, which_day=1, precess=True, overMintues=180)
            everyday_stay_long_times_17_9_ = [round(i / everyday_stay_long_sum_17_9_, 2) for i in
                                              everyday_stay_long_times_17_9_]
            # 计算家庭地址
            '''
                    查看排在第一为的地址出现的概率是否大于第二位的地址概率的threshold,
                     比如超出10% 则认定为第一个为家庭地址，否则 为两者间平均停留时间最长的地址
                    工作地址同理
            '''
            #threshold_pro = 0.15
            if len(everyday_stay_long_times_17_9_) <= 0:
                home_address_guesss = None
                home_address_guesss_process = None
            else:
                maxP = everyday_stay_long_times_17_9_[0]
                now_index = 0
                i = 1
                while i<len(everyday_stay_long_times_17_9_):
                    if abs(everyday_stay_long_times_17_9_[i]-maxP) > threshold_pro:
                        #print("超出")
                        break
                    else:   # 如果概率差值不大于threshold,则选择两个中间停车时长最长的地点
                        if everyday_stay_long_mean_stay_time_17_9_[now_index]<everyday_stay_long_mean_stay_time_17_9_[i]:
                            now_index = i
                        i += 1
                #print("目前最大的地址在：%d个" %(now_index+1))
                home_address_guesss_process = everyday_stay_long_address_17_9_process[now_index]

                for i in range(len(everyday_stay_long_address_17_9)):
                    if home_address_guesss_process in everyday_stay_long_address_17_9[i]:
                        home_address_guesss = everyday_stay_long_address_17_9[i]
                        break

            df_ = df
            df_ = df_[df_['end_address_name']==home_address_guesss]




            # 第一地址是否相同 相同则打印
            if home_address_guesss is not None and  work_address_guesss is not None:
                first_address = 1 if home_address_guesss == work_address_guesss else 0
            else:
                first_address = -1




            # 工作地址
            #work_address_guesss = self.get_work_address(startaddress_process, everyday_end_address_process)
            # 家庭地址
            #home_address_guesss = self.get_home_address(startaddress_process,everyday_end_address_process, not_in_list=list(work_address_guesss))

            workdayGo, workdayGo_ = self.cal_satrt_end_address(df, 10, which_day=1, precess=False)
            workdayGo_process, workdayGo_process_ = self.cal_satrt_end_address(df, 10, which_day=1, precess=True)

            weekendGo, weekendGo_ = self.cal_satrt_end_address(df, 10, which_day=2, precess=False)
            weekendGo_process, weekendGo_process_ = self.cal_satrt_end_address(df, 20, which_day=2, precess=True)

            lista = list(set(startaddress_process + everyday_end_address_process + workdayGo_process))

            having_kindergarten = self.get_kindergarten(lista)
            having_primary_school = self.get_primary_school(lista)
            having_middle_school = self.get_middle_school(lista)
            consumer_address = self.get_consumer_address(weekendGo_process)



            driving_time_often = []
            sort_count_list, each_hour_driving_time = self.cal_weekday_count_figure(df,which_day=1)

            get_value = True # 获取工作时间点 ，假设两者时间至少相差5小时
            loop_index = 0
            while get_value:
                if loop_index > 5 or loop_index >= len(sort_count_list):
                    driving_time_often = []
                    get_value = False
                    logger.info("未求解书上下班时间.....")
                    break
                if loop_index == 0:
                    driving_time_often.append(int(sort_count_list[loop_index][0]))
                else:
                    if abs(int(sort_count_list[loop_index][0]) - driving_time_often[-1]) > 6:
                        driving_time_often.append(int(sort_count_list[loop_index][0]))

                loop_index += 1

                if len(driving_time_often) == 2:
                    get_value = False
            if len(driving_time_often) > 1:
                driving_time_often.sort()   # 时间由早到晚排序
                driving_time_often[0] += 1   # 起始时间加1小时  因为原来统计的时间是一小时之内的 比如8点 代表 8-9点出门 即9点开始上班
                #driving_time_often_str = '-'.join('%s点' % num for num in driving_time_often)

            # 判断是否是上班族
            is_office_worker = self.judge_is_worker(driving_time_often)

            save_msg.loc[row_index] = [user_id, car_id, car_driving_time,
                                        car_driving_day, car_driving_time_everyday,
                                        start_datetime, end_datetime,day_between_start_end,
                                        usage_rate_day,workdaySum_dring,weekendSum_dring,
                                        usage_rate_workday,usage_rate_weekend,
                                        workdaySum,
                                        weekendSum, rate, province, city, region,
                                        start_to_end_list,
                                        driving_time_often, startaddress,
                                        startaddress_process,
                                        count_start_address_,
                                        sum_start_address_,
                                        everyday_end_address, everyday_end_address_process,
                                        count_everyday_end_address_, sum_everyday_end_address_,
                                       everyday_first_stop_address, everyday_first_stop_address_process,
                                       everyday_first_stop_times_, everyday_first_stop_times_sum_,
                                       everyday_stay_long_address, everyday_stay_long_address_process,
                                       everyday_stay_long_times_, everyday_stay_long_sum_,
                                       everyday_stay_long_address_6_21, everyday_stay_long_address_6_21_process,
                                       everyday_stay_long_times_6_21_, everyday_stay_long_sum_6_21_,
                                       everyday_stay_long_mean_stay_time_6_21_,
                                       everyday_stay_long_address_17_9, everyday_stay_long_address_17_9_process,
                                       everyday_stay_long_times_17_9_, everyday_stay_long_sum_17_9_,
                                       everyday_stay_long_mean_stay_time_17_9_,
                                       everyday_stay_long_sum_17_9_gps, everyday_stay_long_sum_17_9_gps_,
                                        first_address, home_address_guesss, home_address_guesss_process,
                                        work_address_guesss,work_address_guesss_process,
                                        weekendGo, weekendGo_process,
                                        workdayGo, workdayGo_process,
                                        having_kindergarten, having_primary_school,
                                        having_middle_school, is_office_worker,
                                        consumer_address,
                                        each_hour_driving_time,
                                       ]
            row_index += 1
            logger.info("写入个人数据：%s....进度%0.2f" % (user_id, row_index/nu_))
        return save_msg

    def save_info(self, filename, df):
        """

        :param filename:  保存到的文件名
        :param df:        传入df数据
        :return:
        """
        logger.info('写入信息完成：%s' %filename)
        df.to_excel(filename)




#filePath = 'GPSData-origin.xlsx'
filePath = 'a800_1000cars.csv'
#personalDataPath = 'personalDataPath/'
#saveFile = 'person_info_all.xls'

personalDataPath = 'test1/'
saveFile = 'person_info_test1.xlsx'
a = Personal_analysis()
# 加载数据
# df = a.load_data(filePath)
# # 处理数据
# df = a.process_data(df)
# # 将数据按个人进行保存
# a.save_personal_data(df, personalDataPath)


# 读取个人数据，然后提取信息点
dfa = a.get_person_info(personalDataPath)
# 将所有人的信息进行保存
a.save_info(saveFile, dfa)


# #读取之前保存的信息，然后进行标记工作类型
# job_df = a.get_job_marked(saveFile, n_clusters=3, method='kmeans')
# # 修改后保存
# a.save_info(saveFile,job_df)







