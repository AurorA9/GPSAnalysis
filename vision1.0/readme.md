#### 这个版本只是根据KDE求解地址的，然后根据地址解析出位置信息。
#### 其中删除了很多东西，然后也这个在了一起，方便对方使用，但是不利于自己测试。
#### 在此先进行保存

主要运行程序在main.py里面
filePath = 'a800_1000cars.csv'    #要读取的文件名 CSV格式
saveFile = 'person_info_all.xls'    # 保存信息的的文件名  最好不要重复，不行的话可以和文件名一样， 但是要以.xls结尾

设置好以上两个变量后可以直接运行一下程序，得到仅包含经纬度信息的文件。

a = personal_info.Personal_analysis()

加载数据

df = a.load_data(filePath)

对数据做预处理

df = a.process_data(df)

读取个人数据，然后提取信息点

dfa = a.get_person_info(df)


将所有人的信息进行保存

a.save_info(saveFile, dfa)


当对得到所有用户信息后，将其进行整合后，可以运行一下程序，解析出家庭地址以及工作地址，transform_gps_data(saveFile)，里面参数为文件名，例如‘person_info_all.xls’

a = personal_info.Personal_analysis()

根据文件进行经纬度解析，并将结果保存在当前文件中

a.transform_gps_data(saveFile)

注意：修改百度API的AK 在文件gps_poi_analysis.py中
