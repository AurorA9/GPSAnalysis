import personal_info
import gps_poi_analysis
import pandas as pd

if __name__ == '__main__':
    #要读取的文件名
    filePath = 'a800_1000cars.csv'
    # 保存信息的的文件名
    saveFile = 'person_info_all.xlsx'

    a = personal_info.Personal_analysis()
    #加载数据
    # df = a.load_data(filePath)
    # # # 对数据做预处理
    # df = a.process_data(df)
    #
    # # 读取个人数据，然后提取信息点
    # dfa = a.get_person_info(df)
    # # 将所有人的信息进行保存
    # a.save_info(saveFile, dfa)

    # # 根据文件进行经纬度解析，并将结果保存在当前文件中
    a.transform_gps_data('person_info_all.xlsx')
