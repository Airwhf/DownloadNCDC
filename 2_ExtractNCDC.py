#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2022/9/30 14:59
# @Author    :Haofan Wang
# @Email     :wanghf58@mail2.sysu.edu.cn
import glob

import pandas as pd
import numpy as np


def extact_data(file_name):
    data = pd.read_table(file_name, header=None)

    '''
    原始数据中以空格分隔的12列数据，分别为：
    年、月、日、小时、温度、露点温度、气压、风向、风速、云量、1小时降雨量和6小时降雨量。
    '''
    # 构建空列表用于存放提取出来的各列数据
    data_list = []
    for line in data.values:
        line_temp = [int(x) for x in line[0].split(' ') if x != '']
        data_list.append(line_temp)

    df = pd.DataFrame(data_list,columns=['年', '月', '日', '小时', '温度', '露点温度', '气压', '风向', '风速', '云量', '1小时雨量', '6小时雨量'])

    # 对数据中-9999的缺失值进行NaN替换
    df = df.replace(-9999, np.nan)

    # 数据说明文档中表示原始数据中温度、露点温度、气压、风速、降雨量的换算系数为10，所以要对原始数据中的对应数据除以10，进行换算。
    df['温度'] = df['温度']/10
    df['露点温度'] = df['露点温度']/10
    df['气压'] = df['气压']/10
    df['风速'] = df['风速']/10
    df['1小时雨量'] = df['1小时雨量']/10
    df['6小时雨量'] = df['6小时雨量']/10

    # 为了便于后续重采样分析数据，给数据增加一个DataFrame列
    df['Date'] = pd.PeriodIndex(year=df['年'], month=df['月'], day=df['日'], hour=df['小时'], freq='H')
    df = df.set_index(df['Date'])
    df.drop(columns='Date', inplace=True)
    # print(df)

    # 保存为同名excel
    df.to_csv('{0}.csv'.format(file_name), encoding="GB2312")
    print("Finish the extracted of {0}.".format(file_name))


if __name__ == '__main__':
    file_list = glob.glob("./data/*")
    for file_name in file_list:
        extact_data(file_name)