#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2022/9/30 14:29
# @Author    :Haofan Wang
# @Email     :wanghf58@mail2.sysu.edu.cn
import os
import warnings
from ftplib import FTP
import pandas as pd
import tqdm

warnings.filterwarnings("ignore")

if __name__ == "__main__":
    # 选择需要下载的年份
    year = "2017"
    # 加载站点信息表


    # 连接需要下载的ftp站点
    host = 'ftp.ncdc.noaa.gov'
    ftp = FTP()
    print(ftp)
    ftp.connect(host, 21)
    useranme = None
    password = None
    ftp.login(useranme, password)
    ftppath = '/pub/data/noaa/isd-lite/{0}'.format(year)
    ftp.cwd(ftppath)
    # ftp.dir()
    # 获取当前列表中的所有文件名称
    filebefore = ftp.nlst()
    # print(filebefore)

    station_info = pd.read_csv("stations.csv")
    station_info['STATION_ID'] = station_info['STATION_ID'].apply(lambda x: str(x).zfill(11))
    station_ids = station_info["STATION_ID"]
    file_list = []
    for station_id in tqdm.tqdm(station_ids):
        station_id = str(station_id)
        file_name = f"{station_id[0:6]}-{station_id[6::]}-{year}.gz"
        if filebefore.__contains__(file_name):
            file_list.append(file_name)

    localpath = './data'
    os.makedirs(localpath, exist_ok=True)
    os.chdir(localpath)
    # 判断部分文件是否存在
    fileafter = []
    for fileone in file_list:
        if not os.path.exists(fileone):
            fileafter.append(fileone)
    print(fileafter)

    for filetwo in fileafter:
        file_handle = open(filetwo, 'wb').write
        ftp.retrbinary('RETR %s' % filetwo, file_handle, blocksize=1024)
        print(f"Finish downloda {filetwo}.")
    ftp.quit()
