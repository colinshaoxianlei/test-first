#coding=utf-8
__author__ = 'Colin'

import datetime

def CompStockId(stockid):
    # 判断股票的ID是否是6位，不是6位的话，在前边补0
    stockid = str(stockid)
    if len(stockid) != 6:
        for i in range(6-len(stockid)):
            stockid = '0' + stockid
    return stockid

def CovDate_One(date):
    # from 2015-6-1 to 20150601
    # from 2015/6/1 to 20150601
    fields = []
    if date.find('-') != -1:
        fields = date.split('-')
    elif date.find('/') != -1:
        fields = date.split('/')
    else:
        pass
    result = fields[0]
    for i in [1,2]:
        if len(fields[i])!=2:
            result += '0' + fields[i]
        else:
            result += fields[i]
    return result

def CovTime_One(time):
    tmp_time = time
    order_time = "%s:%s:%s"%(tmp_time[:-6], tmp_time[-6:-4], tmp_time[-4:-2])
    if len(order_time) != 8:
        order_time = '0' + order_time
    return  order_time

def NextDay(date):
    #input : 20150601
    #output: 20150602
    d1 = datetime.datetime.strptime(date, '%Y%m%d')
    d2 = d1 + datetime.timedelta(days=1)
    return d2.strftime('%Y%m%d')

def DiffSeconds(time1, time2):
    #time1:09:38:49 #靠前的时间
    #time2:09:38:51 #靠后的时间
    #output: 2
    d1 = datetime.datetime.strptime('20150314 ' + time1, '%Y%m%d %H:%M:%S')
    d2 = datetime.datetime.strptime('20150314 ' + time2, '%Y%m%d %H:%M:%S')
    sub = (d2-d1).seconds
    return sub