#coding=utf-8
__author__ = 'Colin'

import os
import copy
import pickle
import Util

def SortShTrade():
    input_file = "C:\\Data\\ETFProfit\\WorkDir\\hua\\shanghai-trade-in.txt"
    data_info = {} #date:number:info
    fin = open(input_file, 'r')
    for line in fin:
        fields = line.strip().split('\t')
        info = line.strip()
        date = fields[0]
        orderid = int(fields[1][6:].lstrip('0'))
        if date not in data_info:
            data_info[date] = {}
        if orderid not in data_info[date]:
            data_info[date][orderid] = info
    fin.close()

    output_file = "C:\\Data\\ETFProfit\\WorkDir\\hua\\shanghai-trade-out.txt"
    fout = open(output_file, 'w')
    date_keys = data_info.keys()
    date_keys.sort()
    for date_key in date_keys:
        orderids = data_info[date_key].keys()
        orderids.sort()
        for orderid in orderids:
            print>>fout, data_info[date_key][orderid]
    fout.close()

def CashSubstituteList(data_dir):

    data_dir = data_dir + "input\\CashSubstituteList\\"
    # 获取2015年交易日的列表
    tradedates = []
    TradeDateFile = data_dir + "TradeDateList.txt"
    fin = open(TradeDateFile, 'r')
    for line in fin:
        date = line.strip()
        tradedates.append(date)
    fin.close()

    # 记录数据，用于后续格式化的处理
    data_info = {}
    input_1 = data_dir + '20150101.csv'
    input_2 = data_dir + '20150601.csv'
    fin_1 = open(input_1, 'r')
    fin_1.readline()
    for line in fin_1:
        fields = line.strip().split(',')
        fields = [item.strip('"') for item in fields]

        date = Util.CovDate_One(fields[1])
        exchange = fields[2]
        fundid = fields[3]
        fundname = fields[4]
        stockid = fields[5]
        stockname = fields[6]
        mount = fields[7]
        flag = fields[8]
        money = fields[9]
        premium = fields[10]

        if fundid not in data_info:
            data_info[fundid] = {}
        if date not in data_info[fundid]:
            data_info[fundid][date] = []

        data_info[fundid][date].append([date,exchange,fundid,fundname,stockid,stockname,mount,flag,money,premium])
    fin_1.close()
    fin_2 = open(input_2, 'r')
    fin_2.readline()
    for line in fin_2:
        fields = line.strip().split(',')
        fields = [item.strip('"') for item in fields]

        date = Util.CovDate_One(fields[1])
        exchange = fields[2]
        fundid = fields[3]
        fundname = fields[4]
        stockid = fields[5]
        stockname = fields[6]
        mount = fields[7]
        flag = fields[8]
        money = fields[9]
        premium = fields[10]

        if fundid not in data_info:
            data_info[fundid] = {}
        if date not in data_info[fundid]:
            data_info[fundid][date] = []

        data_info[fundid][date].append([date,exchange,fundid,fundname,stockid,stockname,mount,flag,money,premium])
    fin_2.close()

    fundid_set = set(['159901','159902','159905','159915',\
                      '510010','510030','510050','510060',\
                      '510170','510180','510230','510260',\
                      '510410','510610','510620','510650',\
                      '510660','510880'])

    output = data_dir + 'CashSubstituteList2015.txt'
    fout = open(output, 'w')

    for fundid in data_info:
        if fundid.split('.')[0] not in fundid_set:
            continue
        index = 0
        for date in tradedates:
            if date in data_info[fundid]:
                for item in data_info[fundid][date]:
                    print>>fout, '\t'.join(item)
            else:
                tmp_index = index-1
                while tmp_index>=0:
                    if tradedates[tmp_index] in data_info[fundid]:
                        break
                    else:
                        tmp_index -= 1
                pre_date = tradedates[tmp_index]
                for item in data_info[fundid][pre_date]:
                    item[0] = date
                    print>>fout, '\t'.join(item)
            index += 1
    fout.close()

def ClearUpSzTrade(data_dir):

    data_dir = data_dir + "input\\SecondaryMarket\\"
    input_file = data_dir + "TradeSz-raw.txt"

    data_info = {} #date:time:合同序号
    fin = open(input_file, 'r')
    for line in fin:
        fields = line.strip().split('\t')
        fields = [item.decode('gb2312').encode('utf-8').strip() for item in fields]

        result = []
        result.append(Util.CovDate_One(fields[0])) #交易日期
        result.append(fields[1]) #证券代号
        result.append(fields[2]) #证券简称
        result.append(fields[3]) #席位代号
        result.append(fields[4]) #席位名称
        result.append(fields[5]) #营业代号
        result.append(fields[6]) #营业部名称
        result.append(fields[7]) #股东代码
        result.append(fields[8]) #股东姓名
        direc = fields[9]
        if direc=='买入':
            direc = 'B'
        if direc=='卖出':
            direc = 'S'
        result.append(direc) #买卖类型
        result.append(fields[11]) #合同序号
        result.append(Util.CovTime_One(fields[10])) #委托时间
        result.append(fields[12]) #委托数量
        result.append(fields[13]) #委托价格
        if fields[14]!='0':
            result.append(Util.CovTime_One(fields[14])) #最早成交时间
        else:
            result.append('NULL')
        if fields[15]!='0':
            result.append(Util.CovTime_One(fields[15])) #最晚成交时间
        else:
            result.append('NULL')
        if fields[16]:
            result.append(fields[16]) #成交数量
        else:
            result.append('NULL')
        if fields[17]:
            result.append(fields[17]) #成交金额
        else:
            result.append('NULL')
        if fields[20]=='0':
            result.append('NULL') #撤单时间
        else:
            result.append(Util.CovTime_One(fields[20]))
        result.append(fields[21]) #撤单数量

        date = result[0]
        time = result[11]
        contractid = result[10]
        if date not in data_info:
            data_info[date] = {}
        if time not in data_info[date]:
            data_info[date][time] = {}
        if contractid in data_info[date][time]:
            print '\t'.join(result)
        data_info[date][time][contractid] = '\t'.join(result)
    fin.close()

    output_file= data_dir + "TradeSz.txt"
    fout = open(output_file, 'w')
    dates = data_info.keys()
    dates.sort()
    for date in dates:
        times = data_info[date].keys()
        times.sort()
        for time in times:
            contractids = data_info[date][time].keys()
            contractids.sort()
            for contractid in contractids:
                print>>fout, data_info[date][time][contractid]
    fout.close()

def ClearUpShTrade(data_dir):
    #   交易日期,证券代号,证券简称,席位代号,席位名称,营业部代号,营业部名称,股东代码,股东姓名,
    #   买卖类别,合同序号,委托时间,委托数量,委托价格,最早成交时间,最晚成交时间,成交数量,成交金额,撤单时间,撤单数量
    data_dir = data_dir + "input\\SecondaryMarket\\"
    input_file = data_dir + "TradeSh-raw.txt"

    data_info = {} #date:time:合同序号
    fin = open(input_file, 'r')

    for line in fin:
        fields = line.strip().split('\t')
        fields = [item.strip() for item in fields]

        result = []
        result.append(Util.CovDate_One(fields[0])) #交易日期
        result.append(Util.CompStockId(fields[11])) #证券代号
        result.append(fields[12]) #证券简称
        result.append(fields[3]) #席位代号
        result.append(fields[4]) #席位名称
        result.append(fields[5]) #营业代号
        result.append(fields[6]) #营业部名称
        result.append(fields[9]) #股东代码
        result.append(fields[10]) #股东姓名
        result.append(fields[13]) #买卖类型
        result.append(fields[1]) #合同序号
        result.append(fields[2]) #委托时间
        result.append(fields[15].split('.')[0]) #委托数量
        result.append(fields[14]) #委托价格
        if fields[19]:
            result.append(fields[19]) #最早成交时间
        else:
            result.append('NULL')
        if fields[20]:
            result.append(fields[20]) #最晚成交时间
        else:
            result.append('NULL')
        if fields[17]:
            result.append(fields[17].split('.')[0]) #成交数量
        else:
            result.append('NULL')
        if fields[18]:
            result.append(fields[18]) #成交金额
        else:
            result.append('NULL')
        if fields[22]:
            result.append(fields[22]) #撤单时间
        else:
            result.append('NULL')
        result.append(fields[21].split('.')[0]) #撤单数量



        date = result[0]
        time = result[11]
        contractid = int(fields[23]) #微秒级数据，用于相同日期、时间内的排序
        if date not in data_info:
            data_info[date] = {}
        if time not in data_info[date]:
            data_info[date][time] = {}
        if contractid in data_info[date][time]:
            print '\t'.join(result)
        data_info[date][time][contractid] = '\t'.join(result)
    fin.close()

    output_file= data_dir + "TradeSh.txt"
    fout = open(output_file, 'w')
    dates = data_info.keys()
    dates.sort()
    for date in dates:
        times = data_info[date].keys()
        times.sort()
        for time in times:
            contractids = data_info[date][time].keys()
            contractids.sort()
            for contractid in contractids:
                print>>fout, data_info[date][time][contractid]
    fout.close()

def ClearUpSzPr(data_dir):


    data_dir = data_dir + "input\\PrimaryMarket\\"
    input_file = data_dir + "PrSz-raw.txt"
    output_file= data_dir + "PrSz.txt"
    fin = open(input_file, 'r')
    fout = open(output_file, 'w')

    for line in fin:
        fields = line.strip().split('\t')
        fields = [item.strip() for item in fields]
    #       日期，时间，成交序号，证券代号，买方席位号，买方股东号，卖方席位号，卖方股东号，
    #       成交数量，成交金额，现金替代标志，申赎方向
        result = []
        result.append(Util.CovDate_One(fields[0])) #日期
        result.append(Util.CovTime_One(fields[11])) #成交时间
        result.append(fields[1]) #成交序号
        result.append(fields[2]) #证券代号
        result.append(fields[3]) #买方席位号
        result.append(fields[5]) #买方股东号
        result.append(fields[6]) #卖方席位号
        result.append(fields[8]) #卖方股东号
        result.append(fields[9]) #成交数量
        result.append(fields[10]) #成交金额
        result.append(fields[12]) #现金替代标志
        result.append(fields[13]) #申赎方向
        print>>fout, '\t'.join(result)
    fin.close()
    fout.close()

def ClearUpShPr(data_dir):
    data_dir = data_dir + "input\\PrimaryMarket\\"
    input_file = data_dir + "PrSh-raw.txt"
    fin = open(input_file, 'r')
    data_info = {} #date:time:orderid:[]
    for line in fin:
        fields = line.strip().split('\t')
        fields = [item.strip() for item in fields]
        #日期，时间，成交序号，证券代号，席位号，股东号，成交数量，成交金额，申赎方向
        result = []
        result.append(Util.CovDate_One(fields[0]))
        time = fields[6]
        if len(time)!=8:
            time = '0' + time
        result.append(time)
        result.append(fields[3])
        result.append(fields[1])
        result.append(fields[5])
        result.append(fields[2])
        result.append(fields[7])
        result.append(fields[8])
        result.append(fields[4])

        date = result[0]
        time = result[1]
        orderid = int(fields[9])
        if date not in data_info:
            data_info[date] = {}
        if time not in data_info[date]:
            data_info[date][time] = {}
        if orderid not in data_info[date][time]:
            data_info[date][time][orderid] = []
        data_info[date][time][orderid].append('\t'.join(result))
    fin.close()

    output_file = data_dir + "PrSh.txt"
    fout = open(output_file, 'w')
    dates = data_info.keys()
    dates.sort()
    for date in dates:
        times = data_info[date].keys()
        times.sort()
        for time in times:
            orderids = data_info[date][time].keys()
            orderids.sort()
            for orderid in orderids:
                for item in data_info[date][time][orderid]:
                    print>>fout,item
    fout.close()

def FundInfo(data_dir):
    ## 获得基金最小单位份数
    input_dir = data_dir + 'input\\'
    fundinfo_file = input_dir + 'Fundinfo.txt' #基金的相关信息，有基金的最小单位及更新时间
    fundinfo = {}  # fundid:date:份数(200000)
    fin_fundinfo = open(fundinfo_file, 'r')
    for line in fin_fundinfo:
        fields = line.strip().split('\t')
        fundid = fields[0]
        fundinfo[fundid] = {}
        date_list = [fields[3], fields[5], fields[7]]
        share_list= [int(fields[4])*10000, int(fields[6])*10000, int(fields[8])*10000]

        date = date_list[0]
        next_date = date_list[1]
        share = share_list[0]
        fundinfo[fundid][date] = share
        while 1:
            date = Util.NextDay(date)
            if date>=next_date:
                break
            fundinfo[fundid][date] = share

        date = date_list[1]
        next_date = date_list[2]
        share = share_list[1]
        fundinfo[fundid][date] = share
        while 1:
            date = Util.NextDay(date)
            if date>=next_date:
                break
            fundinfo[fundid][date] = share

        date = date_list[2]
        next_date = '20160101'
        share = share_list[2]
        fundinfo[fundid][date] = share
        while 1:
            date = Util.NextDay(date)
            if date>=next_date:
                break
            fundinfo[fundid][date] = share
    fin_fundinfo.close()
    return fundinfo

def PcfInfo(data_dir):
    input_dir = data_dir + 'input\\CashSubstituteList\\standard\\'

    pcf_info = {}
    files = os.listdir(input_dir)
    for file in files:
        pcf_file  = input_dir + file #现金替代清单
        fin_pcf = open(pcf_file, 'r')
        fin_pcf.readline()
        #fundid:date:stockid:[股票数，是否现金，溢价标志，现金金额]
        for line in fin_pcf:
            fields = line.strip().split(',')
            fields = [item.decode('gb2312').encode('utf-8') for item in fields]
            fundid = fields[2].split('.')[0]
            date = fields[0]
            stockid = fields[4].split('.')[0]
            is_cash = fields[7]
            stocknum = fields[6].split('.')[0]
            premium = fields[9].split('.')[0]
            money = fields[8]
            if is_cash=='允许':
                is_cash = '1'
                stocknum = int(stocknum)
                premium = int(premium)/100.0
                money = 'NULL'
            if is_cash=='必须':
                is_cash = '2'
                stocknum = 'NULL'
                premium = 'NULL'
                money = float(money)
            if fundid not in pcf_info:
                pcf_info[fundid] = {}
            if date not in pcf_info[fundid]:
                pcf_info[fundid][date] = {}
            pcf_info[fundid][date][stockid] = [stocknum,is_cash, premium,money]
        fin_pcf.close()

    return pcf_info

def PrInfo(data_dir, fundinfo, pcfinfo):

    input_dir = data_dir + 'input\\'
    pr_file   = input_dir + 'PrimaryMarket\\PrSz.txt' #申赎指令清单

    # prinfo的数据结构：
    #       date:seq:[fundid, B/S, Num, tradeid, time, account_user, account_fund, stockinfo]
    # stockinfo的数据结构:
    #       stockid:[k/z, volume, tradeid,time] k为股票，z为现金
    prinfo = {}
    fin_pr = open(pr_file, 'r')
    line = fin_pr.readline().strip()
    direc_set = set(['B', 'S', 'C'])
    flag_set  = set(['K', 'Z'])
    while line :
        ## 处理每个区块的第一条数据，标示此处需要处理的基金的相关信息
        fields = line.split('\t')
        date = fields[0]
        time = fields[1]
        tradeid = fields[2]
        stockid = fields[3]
        stocknum = int(fields[8])
        direc = fields[11]
        account_user = ''
        account_fund = ''
        if direc=='C': #撤单的数据则自动略过
            line = fin_pr.readline().strip()
            continue
        if direc=='B':
            account_user = fields[5]
            account_fund = fields[7]
        if direc=='S':
            account_user = fields[7]
            account_fund = fields[5]
        if stockid not in fundinfo:
            # 检验此时处理区块是否是基金的申赎开始
            # 如果不是，则说明处理过程有问题，需要查找原因
            print line
            print "Error: Not Match..."
            break
        if date not in prinfo:
            prinfo[date] = {}
        seq = len(prinfo[date]) #该天的第几个ETF基金申赎，序号从0开始
        share = fundinfo[stockid][date] #基金的最小份额数
        num = stocknum/share #该次申赎的基金份数
        if stocknum%share!=0:
            # 检验申购赎回是否是基金份额的整数倍，如果不是则报错，查找问题
            print line.strip()
            print "Error: Wrong ETF Share Info..."
            break
        prinfo[date][seq] = [stockid,direc,num,tradeid,time,account_user,account_fund, None]
        ## 处理该基金成分股的申购信息
        # 申购跟赎回的格式有别，需要分开处理
        stockinfo = {}
        while True:
            line = fin_pr.readline().strip()
            if not line: #读到文件的结尾，结束程序
                break
            fields = line.split('\t')
            cons_stockid = fields[3]
            if cons_stockid in fundinfo: #进入新的ETF申购区块，保留上一个区块相关信息
                prinfo[date][seq][7] = copy.deepcopy(stockinfo)
                break
            cons_time    = fields[1]
            cons_tradeid = fields[2]
            cons_volume  = int(fields[8])
            cons_flag    = fields[10]
            if cons_flag not in flag_set: #只关注现金替代和股票兑换的情况
                continue
            if cons_volume==0:
                continue
            stockinfo[cons_stockid] = [cons_flag, cons_volume, cons_tradeid, cons_time]
        # 检验检测出的股票列表是否跟现金替代清单的一致
        s_real = set(stockinfo.keys())
        s_pub  = set(pcfinfo[stockid][date].keys())
        if not( len(s_real)==len(s_pub) and len(s_real|s_pub)==len(s_real)):
            print line
            print "Error: Wrong ETF Constitute Stock..."
            break
    fin_pr.close()
    return prinfo

def IsMatchSz(local_data, fund_stockinfo, block_num):
    # 输出：[local_data, fund_result]
    #   完成对local_data前两个字段的状态修改
    #   fund_result = {} #fundid: [True/False, FundDirec, Fundnum]
    #   fund_stockinfo->fundid:stockid:volume

    fund_result = {}
    # 同时考虑成分股以及成分股的权重，判断该区间数据所对应的基金
    block_seq = 0 #记录当前数据区段中，属于第几个基金的划分
    for fund_id in fund_stockinfo:
        fund_result[fund_id] = [False, 'NULL', 0]
        direc_set  = set() #记录出现的买卖方向，需要方向保持一致
        match_info = {} #stock_id:倍数:次数
        for item in local_data:
            head = item[0]
            if head!='NULL': # 如果该行数据已经被标注，则不要考虑该行数据
                continue
            stock_id = item[3]
            if stock_id not in fund_stockinfo[fund_id]: #非成分股，则不考虑
                continue
            ## 实际买卖的股票数跟成分股权重股数的倍数关系
            # 可能存在多种倍数
            # 可能存在不能整除的情况
            stock_num = int(item[14]) #实际买卖的股票数量
            cons_num  = fund_stockinfo[fund_id][stock_id] #成分股的股票数量
            multiple = 0
            if stock_num%cons_num==0:
                multiple = stock_num/cons_num
            else:
                multiple = stock_num/float(cons_num)
            if stock_id not in match_info:
                match_info[stock_id] = {}
            if multiple not in match_info[stock_id]:
                match_info[stock_id][multiple] = 1
            else:
                match_info[stock_id][multiple] +=1
            stock_direc = item[11]
            direc_set.add(stock_direc)
        if len(direc_set)!=1: #成分股的买卖方向不一致，则直接判定为不匹配
            continue
        fund_direc = direc_set.pop()
        if len(match_info)/float(len(fund_stockinfo[fund_id])) < 0.9: #所包含的成分股过少，则直接判定为不匹配
            continue
        ## 判断数据区块中所包含的基金申赎情况
        # match_info = {} #stock_id:倍数:次数
        match_set = set() #记录需要匹配的倍数以及该倍数出现的次数
        multiple_info = {} #倍数：stock_id：次数
        for stock_id in match_info:
            for multiple in match_info[stock_id]:
                if multiple not in multiple_info:
                    multiple_info[multiple] = {}
                multiple_info[multiple][stock_id] = match_info[stock_id][multiple]
        for multiple in multiple_info:
            if len(multiple_info[multiple])/float(len(fund_stockinfo[fund_id]))<0.9:
                continue
            count_map = {}
            for stock_id in multiple_info[multiple]:
                count = multiple_info[multiple][stock_id]
                if count not in count_map:
                    count_map[count] = 1
                else:
                    count_map[count]+= 1
            sorted_count = sorted(count_map.items(),key=lambda d:d[1], reverse=True)
            sum_count = 0
            for sorted_item in sorted_count:
                sum_count += sorted_item[1]
            if len(count_map)==0 or \
               len(count_map)>3  or \
               sorted_count[0][1]/float(sum_count)<0.8:
                continue
            match_set.add((multiple, sorted_count[0][0]))
        ## 根据match_set记录的数据划分情况，标记local_data
        for set_item in list(match_set):
            fund_multiple = set_item[0] #倍数
            fund_count    = set_item[1] #区间个数
            inner_tag = {} #stock_id:seq
            # local_data每行数据的前两个字段分别标记为：
            #   基金ID，block_num-block_seq-direc-fund_num
            for item in local_data:
                head = item[0]
                if head != 'NULL': # 如果该行数据已经被标注，则不要考虑该行数据
                    continue
                stock_id = item[3]
                if stock_id not in fund_stockinfo[fund_id]: #非成分股，则不考虑
                    continue
                ## 实际买卖的股票数跟成分股权重股数的倍数关系
                stock_num = int(item[14]) #实际买卖的股票数量
                cons_num  = fund_stockinfo[fund_id][stock_id] #成分股的股票数量
                multiple = stock_num/cons_num
                if multiple != fund_multiple:
                    continue
                if stock_id not in inner_tag:
                    inner_tag[stock_id] = block_seq
                else:
                    inner_tag[stock_id] += 1
                item[0] = fund_id
                item[1] = "%s|%s|%s|%s"%(block_num,inner_tag[stock_id],fund_direc,fund_multiple)
            block_seq += fund_count
        ## 修改fund_result的状态
        for item in list(match_set):
            fund_result[fund_id][0] = True
            fund_result[fund_id][1] = fund_direc
            fund_result[fund_id][2] += item[0]*item[1]
    # 标注异常的block
    # end of fund_stockinfo loop

    return fund_result

def IsMatchSz_bak2(block_data, fund_stockinfo):
    # 如果匹配则输出:(True, fundid, direc, fundnum)
    # 如果不匹配则输出:(False,'NULL','NULL', 0)

    # fund_stockinfo->fundid:stockid:volume
    result = [False, 'NULL', 'NULL', 0]

    # 记录block_data的股票数及买卖方向
    stockinfo = {} #stockid:number
    direc_set = set() #记录block_data存在的买卖方向
    for item in block_data:
        stockid = item[3]
        volume  = int(item[14])
        direc   = item[11]
        if stockid in stockinfo:
            #股票列表出现重复项
            return result
        stockinfo[stockid] = volume
        direc_set.add(direc)
    if len(direc_set)!=1:
        #数据块的买卖方向不一致
        return result
    block_direc = direc_set.pop()

    ### 判断block_data能跟那个基金匹配上
    # 挨个判断该天内发生申赎的基金
    fund_result = {} #fund:[False, 'NULL', 'NULL', 0]
    for fund_id in fund_stockinfo:
        fund_result[fund_id] = [True, 'NULL', 'NULL', 0]
        if ( len(stockinfo)/float(len(fund_stockinfo[fund_id]))<0.8 ) or \
           ( len(stockinfo)>len(fund_stockinfo[fund_id]) ):
            fund_result[fund_id][0] = False
        for stock_id in stockinfo:
            if stock_id not in fund_stockinfo[fund_id]:
                fund_result[fund_id][0] = False
        if not fund_result[fund_id][0]:
            continue
        fund_result[fund_id][1] = fund_id
        fund_result[fund_id][2] = block_direc
        # 最后判断对应的是几个基金份额的申赎
        multiple_count = {}
        for stock_id in stockinfo:
            multiple = stockinfo[stock_id]/fund_stockinfo[fund_id][stock_id]
            if multiple not in multiple_count:
                multiple_count[multiple] = 1
            else:
                multiple_count[multiple] += 1
        sorted_multiple = sorted(multiple_count.items(), key=lambda d:d[1], reverse=True)
        match_cnt = sorted_multiple[0][0]
        fund_result[fund_id][3] = match_cnt
    # 最后判断结果,保证匹配的基金类别是唯一的
    true_count = 0
    for fund_id in fund_result:
        if fund_result[fund_id][0]:
            true_count += 1
            result = fund_result[fund_id]
    if true_count==1:
        return result
    else:
        return [False, 'NULL', 'NULL', 0]

def MatchPrTrade_bak2(data_dir, fundinfo, pcfinfo, prinfo):
    input_dir = data_dir  + 'input\\'
    trade_file= input_dir + 'SecondaryMarket\\TradeSz.txt' #委托交易流水

    #### 首先匹配申购、赎回对应的股票买卖连续区间；然后匹配后续的补买、补卖行为。
    # 保证委托数量的准确匹配
    # 完全撤单的先不要删掉，有可能是在凑一篮子股票或卖出一篮子股票时没有交易成功的
    # 保证委托时间相近，相差不超过一秒
    # 拿到一篮子股票后不会全部卖出，卖出篮子的时候可能是以小于篮子个数出手，然后分多次卖出
    tmp_output = data_dir + 'input\\test_output_trade.txt'
    tmp_fout = open(tmp_output, 'w')

    '''
    dates = prinfo.keys()
    dates.sort()
    for date in dates:
        seqs = prinfo[date].keys()
        seqs.sort()
        for seq in seqs:
            count = 0
            if not prinfo[date][seq][7]:
                continue
            for stockid in prinfo[date][seq][7]:
                if prinfo[date][seq][7][stockid][0]=='K':
                    count += 1
            print>>tmp_fout, "%s\t%s\t%s\t%s"%(date,seq, \
                '\t'.join([str(item) for item in prinfo[date][seq][:5]]), count)
    '''

    #按照日期，顺序存放每天的交易数据
    print "\t%s"%('Load TradeSz.txt....')
    fin_trade = open(trade_file, 'r')
    data_info = {}
    for line in fin_trade:
        fields = line.strip().split('\t')
        date = fields[0]
        if date>'20150414':
            break
        if date not in data_info:
            data_info[date] = []
        #在每行的开头添加两个字段(基金代码，申赎方向-区块的顺序-基金的个数)，用于记录对应的是何种ETF的申赎
        fields = ['NULL', 'NULL'] + fields
        data_info[date].append(fields)
    fin_trade.close()

    # 分天处理，首先识别出申购赎回时的批量买入、卖出
    # 深交所交易数据的字段：
    #   交易日期,证券代号,证券简称,席位代号,席位名称,营业部代号,营业部名称,股东代码,股东姓名,
    #   买卖类别,合同序号,委托时间,委托数量,委托价格,最早成交时间,最晚成交时间,成交数量,成交金额,撤单时间,撤单数量
    # prinfo的数据结构：
    #       date:seq:[fundid, B/S, Num, tradeid, time, account_user, account_fund, stockinfo]
    # stockinfo的数据结构:
    #       stockid:[k/z, volume, tradeid, time] k为股票，z为现金
    print '\t%s'%('StepOne...')
    output_dir  = data_dir + 'output\\TradeMatch\\'
    output_file = output_dir + "SzStepOne.txt"
    fout = open(output_file, 'w')
    dates = data_info.keys()
    dates.sort()
    for date in dates:
        # 按照时间归并该天的数据
        if date > '20150414':
            break
        inner_data = {} #time:[fields, fields]
        for item in data_info[date]:
            time = item[13]
            if time not in inner_data:
                inner_data[time] = []
            inner_data[time].append(item)
        times = inner_data.keys()
        times.sort()

        #获得该天内发生申赎基金的股票替代列表
        fund_stockinfo = {} #fundid:stockid:volume
        #获得该天内发生申赎的基金个数，用于最后验证该天的检测是否成功
        fund_prinfo = {}  #fundid:direc:count
        if date not in prinfo:
            continue
        for seq in prinfo[date]:
            fund_id = prinfo[date][seq][0]
            fund_direc = prinfo[date][seq][1]
            fund_num = prinfo[date][seq][2]
            fund_stockinfo[fund_id] = {}
            if fund_id not in fund_prinfo:
                fund_prinfo[fund_id] = {}
            if fund_direc not in fund_prinfo[fund_id]:
                fund_prinfo[fund_id][fund_direc] = fund_num
            else:
                fund_prinfo[fund_id][fund_direc]+= fund_num
        for fund_id in fund_stockinfo:
            for stock_id in pcfinfo[fund_id][date]:
                if pcfinfo[fund_id][date][stock_id][1]=='2':
                    continue
                fund_stockinfo[fund_id][stock_id] = int(pcfinfo[fund_id][date][stock_id][0])

        #检测该天内基金的申赎区块
        fund_seq = 0 #所检测出的基金申赎区块是该天内第多少个区块
        index = 0    #记录对times的遍历进行到的位置
        while index<len(times):
            cond_a = False #下一个时间是否存在
            cond_b = False #下一个时间是否与当前时间相差1秒
            cond_c = False #当前数据块跟下一数据块是否能匹配上基金
            cond_d = False #当前数据块是否能匹配上基金

            match_info_next = [False, 'NULL', 'NULL', 0] #[True, fundid, direc, fundnum]
            if (index+1)<len(times):
                cond_a = True
            if cond_a:
                if Util.DiffSeconds(times[index], times[index+1]) <2:
                    cond_b = True
                match_info_next = IsMatchSz(inner_data[times[index]]+inner_data[times[index+1]], \
                                            fund_stockinfo)
            match_info_cur = IsMatchSz(inner_data[times[index]], fund_stockinfo)
            if match_info_next[0]:
                cond_c = True
            if match_info_cur[0]:
                cond_d = True
            if match_info_next[0]:
                cond_c = True
            if match_info_cur[0]:
                cond_d = True

            if cond_a and cond_b and cond_c:
                fundid    = match_info_next[1]
                fundnum   = match_info_next[3]
                funddirec = match_info_next[2]
                for item in inner_data[times[index]]+inner_data[times[index+1]]:
                    tmp_item    = item
                    tmp_item[0] = fundid
                    tmp_item[1] = "%s|%s|%s"%(funddirec,fund_seq,fundnum)
                    print>>fout, '\t'.join(tmp_item)
                fund_seq += 1
                index    += 2
                fund_prinfo[fundid][funddirec] -= fundnum
            elif cond_d:
                fundid    = match_info_cur[1]
                fundnum   = match_info_cur[3]
                funddirec = match_info_cur[2]
                for item in inner_data[times[index]]:
                    tmp_item = item
                    tmp_item[0] = fundid
                    tmp_item[1] = "%s|%s|%s"%(funddirec,fund_seq,fundnum)
                    print>>fout, '\t'.join(tmp_item)
                fund_seq += 1
                index += 1
                fund_prinfo[fundid][funddirec] -= fundnum
            else:
                for item in inner_data[times[index]]:
                    print>>fout, '\t'.join(item)
                index += 1
        for fund_id in fund_prinfo:
            for fund_direc in fund_prinfo[fund_id]:
                if fund_prinfo[fund_id][fund_direc] !=0:
                    print "Error: Fund Match Incomplete!"
                    print "\t%s\t%s\t%s\t%s"%(date, fund_id, fund_direc, fund_prinfo[fund_id][fund_direc])
    fout.close()

def IsMatchSz_bak1(block_data, stockinfo, funddirec, fundnum):
    # 如果匹配则输出二元组:(True, block_data所对应的基金个数)
    # 如果不匹配则输出二元组:(False, 0)
    is_match  = False
    match_cnt = 0

    # 判断的block_data委托特征是否满足stockinfo中ETF申赎的股票特征
    stock_count = 0 #该支基金使用股票替换的支数
    for stockid in stockinfo:
        if stockinfo[stockid][0]=='K':
            stock_count += 1

    #首先判断股票数是否匹配
    if (len(block_data)/float(stock_count) < 0.9) or (len(block_data)>stock_count):
        return is_match, match_cnt

    #然后判断数据区间内的股票是否全部在基金的股票替换列表中，不全部都在则不符合
    #同时判断方向是否一致,方向不一致则直接判断为不符合
    stock_block = {}
    for item in block_data:
        stockid = item[3]
        volume  = int(item[14])
        direc   = item[11]
        if direc!= funddirec:
            return is_match, match_cnt
        stock_block[stockid] = volume
    stock_fund  = {}
    for stockid in stockinfo:
        if stockinfo[stockid][0]=='Z':
            continue
        stock_fund[stockid] = stockinfo[stockid][1]/fundnum

    for stockid in stock_block:
        if stockid not in stock_fund:
            return is_match, match_cnt

    is_match = True

    #最后判断对应的是几个基金份额
    multiple_count = {}
    for stockid in stock_block:
        multiple = stock_block[stockid]/stock_fund[stockid]
        if multiple not in multiple_count:
            multiple_count[multiple] = 1
        else:
            multiple_count[multiple] += 1
    sorted_multiple = sorted(multiple_count.items(), key=lambda d:d[1], reverse=True)
    match_cnt = sorted_multiple[0][0]
    return is_match, match_cnt

def MatchPrTrade_bak1(data_dir, fundinfo, pcfinfo, prinfo):
    input_dir = data_dir  + 'input\\'
    trade_file= input_dir + 'SecondaryMarket\\TradeSz.txt' #委托交易流水


    tmp_output = input_dir + 'test_output_trade.txt'
    tmp_fout = open(tmp_output, 'w')

    #### 首先匹配申购、赎回对应的股票买卖连续区间；然后匹配后续的补买、补卖行为。
    # 保证委托数量的准确匹配
    # 完全撤单的先不要删掉，有可能是在凑一篮子股票或卖出一篮子股票时没有交易成功的
    # 保证委托时间相近，相差不超过一秒
    # 拿到一篮子股票后不会全部卖出，卖出篮子的时候可能是以小于篮子个数出手，然后分多次卖出

    #按照日期，顺序存放每天的交易数据
    print "\t%s"%('Load TradeSz.txt....')
    fin_trade = open(trade_file, 'r')
    data_info = {}
    for line in fin_trade:
        fields = line.strip().split('\t')
        date = fields[0]
        if date!='20150116':
            continue
        if date not in data_info:
            data_info[date] = []
        #在每行的开头添加两个字段(基金代码，申赎方向-区块的顺序-基金的个数)，用于记录对应的是何种ETF的申赎
        fields = ['NULL', 'NULL',] + fields
        data_info[date].append(fields)
    fin_trade.close()

    # 分天处理，首先识别出申购赎回时的批量买入、卖出
    # 深交所交易数据的字段：
    #   交易日期,证券代号,证券简称,席位代号,席位名称,营业部代号,营业部名称,股东代码,股东姓名,
    #   买卖类别,合同序号,委托时间,委托数量,委托价格,最早成交时间,最晚成交时间,成交数量,成交金额,撤单时间,撤单数量
    # prinfo的数据结构：
    #       date:seq:[fundid, B/S, Num, tradeid, time, account_user, account_fund, stockinfo]
    # stockinfo的数据结构:
    #       stockid:[k/z, volume, tradeid, time] k为股票，z为现金
    print '\t%s'%('StepOne...')
    output_dir  = data_dir  + 'output\\TradeMatch\\'
    output_file = output_dir + "SzStepOne.txt"
    fout = open(output_file, 'w')
    dates = data_info.keys()
    dates.sort()
    for date in dates:
        # 按照时间归并该天的数据
        if date != '20150116':
            continue
        inner_data = {} #time:[fields, fields]
        for item in data_info[date]:
            time = item[13]
            if time not in inner_data:
                inner_data[time] = []
            inner_data[time].append(item)
        times = inner_data.keys()
        times.sort()

        fund_seq = 0 #所检测出的基金申赎区块是该天内第多少个区块

        inner_fund = prinfo[date] #该天内的所有基金申赎信息
        index_seq  = 0  #记录当前已经进行到哪支基金的申赎
        stockinfo  = inner_fund[index_seq][7] #该支基金对应的真实现金替代明细
        fund_left  = inner_fund[index_seq][2] #一次申赎可能对应多个篮子，记录对于目前的申赎，还有几个篮子没有识别

        index = 0 #记录对times的遍历进行到的位置
        is_finish = False #记录该天的基金是否都已经找出来了
        while index<len(times):
            #该天的所有基金都已经寻找完毕，将剩余没有遍历的数据输出
            if is_finish:
                for item in inner_data[times[index]]:
                    print>>fout, '\t'.join(item)
                index += 1
                continue
            cond_a = False #下一个时间是否存在
            cond_b = False #下一个时间是否与当前时间相差1秒
            cond_c = False #当前数据块跟下一数据块是否能匹配上基金
            cond_d = False #当前数据块是否能匹配上基金

            match_info_next = [False,0]
            if (index+1)<len(times):
                cond_a = True
            if cond_a:
                if Util.DiffSeconds(times[index], times[index+1]) <2:
                    cond_b = True
                match_info_next = IsMatchSz(inner_data[times[index]]+inner_data[times[index+1]], \
                                            stockinfo, \
                                            inner_fund[index_seq][1],\
                                            inner_fund[index_seq][2])
            match_info_cur = IsMatchSz(inner_data[times[index]], \
                                       stockinfo, \
                                       inner_fund[index_seq][1],\
                                       inner_fund[index_seq][2])
            if match_info_next[0]:
                cond_c = True
            if match_info_cur[0]:
                cond_d = True

            if cond_a and cond_b and cond_c:
                fundid = inner_fund[index_seq][0]
                fundnum= match_info_next[1]
                funddirec = inner_fund[index_seq][1]
                for item in inner_data[times[index]]+inner_data[times[index+1]]:
                    tmp_item = item
                    tmp_item[0] = fundid
                    tmp_item[1] = "%s|%s|%s"%(funddirec,fund_seq,fundnum)
                    print>>fout, '\t'.join(tmp_item)
                fund_seq += 1
                index += 2
                fund_left -= fundnum
                while fund_left<=0:
                    index_seq += 1
                    if index_seq==len(inner_fund):
                        is_finish = True
                        break
                    stockinfo  = inner_fund[index_seq][7]
                    fund_left += inner_fund[index_seq][2]
            elif cond_d:
                fundid = inner_fund[index_seq][0]
                fundnum= match_info_cur[1]
                funddirec = inner_fund[index_seq][1]
                for item in inner_data[times[index]]:
                    tmp_item = item
                    tmp_item[0] = fundid
                    tmp_item[1] = "%s|%s|%s"%(funddirec,fund_seq,fundnum)
                    print>>fout, '\t'.join(tmp_item)
                fund_seq += 1
                index += 1
                fund_left -= fundnum
                while fund_left<=0:
                    index_seq += 1
                    if index_seq==len(inner_fund):
                        is_finish = True
                        break
                    stockinfo  = inner_fund[index_seq][7]
                    fund_left += inner_fund[index_seq][2]
            else:
                for item in inner_data[times[index]]:
                    print>>fout, '\t'.join(item)
                index += 1
        if index_seq!=len(inner_fund) or fund_left!=0:
            print index_seq,len(inner_fund),fund_left
            print date
            print "Error: Fund Match Failed in This Day!"

    fout.close()

def OrderDiff(OrderId1, OrderId2, num):
    #输入：
    #   OrderId1->C2012377,D2002649
    #   num:判断两个委托ID是否相差num
    #输出：True,False
    result = False
    head1 = OrderId1[:2] #委托编号的头两位是主机号
    head2 = OrderId2[:2]
    #如果主机号不一样，则直接返回False
    if head1!=head2:
        return result
    num1 = int(OrderId1[2:].lstrip('0'))
    num2 = int(OrderId2[2:].lstrip('0'))

    if (num2-num1)==num:
        result = True
    return result

def MatchPrTrade_StepOne(data_dir, fundinfo, pcfinfo, prinfo):
    print "MatchPrTrade_StepOne..."
    input_dir = data_dir  + 'input\\'
    trade_file= input_dir + 'SecondaryMarket\\TradeSz.txt' #委托交易流水

    #### 首先匹配申购、赎回对应的股票买卖连续区间；然后匹配后续的补买、补卖行为。
    tmp_output = data_dir + 'input\\test_output_trade.txt'
    tmp_fout = open(tmp_output, 'w')

    #按照日期，顺序存放每天的交易数据
    print "\t%s"%('Load TradeSz.txt....')
    fin_trade = open(trade_file, 'r')
    data_info = {}
    for line in fin_trade:
        fields = line.strip().split('\t')
        date = fields[0]
        if date>'20150330':
            break
        if date not in data_info:
            data_info[date] = []
        #在每行的开头添加两个字段(基金代码，申赎方向-区块的顺序-基金的个数)，用于记录对应的是何种ETF的申赎
        fields = ['NULL', 'NULL'] + fields
        data_info[date].append(fields)
    fin_trade.close()

    # 分天处理，首先识别出申购赎回时的批量买入、卖出
    # 深交所交易数据的字段：
    #   交易日期,证券代号,证券简称,席位代号,席位名称,营业部代号,营业部名称,股东代码,股东姓名,
    #   买卖类别,合同序号,委托时间,委托数量,委托价格,最早成交时间,最晚成交时间,成交数量,成交金额,撤单时间,撤单数量
    # prinfo的数据结构：
    #       date:seq:[fundid, B/S, Num, tradeid, time, account_user, account_fund, stockinfo]
    # stockinfo的数据结构:
    #       stockid:[k/z, volume, tradeid, time] k为股票，z为现金
    print '\t%s'%('MatchStart...')
    output_dir  = data_dir + 'output\\TradeMatch\\'
    output_file = output_dir + "SzStepOne.txt"
    fout = open(output_file, 'w')
    dates = data_info.keys()
    dates.sort()
    for date in dates:
        if date>'20150330':
            break

        #获得该天内发生申赎基金的股票替代列表
        fund_stockinfo = {} #fundid:stockid:volume
        #获得该天内发生申赎的基金个数，用于最后验证该天的检测是否成功
        fund_prinfo = {}  #fundid:direc:count
        if date not in prinfo:
            continue
        for seq in prinfo[date]:
            fund_id = prinfo[date][seq][0]
            fund_direc = prinfo[date][seq][1]
            fund_num = prinfo[date][seq][2]
            fund_stockinfo[fund_id] = {}
            if fund_id not in fund_prinfo:
                fund_prinfo[fund_id] = {}
            if fund_direc not in fund_prinfo[fund_id]:
                fund_prinfo[fund_id][fund_direc] = fund_num
            else:
                fund_prinfo[fund_id][fund_direc]+= fund_num
        for fund_id in fund_stockinfo:
            for stock_id in pcfinfo[fund_id][date]:
                if pcfinfo[fund_id][date][stock_id][1]=='2':
                    continue
                fund_stockinfo[fund_id][stock_id] = int(pcfinfo[fund_id][date][stock_id][0])

        #寻找该天内委托编号连续递增的区间
        inner_data = {} #顺序号：[fields, fields, ...]
        index = 0
        while index<len(data_info[date]):
            sequence = len(inner_data)
            local_data = [ data_info[date][index] ]
            while (index+1)<len(data_info[date]) and \
                OrderDiff(data_info[date][index][12], data_info[date][index+1][12], 1):
                index += 1
                local_data.append( data_info[date][index] )
            inner_data[sequence] = copy.deepcopy(local_data)
            index += 1

        #检测该天内基金的申赎区块
        len_seq = len(inner_data)
        seq = 0
        block_num = 0 #记录当前判断的是第几个数据块，用来标记每行数据所属的基金

        while seq<len_seq:
            local_data = inner_data[seq]
            while (seq+1)<len_seq and \
                OrderDiff(inner_data[seq][-1][12], inner_data[seq+1][0][12] ,2) and \
                Util.DiffSeconds(inner_data[seq][-1][13],inner_data[seq+1][0][13])<3:
                seq += 1
                local_data += inner_data[seq]
            # 完成local_data数据的标注，以及数据所属基金的判断
            # local_data每行数据的前两个字段分别标记为：基金ID，block_num-block_seq-direc-fund_num
            fund_result = IsMatchSz(local_data, fund_stockinfo, block_num)
            for item in local_data:
                print>>fout, '\t'.join(item)
            block_inc = False
            for fund_id in fund_result:
                if not fund_result[fund_id][0]:
                    continue
                block_inc = True
                fund_direc = fund_result[fund_id][1]
                fund_num   = fund_result[fund_id][2]
                fund_prinfo[fund_id][fund_direc] -= fund_num

            seq += 1
            if block_inc:
                block_num += 1

        for fund_id in fund_prinfo:
            for fund_direc in fund_prinfo[fund_id]:
                if fund_prinfo[fund_id][fund_direc] !=0:
                    print "Error: Fund Match Incomplete!"
                    print "\t%s\t%s\t%s\t%s"%(date, fund_id, fund_direc, fund_prinfo[fund_id][fund_direc])
    fout.close()

def MatchSzPrimarySecondary(data_dir):

    ## 获得基金的最小单位份数
    # fundid:date:share
    # 510050:20150105:1000000
    print "FundInfo..."
    fundinfo = FundInfo(data_dir)

    ## 获得基金的现金替代清单
    # fundid:date:stockid:[股票数，是否现金，溢价标志，现金金额]
    # 510050:20150105:000001:[100,1,0.15,NULL] 允许现金替代
    # 510050:20150105:000002:[NULL,2,NULL,26358.40] 必须现金替代
    print "PcfInfo..."
    pcfinfo = PcfInfo(data_dir)

    ## 归并申赎指令，用于后续在委托交易流水中识别对应的交易
    # 分天进行记录，考虑ETF的份数，申购和赎回的不同处理
    # prinfo的数据结构：
    #       date:seq:[fundid, B/S, Num, tradeid, time, account_user, account_fund, stockinfo]
    # stockinfo的数据结构:
    #       stockid:[k/z, volume, tradeid, time] k为股票，z为现金
    print "PrInfo..."
    prinfo = PrInfo(data_dir, fundinfo, pcfinfo)

    '''
    tmp_output = data_dir + 'input\\test_output.txt'
    tmp_fout = open(tmp_output, 'w')
    '''

    '''
    stockinfo = prinfo['20150113'][2][7]
    result = '\t'.join([str(item) for item in prinfo['20150113'][2][:7]])
    for stockid in stockinfo:
        print>>tmp_fout, '%s\t%s\t%s'%(result, stockid, '\t'.join([str(item) for item in stockinfo[stockid]]) )

    '''
    '''
    dates = prinfo.keys()
    dates.sort()
    for date in dates:
        seqs = prinfo[date].keys()
        seqs.sort()
        for seq in seqs:
            print>>tmp_fout, "%s\t%s\t%s"%(date,seq, \
                '\t'.join([str(item) for item in prinfo[date][seq][:5]]))
    '''

    #记录基金申赎成交编号跟股票申赎成交编号的对应关系
    print 'MatchPrTrade...'
    MatchPrTrade_StepOne(data_dir, fundinfo, pcfinfo, prinfo)

    ## 匹配申赎指令跟交易流水
    # 注意没有产生成交的委托指令


def main():

    data_dir = "C:\\Data\\ETFProfit\\WorkDir\\hua\\"
    #1、整理各个基金2015年每天的现金替代清单
    #CashSubstituteList(data_dir)

    #2、整理沪深两个市场的交易数据，根据交易真实发生的顺序进行排序
    # 交易数据需要保留的字段：
    #   交易日期,证券代号,证券简称,席位代号,席位名称,营业部代号,营业部名称,股东代码,股东姓名,
    #   买卖类别,合同序号,委托时间,委托数量,委托价格,最早成交时间,最晚成交时间,成交数量,成交金额,撤单时间,撤单数量
    # 为空的字段，则用NULL替代
    #ClearUpSzTrade(data_dir)
    #ClearUpShTrade(data_dir)

    #3、整理沪深两个市场的ETF申购赎回交易指令，根据申赎真实发生的顺序进行排序
    # 深交所申赎指令中，需要保留的字段有：
    #       日期，时间，成交序号，证券代号，买方席位号，买方股东号，卖方席位号，卖方股东号，
    #       成交数量，成交金额，现金替代标志，申赎方向
    #ClearUpSzPr(data_dir)
    # 上交所申赎指令中，需要保留的字段有：
    #       日期，时间，成交序号，证券代号，席位号，股东号，成交数量，成交金额，申赎方向
    #ClearUpShPr(data_dir)

    #4、匹配二级市场交易记录跟一级市场的ETF申赎记录，找到一级市场的每一个成交ID所对应的二级市场交易
    # 匹配深交所的交易
    MatchSzPrimarySecondary(data_dir)
    # 匹配上交所的交易

    #对上交所的交易数据排序
    #SortShTrade()

if __name__ == '__main__':
    main()