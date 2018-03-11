#coding=utf-8

__author__ = 'Colin'


def MergeData():

    # 合并在上交所、深交所的交易及申赎记录
    # 需要的字段：
    #   日期、时间、数据来源(上海、深圳)、动作类型(申赎、交易)、股东名称、股东代码
    #   股票名称、股票代码、交易方向、成交数量、成交金额、是否现金(k-股票,z-现金替代)
    data_dir = "C:\\Data\\ETFProfit\\WorkDir\\hua\\merge\\"

    sh_trade = data_dir + "sh-trade.csv"
    sh_pr    = data_dir + "sh-pr.txt"
    sz_trade = data_dir + "sz-trade.txt"
    sz_pr    = data_dir + "sz-pr.txt"
    output = data_dir + "merge_result.txt"
    fout = open(output, 'w')



    fin_sz_trade = open(sz_trade, 'r')
    for line in fin_sz_trade:
        fields = line.strip().split('\t')
        fields = [item.strip() for item in fields]
        date_fields = fields[0].split('/')
        year = date_fields[0]
        month = date_fields[1]
        if len(month) != 2:
            month = '0' + month
        day = date_fields[2]
        if len(day) != 2:
            day = '0' + day
        date = "%s%s%s"%(year, month, day)
        tmp_time = fields[10]  # 委托时间
        if tmp_time=='0':
            continue
        time = "%s:%s:%s"%(tmp_time[:-6], tmp_time[-6:-4], tmp_time[-4:-2])
        if len(time) != 8:
            time = '0' + time
        source = 'sz'
        act_type = 'trade'
        account_name = fields[8]
        account_id   = fields[7]
        stock_name = fields[2]
        stock_id   = fields[1]
        direc = fields[9].decode('gb2312').encode('utf-8')
        if direc=='买入':
            direc = 'B'
        if direc=='卖出':
            direc = 'S'
        volume = fields[16]
        money  = fields[17]
        tmp_first_time = fields[14]
        first_time = "%s:%s:%s"%(tmp_first_time[:-6], tmp_first_time[-6:-4], tmp_first_time[-4:-2])
        if len(first_time) != 8:
            first_time = '0' + first_time
        tmp_last_time = fields[15]
        last_time = "%s:%s:%s"%(tmp_last_time[:-6], tmp_last_time[-6:-4], tmp_last_time[-4:-2])
        if len(last_time) != 8:
            last_time = '0' + last_time
        is_cash = 'NULL'
        print>>fout, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"% \
                     (date, time, source, act_type, account_name, account_id, \
                      stock_name, stock_id, direc, volume, money, is_cash, \
                      first_time, last_time)
    fin_sz_trade.close()


    '''
    # 此处为完成
    fin_sz_pr = open(sz_pr, 'r')
    for line in fin_sz_pr:
        fields = line.strip().split('\t')
        fields = [item.strip() for item in fields]
        #print fields
        date_fields = fields[0].split('/')
        year = date_fields[0]
        month = date_fields[1]
        if len(month) != 2:
            month = '0' + month
        day = date_fields[2]
        if len(day) != 2:
            day = '0' + day
        date = "%s%s%s"%(year, month, day)
        tmp_time = fields[11]
        if tmp_time=='0':
            continue
        time = "%s:%s:%s"%(tmp_time[:-6], tmp_time[-6:-4], tmp_time[-4:-2])
        if len(time) != 8:
            time = '0' + time
        source = 'sz'
        act_type = 'pr'
        account_name = fields[8]
        account_id   = fields[7]
        stock_name = fields[2]
        stock_id   = fields[1]
        direc = fields[9].decode('gb2312').encode('utf-8')
        if direc=='买入':
            direc = 'B'
        if direc=='卖出':
            direc = 'S'
        volume = fields[16]
        money  = fields[17]
        is_cash = 'NULL'
        print>>fout, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"% \
                     (date, time, source, act_type, account_name, account_id, \
                      stock_name, stock_id, direc, volume, money, is_cash)
    fin_sz_pr.close()
    '''


    fin_sh_trade = open(sh_trade, 'r')
    fin_sh_trade.readline()
    for line in fin_sh_trade:
        fields = line.strip().split(',')
        fields = [item.strip() for item in fields]
        date = fields[0].replace('-', '')
        time = fields[2]  # 委托时间
        if not time:
            continue
        source = 'sh'
        act_type = 'trade'
        account_name = fields[10]
        account_id   = fields[9]
        stock_name = fields[12]
        stock_id   = fields[11]
        direc = fields[13]
        volume = int(float(fields[17]))
        money  = fields[18]
        is_cash = 'NULL'
        first_time = fields[19]
        last_time  = fields[20]
        print>>fout, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"% \
                     (date, time, source, act_type, account_name, account_id, \
                      stock_name, stock_id, direc, volume, money, is_cash, \
                      first_time, last_time)
    fin_sh_trade.close()

    '''
    fin_sh_pr = open(sh_pr, 'r')
    fin_sh_pr.readline()
    for line in fin_sh_pr:
        fields = line.strip().split('\t')
        fields = [item.strip() for item in fields]
        date_fields = fields[0].split('/')
        year = date_fields[0]
        month = date_fields[1]
        if len(month) != 2:
            month = '0' + month
        day = date_fields[2]
        if len(day) != 2:
            day = '0' + day
        date = "%s%s%s"%(year, month, day)
        time = fields[8]
        if len(time) != 8:
            time = '0' + time
        source = 'sh'
        act_type = 'pr'
        account_name = fields[4]
        account_id   = fields[3]
        stock_name = fields[2]
        stock_id   = fields[1]
        direc = fields[6]
        volume = fields[9].replace(',', '')
        money  = fields[10].replace(',', '')
        is_cash = 'NULL'
        print>>fout, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"% \
                     (date, time, source, act_type, account_name, account_id, \
                      stock_name, stock_id, direc, volume, money, is_cash)
    fin_sh_pr.close()
    '''

def SortData():
    data_dir = "C:\\Data\\ETFProfit\\WorkDir\\hua\\merge\\"
    input_file = data_dir + "merge_result.txt"
    output_file= data_dir + "sort_result.txt"

    fin = open(input_file, 'r')
    fout= open(output_file, 'w')
    data_info = {} #key:info key=date:time:
    for line in fin:
        fields = line.strip().split('\t')
        fields = [item.decode('gb2312').encode('utf-8') for item in fields]
        date = fields[0]
        time = fields[1]
        key = "%s|%s"%(date, time)
        if key in data_info:
            count = 0
            while True:
                key = key + "|" + str(count)
                if key in data_info:
                    count += 1
                else:
                    break
        data_info[key] = '\t'.join(fields)
    fin.close()

    keys = data_info.keys()
    keys.sort()
    for key in keys:
        print>>fout, data_info[key]
    fout.close()

def IsStock(stock_id):
    if stock_id.startswith('6') or \
        stock_id.startswith('3') or \
        stock_id.startswith('0'):
        return True
    else:
        return False

def MergeSort():

    data_dir = "C:\\Data\\ETFProfit\\WorkDir\\hua\\merge\\"
    #  输入数据存在的字段：
    #  日期、委托时间、数据来源(上海、深圳)、动作类型(申赎、交易)、股东名称、股东代码
    #  股票名称、股票代码、交易方向、成交数量、成交金额、是否现金(k-股票,z-现金替代)
    #  最早成交时间、最晚成交时间
    input_file = data_dir + "sort_result.txt"
    output_file= data_dir + "MergeSort.txt"

    fin = open(input_file, 'r')
    fout= open(output_file,'w')

    # 首先将所有的数据存放到data_info中,相同时间的数据依次存放在该时间对应的列表中
    # 对于最早成交时间为空，即无成交的委托，在此略过不计
    data_info = {} #key:info key=date:time ，value:[fields, fields]
    for line in fin:
        fields = line.strip().split('\t')
        if len(fields) < 13:
            continue
        date = fields[0]
        time = fields[1]
        first_time = fields[12]
        if not first_time:
            continue
        key = "%s|%s"%(date, time)
        if key not in data_info:
            data_info[key] = []
        data_info[key].append(fields)
    fin.close()

    # 按照时间顺序逐秒分析原始数据，将相同时间，一致的股票买卖行为合并
    # 一致的股票买卖行为：时间相同；股东代码相同；买卖方向相同
    keys = data_info.keys()
    keys.sort()
    for key in keys:
        inner_data = data_info[key]
        date = key.split('|')[0]
        time = key.split('|')[1]
        # etf  输出：date,time,account_id,direc,account_name,volume,money
        # stock输出：data,time,account_id,direc,股票支数,第一支股票信息,最后一只股票信息
        #           股票信息：stock_name|stock_id|sh/sz|volume|money
        stock_info = {}# value:[[(stock_name,stock_id,sh/sz,volume,money)],[]]
        stock_key  = {} # 记录股票分组对应stock_info中的索引号 key->n
        stock_key_rev = {} # n->key
        for fields in inner_data:
            source = fields[2]
            account_name = fields[4]
            account_id   = fields[5]
            stock_name = fields[6]
            stock_id   = fields[7]
            direc = fields[8]
            volume = fields[9]
            money  = fields[10]

            is_stock = IsStock(stock_id)
            if is_stock: #是股票的情况，先将结果暂存在stock_info中
                account_key = "%s|%s"%(account_id,direc)
                if account_key not in stock_key:
                    lenth = len(stock_key)
                    stock_key[account_key] = lenth
                    stock_key_rev[lenth] = account_key
                    stock_info[lenth] = []
                    stock_info[lenth].append([account_name,stock_name,stock_id,source,volume,money])
                else:
                    lenth = stock_key[account_key]
                    stock_info[lenth].append([account_name,stock_name,stock_id,source,volume,money])
            else:
                for i in range(len(stock_info)):
                    account_key = stock_key_rev[i]
                    print>>fout, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"%('stock', date, time,\
                                account_key.split('|')[0], \
                                account_key.split('|')[1], \
                                len(stock_info[i]), \
                                '|'.join(stock_info[i][0]), \
                                '|'.join(stock_info[i][-1]))
                stock_info = {}
                stock_key  = {}
                stock_key_rev = {}
                print>>fout, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"%\
                             ('etf',date,time,account_id, direc,account_name,volume, money)
        for i in range(len(stock_info)):
            account_key = stock_key_rev[i]
            print>>fout, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"%('stock', date, time,\
                account_key.split('|')[0], \
                account_key.split('|')[1], \
                len(stock_info[i]), \
                '|'.join(stock_info[i][0]), \
                '|'.join(stock_info[i][-1]))
    fout.close()



def main():

    ## 合并上交所跟深交所的交易数据
    #MergeData()
    ## 按照时间顺序，对MergeData的输出结果排序
    #SortData()
    ## 相同时间点(日期相同，秒数相同)的同方向股票买入进行合并，并按照时间顺序输出结果
    MergeSort()




    '''
    data_dir = "C:\\Data\\ETFProfit\\WorkDir\\hua\\merge\\"
    data_file= data_dir + "sort_result.txt"
    fin = open(data_file, 'r')
    for line in fin:
        print line.strip()
    '''


if __name__=="__main__":
    main()