import akshare as ak
import pandas as pd
import pymysql
from datetime import datetime, timedelta
from functools import lru_cache
import time
import logging
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import datetime


# 这两行代码解决 plt 中文显示的问题
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Fmj961688',
    'database': 'fund_db',
    'charset': 'utf8mb4'
}

# 创建引擎  sqlalchemy
engine = create_engine("mysql+pymysql://root:Fmj961688@localhost:3306/fund_db?charset=utf8mb4")




def get_previos_workday():
    #如果是星期一 (0) 则向前推2天
    curtransday = datetime.date.today()
    if curtransday.weekday() == 0:
        return curtransday - timedelta(days=3)
    #如果是星期六 (5) 则向前推1天
    elif curtransday.weekday() == 5:
        return curtransday - timedelta(days=1)
    #如果是星期天 (6) 则向前推2天
    elif curtransday.weekday() == 6:
        return curtransday - timedelta(days=2)
    #其他日期，直接返回工作日
    else:
        return curtransday

def read_funddata_to_dataframe():
    try:

        sql = """
            SELECT fund_code,fund_name,date,net_value 
            FROM fund_db.fund_daily_data 
        """

        df_sql = pd.read_sql_query(sql, engine)
        print("通过SQL获取所有基金的数据DataFrame:")
        print(df_sql.head())
        return df_sql

    except Exception as e:
        print(f"数据库操作失败: {e}")
    finally:
        # 确保释放数据库连接池资源
        if engine is not None:
            engine.dispose()
            print("数据库连接已释放")

#列表中的值求和
def sum_list(items):
    sum_numbers = 0
    for x in items:
        sum_numbers += x
    return sum_numbers


'''
多线程抓取基金明细后合并的结果fundlist pd，对该pd中的基金进净值涨幅计算,该fundlist中的基金数据是非串行的，时间倒序的。
所以不能用索引名字（默认行号）进行基金净值获取，如当前index的名称100，不是加5，105就是上周基金净值所在行。
过滤出某只基金的明细数据，iloc + 5 一定是前一周的。
st_day是当前进行回测的日期, 如这种格式"2020-08-05" 
daytype指工作日WD还是自然日CD，
daynum：指month或者week中间隔的日子数量,daynum=5 则对应着week，daynum=23则对应着month，
period_n 是一个list，包含要计算出多少个周期的，[1,2,3,6]代表从回测日期向前1个月这段时间的增幅，然后再向前2个月的增幅，然后再
window 是持有周期，对应着几个 daynum
'''


def fund_period_rate(fundlist, st_day, daytype, daynum, period_n, window):
    # 将fundlist中基金代码去重，并转换成列表
    list1 = fundlist['基金代码'].drop_duplicates().values.tolist()
    all_count = len(list1)
    global lasttrans_day

    # 按照工作日来处理，那么是根据fundlist数据集中的index来定位。
    if daytype == "WD":
        # 创建一个pd，用于存放计算出来的每月涨幅数据。 这里存放列名
        fund_idxname = []

        # 将period_n 中的总周期计算出来，便于计算需要的最小数据量
        howlong = sum_list(period_n)
        min_count = daynum * howlong + daynum * window + 2
        # period_n 中周期的个数
        period_lenth = len(period_n)

        # 单独过滤出来的某只基金的pd，索引编号都是0开始，索引名称并不一定是顺序的（因为多线程抓取的原因）,所以下面都用iloc，
        # 列从0开始，2表示第3列，是净值日期
        # 从全量基金中过滤初000083的pd，是按照日期降序的
        fund_tmp = fundlist[fundlist['基金代码'] == "000083"]
        # 这里重置索引名称，将索引编号重置为从0开始
        fund_xx = fund_tmp.reset_index(drop=True)
        # 从中找出st_day开始的索引号
        fund_index1 = fund_xx[fund_xx.净值日期 == st_day].index.tolist()[0]

        fund_day = st_day
        fund_idxname.append(fund_day)

        m = 0
        while m < period_lenth:
            fund_index1 = fund_index1 + daynum * period_n[m]
            fund_day = fund_xx.iloc[fund_index1, 2]
            fund_idxname.append(fund_day)
            m = m + 1

        fund_idxname.pop()
        fund_idxname.insert(0, '筛选时间')
        fund_idxname.insert(0, '基金简称')
        fund_idxname.insert(0, '基金代码')
        fund_idxname.append('持有时间')
        fund_idxname.append('持有收益')
        fundrate_result = pd.DataFrame(columns=fund_idxname)
        print(fundrate_result)
        # 保存基金涨幅的索引号
        j = 0
        # 开始计算每个基金的涨幅
        for fund in list1:
            fund_tmp = fundlist[fundlist['基金代码'] == fund]
            fund_xx = fund_tmp.reset_index(drop=True)
            print("-------------------------------------------------------------------------")
            # print("-- %s 的详细数据: %s" %(fund,fund_xx))
            fund_indexs = []
            fund_rates = []
            if len(fund_xx.index) < min_count:
                print("%s 总数据量不足 %d ,当前有%d" % (fund, min_count, len(fund_xx.index)))
                continue

            # 单独过滤出来的某只基金的pd，索引编号都是0开始，索引名称并不一定是顺序的（因为多线程抓取的原因）,所以下面都用iloc
            # 从中找出st_day开始的索引号
            try:
                fund_index1 = fund_xx[fund_xx.净值日期 == st_day].index.tolist()[0]
                print("%s 的索引号为: %s" % (st_day, fund_index1))
            except:
                print("%s 的索引号没有找到" % (st_day))
                continue

            # 判断测试开始日期之后的持有数据量是否满足
            if (fund_index1 - daynum * window) < 0:
                print("开始测试日期: %s 之后需要持有数据量为: %s, 当前持有数据量为: %s" % (
                st_day, daynum * window, fund_index1))
                continue
            fund_indexs.append(fund_index1)
            # 找到st_day的净值
            cur_value = fund_xx.iloc[fund_index1, 3]

            # 判断测试开始日期之前的回测周期数据量是否满足
            if (len(fund_xx.index) - daynum * howlong - 1) < fund_index1:
                print("开始测试日期: %s 之前需要回测数据量为: %s, 当前回测数据量为: %s" % (
                st_day, daynum * howlong, len(fund_xx.index) - fund_index1 - 1))
                continue

            # 找到持有日期和对应的持有净值
            fund_get = fund_index1 - daynum * window
            get_day = fund_xx.iloc[fund_get, 2]
            get_value = fund_xx.iloc[fund_get, 3]
            print("持有日期: %s 的索引号为: %s" % (get_day, fund_get))
            # 持有收益率
            cur_value = float(cur_value)
            get_value = float(get_value)
            result_rate = round((get_value - cur_value) / cur_value * 100, 2)

            m = 0
            while m < period_lenth:
                fund_index1 = fund_index1 + daynum * period_n[m]
                fund_indexs.append(fund_index1)

                # 列从0开始，3表示第4列，是基金净值
                first_value = fund_xx.iloc[fund_indexs[m], 3]
                second_value = fund_xx.iloc[fund_indexs[m + 1], 3]
                first_value = float(first_value)
                second_value = float(second_value)
                first_rate = round((first_value - second_value) / second_value * 100, 2)
                fund_rates.append(first_rate)
                m = m + 1
            name = fund_xx.iloc[fund_indexs[m], 1]
            fund_rates.insert(0, st_day)
            fund_rates.insert(0, name)
            fund_rates.insert(0, fund)

            fund_rates.append(get_day)
            fund_rates.append(result_rate)
            print(fund_rates)
            fundrate_result.loc[j] = fund_rates
            j = j + 1

            #print(
            #    "开始测试日期: %s , All_Founds_Count: %d , Finished_Count: %d , 基金名称: %s , Finished_Rate: %f %%" % (
            #    st_day, all_count, j, name, float(j / all_count * 100)))

            logger.info(f"开始测试日期: {st_day}, " 
                        f"All_Founds_Count: {all_count}, "
                        f"Finished_Count: {j}, " 
                        f"基金名称: {name}, "
                        f"Finished_Rate: {float(j / all_count * 100)} %")
    # 按照datetype=CD来处理
    else:
        print("I don't finish this part")
        # if type=="week":

        # 按照type=="month"来处理
        # else:

    return fundrate_result


'''
将传入的计算好的包含基金rate的 pd， 按照最近period_n个周期，对每个周期进行按rate排序，取top_n。 然后再将这些周期的top_n的 pd取交集
'''


def fund_rate_sort(fund_ratelist, period_n, top_n):
    # 定义一个动态变量
    names = locals()

    # 要排序几个周期
    sortlist = period_n

    m = 0
    # fund_ratelist的前3列为：基金代码	基金简称	最新日期， 所以从第4列(即下标3）开始排序，并取前top_n 行的 pd
    n = 3

    for i in range(sortlist):
        # 将每列的top_n, 然后付给动态变量
        names['s' + str(i)] = fund_ratelist.sort_values(by=fund_ratelist.columns.values[n], ascending=False).head(top_n)
        print(str(i) + "*** fund_top ***")
        # 展示动态变量中的pd
        print(names.get('s' + str(i)), end='\n')
        m = m + 1
        n = n + 1

    # 将动态变量中包含每个rate列top_n的pd，求交集
    j = 0
    k = 1
    inner_sort = pd.merge(names.get('s' + str(j)), names.get('s' + str(k)), how='inner')
    k = k + 1
    while k < sortlist:
        inner_sort = pd.merge(inner_sort, names.get('s' + str(k)), how='inner')
        k = k + 1
    print("****** sort innner pd *****")
    print(inner_sort)
    return inner_sort


'''
fund_pd : 包含全量基金净值数据的pd
st_day: 开始测试的时间
daynum_window: 传入的工作日窗口天数， 5代表1周，23代表1个月
period_test: 要回测的周期， 是从st_day向前数 daynum_window * period_test天
period_get: 持有的周期, 持有 daynum_window * period_get 工作日天数，
earn_rate: 期望收益率数值，如5代表5%， 用这个收益率来衡量模型的查准率和查全率；
model : 筛选基金收益率的周期列表，如： [1,2,3,6] 代表选择最近1个月，然后再往前2个月， 再往前3个月，再往前6个月， 正好1年。
'''


def model_eva(fund_pd, st_day, daynum_window, period_test, period_get, earn_rate, model):

    # 从全量基金中过滤初000083的pd，是按照日期降序的
    fund_tmp = fund_pd[fund_pd['基金代码'] == "000083"]
    # 这里重置索引名称，将索引编号重置为从0开始
    fund_xx = fund_tmp.reset_index(drop=True)
    print("---fund 000083---")
    print(fund_xx)
    # 单只基金的数据量
    count = len(fund_xx)
    print("---000083 count----")
    print(count)

    fund_index1 = fund_xx[fund_xx['净值日期'] == st_day].index.tolist()[0]

    # 定义一个测试窗口日期的列表
    test_days = []
    test_days.append(st_day)
    i = 1
    while i < period_test * daynum_window:
        fund_index1 = fund_index1 + 1
        tday = fund_xx.iloc[fund_index1, 2]
        test_days.append(tday)
        i = i + 1
    print("测试窗口日期: %s" % (test_days))
    # 采用当前选基金的模型，1，2-3，4-6选取topn200的交集
    period_model = model
    topn = 200

    # 定义一个列表，保存测试窗口所有计算的pd
    all_list = []
    # 定义一个列表，保存测试窗口中选取出来的pd
    get_list = []

    # 针对测试窗口日期，每一个都进行周期rate排名，取交集
    for day in test_days:
        print("--- Test day : %s --" % (day))
        fund_rate = fund_period_rate(fund_pd, day, "WD", daynum_window, period_model, period_get)
        fund_get = fund_rate_sort(fund_rate, len(period_model), topn)
        all_list.append(fund_rate)
        get_list.append(fund_get)

    result_all = pd.concat(all_list)
    # 将测试窗口中每天取到的结果合并为一个pd
    get_all = pd.concat(get_list)

    # 计算查全率
    recall_son = len(get_all[get_all['持有收益'] > earn_rate])
    recall_mon = len(result_all[result_all['持有收益'] > earn_rate])
    recall = recall_son / recall_mon
    print("测试开始日期为：%s ,测试结束日期：%s , 选取收益率为：%f, 模型筛选出来的召回率为: %f" % (
    st_day, test_days[-1], earn_rate, recall))
    # 计算查准率
    acc_son = len(get_all[get_all['持有收益'] > earn_rate])
    acc_mon = len(get_all)
    acc = acc_son / acc_mon
    print("测试开始日期为：%s ,测试结束日期：%s , 选取收益率为：%f, 模型筛选出来的查准率为: %f" % (
    st_day, test_days[-1], earn_rate, acc))



if __name__ == "__main__":
    allfund_data = read_funddata_to_dataframe()

    #对列名进行重命名
    allfund_data = allfund_data.rename(columns={'fund_code': '基金代码', 'fund_name': '基金名称', 'date': '净值日期', 'net_value': '单位净值'})
    #对数据进行按日期倒序排列
    allfund_data.sort_values(by='净值日期', ascending=False, inplace=True)
    #将日期列的datetime格式转为字符串格式
    allfund_data = allfund_data.astype({'净值日期': 'string'})
    print("---allfund_data---")
    print(allfund_data)
    model = [1, 1, 1]
    model_eva(allfund_data, '2025-02-12', 5, 3, 2, 5, model)