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
import os

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


def diff_time_ok(time1,time2,ok_days):
    #curtransday = datetime.datetime.strptime(time1, '%Y-%m-%d')
    #lastransday = datetime.datetime.strptime(time2, '%Y-%m-%d')
    #diffdays = curtransday - lastransday
    diffdays = time1 - time2
    diffdays = int(diffdays.days)
    if diffdays > ok_days:
        print("%s 减 %s 等于 %s ,日大于 %s 天"  %(time1,time2,diffdays,ok_days))
        return True
    else:
        print("%s 减 %s 等于 %s ,日小于 %s 天"  %(time1,time2,diffdays,ok_days))
        return False


'''
多线程抓取基金明细后合并的结果fundlist pd，对该pd中的基金进净值涨幅计算,该fundlist中的基金数据是非串行的，时间倒序的。
所以不能用索引名字（默认行号）进行基金净值获取，如当前index的名称100，不是加5，105就是上周基金净值所在行。
过滤出某只基金的明细数据，iloc + 5 一定是前一周的。
type分为按month、week进行计算; daytype指工作日WD还是自然日CD，daynum指month或者week中间隔的日子数量,period_n 只计算出多少个周期的
'''
def fund_rate_mt(fundlist, type, daytype, daynum, period_n):
    # 将fundlist中基金代码去重，并转换成列表
    list1 = fundlist['基金代码'].drop_duplicates().values.tolist()
    global lasttrans_day

    start_time = time.time()
    # 按照工作日来处理，那么是根据fundlist数据集中的index来定位。 处理daynum=5 则对应着week，daynum=22则对应着month，获取对应数据
    if daytype == "WD":
        # 创建一个pd，用于存放计算出来的每月涨幅数据。 如果传入的非全量基金明细pd，这里的基金代码要调整为pd中包含的基金代码
        fund_xx = fundlist[fundlist['基金代码'] == "000083"]
        fund_idxname = []

        # 计算周数据，查看最近period_n周的数据，一般短期看最近6周的就够了。 同时要计算一下不同类型需要的最少数据是多少
        if type == "week":
            howlong = period_n
            min_count = daynum * period_n + 2
            # 否则月数据，查看最近period_n个月的数据，一般中长期看最近6个月的就够了
        if type == "month":
            howlong = period_n
            min_count = daynum * period_n + 2

        # 单独过滤出来的某只基金的pd，索引编号都是0开始，索引名称并不一定是顺序的（因为多线程抓取的原因）,所以下面都用iloc，
        # 列从0开始，2表示第3列，是净值日期
        fund_index1 = 0
        fund_day = fund_xx.iloc[fund_index1, 2]
        fund_idxname.append(fund_day)
        m = 0
        while m < howlong:
            fund_index1 = fund_index1 + daynum
            fund_day = fund_xx.iloc[fund_index1, 2]
            fund_idxname.append(fund_day)
            m = m + 1

        fund_idxname.pop()
        fund_idxname.insert(0, '最新日期')
        fund_idxname.insert(0, '基金简称')
        fund_idxname.insert(0, '基金代码')
        fundrate_result = pd.DataFrame(columns=fund_idxname)
        print(fundrate_result)
        # 保存基金涨幅的索引号
        j = 0

        # 计算完成的基金数量
        finished_count = 0

        # 开始计算每个基金的涨幅
        for fund in list1:
            fund_xx = fundlist[fundlist['基金代码'] == fund]
            fund_indexs = []
            fund_rates = []
            if len(fund_xx.index) < min_count:
                print("%s has no enough count" % (fund))
                continue

            m = 0
            # 单独过滤出来的某只基金的pd，索引编号都是0开始，索引名称并不一定是顺序的（因为多线程抓取的原因）,所以下面都用iloc
            fund_index1 = 0
            # 检查当前基金最新的净值日期是否为最近3个交易日，如果不是则不进行后面的计算
            fund_day = fund_xx.iloc[fund_index1, 2]
            if diff_time_ok(lasttrans_day, fund_day, 3):
                print("%s last trans day is %s, too old" % (fund, fund_day))
                continue

            fund_indexs.append(fund_index1)
            while m < howlong:
                fund_index1 = fund_index1 + daynum
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
            fund_rates.insert(0, lasttrans_day)
            fund_rates.insert(0, name)
            fund_rates.insert(0, fund)
            print(fund_rates)
            fundrate_result.loc[j] = fund_rates
            j = j + 1

            finished_count += 1
            logger.info(f"Processing Progress: {finished_count}/{len(list1)}. "
                        f"Time: {time.time() - start_time:.2f}s")
    # 按照datetype=CD来处理
    else:
        print("I don't finish this part")
        # if type=="week":

        # 按照type=="month"来处理
        # else:

    return fundrate_result



#对排序获得的集合数据pddata, 进行画图并保存图片名为picname， wz 是画图数据的列的位置， *col是从pddata中选择哪些列的列表
def pic_execl(pddata,picname,st,wz,*col):
    global today_imagesdir
    #定义画线用的颜色
    colors=["pink","fuchsia","darkorchid","blue","cyan","lime","green","yellowgreen","yellow","orange","red","gray","peru","black","olive"]
    i = 0
    #pdata = pd.DataFrame(pddata,columns=['序号', '基金代码', '基金简称', '日期', '自选', '最近1月', '最近2-3月', '最近4-6月', '最近7-12月', '最近1-2年', '最近2-3年', '从前'])
    if len(col) > 0 :
        pdata = pd.DataFrame(pddata,columns=col)
    else:
        pdata = pddata
    # print ("--pdata---")
    # print(pdata)
    #横坐标是代表周期的列名，从0开始，这里是从4开始，到传入的位置截至。 如上面的列子是从："自选"开始
    xtime = pdata.columns.values[st:wz]
    # print("---xtime---")
    # print(xtime)
    #取每一行的数据组成曲线图，并将"基金代码，基金简称" 作为曲线名称
    for index, row in pdata.iterrows():
        # print(list(row)[5:-1])
        plt.plot(xtime, list(row)[st:wz], '.-', label=list(row)[0:2], color=colors[i])
        i = i + 1
    plt.xticks(xtime)
    plt.xticks(rotation=90)
    fig, ax = plt.subplots()
    plt.figure(1)
    plt.xlabel('周期')
    plt.ylabel('涨幅百分比')
    plt.legend(loc='center left',bbox_to_anchor=(1.0,0.5))
    #fig.subplots_adjust(right=0.6)
    plt.savefig(today_imagesdir + "/" + time.strftime("%Y%m%d", time.localtime()) + "_" + picname + ".png",dpi=600,bbox_inches='tight')
    plt.show()


'''
将传入的计算好的包含基金rate的 pd， 按照最近period_n个周期，对每个周期进行按rate排序，取top_n。 然后再将这些周期的top_n的 pd取交集。
shengjiang 传入的为True 则为升序（取得最近涨幅最低得）， 传入的False则为降序(取得最近涨幅最高得)
'''
def fund_rate_sort(fund_ratelist,period_n,top_n,shengjiang):
    #定义一个动态变量
    names = locals()
    #要排序几个周期
    sortlist=period_n

    m=0
    #fund_ratelist的前3列为：基金代码	基金简称	最新日期， 所以从第4列(即下标3）开始排序，并取前top_n 行的 pd
    n=3

    for i in range(sortlist):
        #将每列的top_n, 然后付给动态变量
        names['s'+ str(i)] = fund_ratelist.sort_values(by=fund_ratelist.columns.values[n],ascending=shengjiang).head(top_n)
        print(str(i) + "*** fund_top ***")
        #展示动态变量中的pd
        print(names.get('s'+ str(i)), end = '\n' )
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
将传入的计算好的包含基金rate的 pd， 
postion ， 从最近第几个周期开始的位置（0代表最近的周期开始，1代表上一个周期开始，2代表上上个周期），例如：0 代表当前的10月，1代表9月，2代表8月
period_n , 按照最近period_n个周期，对每个周期进行按rate排序，取top_n。 然后再将这些周期的top_n的 pd取交集。
shengjiang 传入的为True 则为升序（取得最近涨幅最低得）， 传入的False则为降序(取得最近涨幅最高得)
'''
def fund_rate_sort_bypostion(fund_ratelist, postion, period_n, top_n, shengjiang):
    recent_period_name = fund_ratelist.columns.values.tolist()[3]
    # 定义一个动态变量
    names = locals()

    # 要排序几个周期
    sortlist = period_n

    m = 0
    # fund_ratelist的前3列为：基金代码	基金简称	最新日期， 所以从第4列(即下标3）开始排序，并取前top_n 行的 pd
    n = 3 + postion
    for i in range(sortlist):
        # 将每列的top_n, 然后付给动态变量
        names['s' + str(i)] = fund_ratelist.sort_values(by=fund_ratelist.columns.values[n], ascending=shengjiang).head(
            top_n)
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
    print("****** 开始反转的 inner pd *****")
    inner_sort = inner_sort[inner_sort[recent_period_name] > 1]
    return inner_sort


def generate_html_page(output_file, title, content_list):
    """
    生成完整HTML页面
    :param output_file: 输出文件名
    :param title: 页面标题
    :param content_list: 内容列表，每个元素是包含(image_path, df)的元组
    """
    html_content = []

    # 生成页面头部
    html_content.append(f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <style>
        body {{ 
            font-family: Arial, sans-serif; 
            margin: 2em;
            background-color: #f5f5f5;
        }}
        .content-block {{
            margin-bottom: 3em;
            padding: 1.5em;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .image-container {{
            text-align: center;
            margin-bottom: 1em;
        }}
        img {{
            max-width: 80%;
            height: auto;
            border-radius: 4px;
        }}
        .table-container {{
            max-height: 400px;
            overflow-y: auto;
            margin: 0 auto;
            width: 95%;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 1em 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            position: sticky;
            top: 0;
        }}
        tr:nth-child(even){{background-color: #f2f2f2;}}
        h1 {{
            color: #333;
            text-align: center;
            margin-bottom: 1.5em;
        }}
    </style>
</head>
<body>
    <h1>{title}</h1>""")

    # 生成内容区块
    for idx, (img_path, df) in enumerate(content_list, 1):
        # 图片部分
        html_content.append(f"""
        <div class="content-block">
            <div class="image-container">
                <img src="{img_path}" alt="图片{idx}">
            </div>""")

        # 表格部分
        html_content.append("""
            <div class="table-container">""")
        html_content.append(df.head(20).to_html(index=False, classes="data-table"))
        html_content.append("""
            </div>
        </div>""")

    # 生成页面尾部
    html_content.append("""
</body>
</html>""")

    # 写入文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(html_content))


if __name__ == "__main__":
    # 初始化数据库
    allfund_data=read_funddata_to_dataframe()
    print("ALL FUND DATA: \n" )
    print(allfund_data)
    print("000018-----------fundata")
    print(allfund_data[allfund_data['fund_code'] == "970214"])

    lasttrans_day = get_previos_workday()
    allfund_data = allfund_data.rename(columns={'fund_code': '基金代码', 'fund_name': '基金名称', 'date': '净值日期', 'net_value': '单位净值' })
    allfund_data.sort_values(by='净值日期', ascending=False, inplace=True)

    allfund_data['单位净值'] = pd.to_numeric(allfund_data['单位净值'])

    # 将所有基金的明细，按周计算增幅，每周5个工作日， 取最近6周每周的涨幅
    all_w_rate1 = fund_rate_mt(allfund_data, "week", "WD", 5,6)
    print("计算出的基金周期涨幅数据")
    print(all_w_rate1)

    #创建目录用于保存后面生成的图
    today = datetime.date.today()
    today_dir = str(today)
    today_imagesdir = today_dir + "/images"
    os.makedirs(today_imagesdir)

    #用于传入生成html的列表，包含图片和对应的交集数据
    content = []

    ###############################################
    ###将所有基金周涨幅，取最近2周，每周top1000，求交集
    all_week2_top_rate = fund_rate_sort(all_w_rate1, 2, 1000, False)

    # 对所有基金明细交集的内容，取前10，画图
    if len(all_week2_top_rate) > 0:
        pic_execl(all_week2_top_rate[:10], "all_top1000_2week", 3, 9)
        content.append(("images/"+time.strftime("%Y%m%d", time.localtime())+"_all_top1000_2week.png", all_week2_top_rate))
    else:
        print("最近2周的TOP1000没有交集")

    ###############################################
    ###将所有基金周涨幅，取最近3周，每周top1000，求交集
    all_week3_top_rate = fund_rate_sort(all_w_rate1, 3, 1000, False)

    # 对所有基金明细交集的内容，取前10，画图
    if len(all_week3_top_rate) > 0:
        pic_execl(all_week3_top_rate[:10], "all_top1000_3week", 3, 9)
        content.append(("images/"+time.strftime("%Y%m%d", time.localtime())+"_all_top1000_3week.png", all_week3_top_rate))
    else:
        print("最近3周的TOP1000没有交集")

    ###############################################
    ###将所有基金周涨幅，取最近4周，每周top1000，求交集
    all_week4_top_rate = fund_rate_sort(all_w_rate1, 4, 1000, False)

    # 对所有基金明细交集的内容，取前10，画图
    if len(all_week4_top_rate) > 0:
        pic_execl(all_week4_top_rate[:10], "all_top1000_4week", 3, 9)
        content.append(("images/"+time.strftime("%Y%m%d", time.localtime())+"_all_top1000_4week.png", all_week4_top_rate))
    else:
        print("最近4周的TOP1000没有交集")

    ###############################################
    # 将所有基金周涨幅，取最近3周不在跌幅top3000里，再往前3周下跌且每周跌幅都在bottom3000，求交集
    all_week_bottom_rate = fund_rate_sort_bypostion(all_w_rate1, 3, 3, 3000, True)

    # 对所有基金明细交集的内容，取最差涨幅的前10，画图
    if len(all_week_bottom_rate) > 0:
        pic_execl(all_week_bottom_rate[:10], "all_fanzhuan_bottom3000_3week", 3, 9)
        content.append(("images/"+time.strftime("%Y%m%d", time.localtime())+"_all_fanzhuan_bottom3000_3week.png", all_week_bottom_rate))
    else:
        print("最近5周的BOTTOM3000没有反转")

    #生成网页
    generate_html_page(today_dir+"/"+today_dir+".html", str(lasttrans_day)+"分析报告", content)

    # 配置定时任务（每天18:00执行）
    #scheduler = BlockingScheduler()
    #scheduler.add_job(main_job, 'cron', hour=18, minute=0)

    #try:
    #    logger.info("Scheduler started...")
    #    scheduler.start()
    #except (KeyboardInterrupt, SystemExit):
    #    logger.info("Scheduler stopped.")