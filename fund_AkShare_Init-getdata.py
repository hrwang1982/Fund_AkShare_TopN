import akshare as ak
import pandas as pd
import pymysql
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from functools import lru_cache
import time
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine

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

# 全局变量
MAX_WORKERS = 2  # 最大线程数
CACHE_SIZE = 1000  # 缓存大小
TABLE_LIST = 'fund_list'  # 基金列表表名
TABLE_DAILY = 'fund_daily_data'  # 每日数据表名
lock = Lock()  # 用于线程安全操作


def create_database_tables():
    """创建数据库和表结构"""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            # 创建基金列表表
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_LIST} (
                    fund_code VARCHAR(20) PRIMARY KEY,
                    fund_name VARCHAR(255)
            )""")
            # 创建每日数据表
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_DAILY} (
                    fund_code VARCHAR(20),
                    fund_name VARCHAR(255),
                    date DATE,
                    net_value DECIMAL(10,4),
                    accumulative_net_value DECIMAL(10,4),
                    PRIMARY KEY (fund_code, date),
                    FOREIGN KEY (fund_code) REFERENCES {TABLE_LIST}(fund_code)
                )
            """)
        conn.commit()
        logger.info("Tables created/verified successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
    finally:
        conn.close()


def read_distinct_data_to_dataframe():
    try:
        # 连接数据库
        conn = pymysql.connect(**DB_CONFIG)

        # 示例：根据 name 和 email 去重（替换成实际字段）
        sql = """
            SELECT DISTINCT fund_code, fund_name 
            FROM fund_db.fund_daily_data
        """

        df_sql_deupe = pd.read_sql_query(sql, engine)
        print("通过SQL去重后的DataFrame:")
        print(df_sql_deupe.head())
        return df_sql_deupe

    except Exception as e:
        print(f"数据库操作失败: {e}")
    finally:
        conn.close()


def read_funddatacount_to_dataframe():
    try:

        sql = """
            SELECT fund_code,fund_name,count(1) as count
            FROM fund_db.fund_daily_data GROUP BY fund_code,fund_name
        """

        df_sql = pd.read_sql_query(sql, engine)
        print("通过SQL获取每个基金的数据量DataFrame:")
        print(df_sql.head())
        return df_sql

    except Exception as e:
        print(f"数据库操作失败: {e}")
    finally:
        # 确保释放数据库连接池资源
        if engine is not None:
            engine.dispose()
            print("数据库连接已释放")



def fund_need_getdata(alldf, count_num):
    #alldf 是包含了基金代码、基金代码名称、当前的数据条数,
    #count_num是判断标准，当前的基金在数据库中的数据条数大于该值，则认为不需要全量抓取；小于该值，则认为需要初始化抓取最近3个月的值
    need_initget_fundlist = pd.DataFrame(columns=["基金代码","基金简称","数据量"])
    for index, row in alldf.iterrows():
        code = row["fund_code"]
        name = row["fund_name"]
        data_count = row["count"]
        print("对当前的基金进行判断:  %s - %s -%s " % (code, name, data_count))
        if data_count < count_num:
            print(">>>>>>需要进行初始化获取: %s - %s - %d " % (code, name, data_count))
            row_df = pd.DataFrame(row).T
            need_initget_fundlist = pd.concat([need_initget_fundlist, row_df], ignore_index=True)
    print("below %s fund need init get data:\n %s" % (len(need_initget_fundlist), need_initget_fundlist))
    return need_initget_fundlist


def get_fund_list():
    """获取所有基金列表并更新数据库"""
    try:
        # 从AKshare获取基金列表
        #这个是获取所有的历史数据
        #df = ak.fund_name_em()
        #我们只想要当前还在交易的且有新数据的基金，所以换成这个接口
        df = ak.fund_open_fund_daily_em()
        df = df[['基金代码', '基金简称']].rename(columns={
            '基金代码': 'fund_code',
            '基金简称': 'fund_name'
        })
        logger.info("funds list...")
        print(df)
        time.sleep(5)
        # 更新到数据库
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            # 使用批量插入并忽略重复
            sql = f"""
                INSERT IGNORE INTO {TABLE_LIST} (fund_code, fund_name)
                VALUES (%s, %s)
            """
            data = [tuple(row) for row in df.values]
            cursor.executemany(sql, data)
            conn.commit()
            logger.info(f"Inserted/Updated {len(df)} funds into {TABLE_LIST}")
        #return df['fund_code'].tolist()
        return df
    except Exception as e:
        logger.error(f"Error getting fund list: {e}")
        return []


@lru_cache(maxsize=CACHE_SIZE)
def get_fund_daily_data(fund_code):
    """带缓存的基金数据获取（缓存当天的请求）"""
    try:
        # 计算日期范围：最近3个月
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

        # 获取基金数据
        df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")

        if df.empty:
            return pd.DataFrame()

        # 数据清洗
        #df = df[['净值日期', '单位净值', '日增长率']]
        #df.columns = ['date', 'net_value', 'accumulative_net_value']
        df['基金代码'] = fund_code
        df['净值日期'] = pd.to_datetime(df['净值日期']).dt.date
        logger.info(f" {fund_code}: {start_date} -- {end_date} ")
        #print(df)

        # 筛选最近3个月的数据
        mask = (df['净值日期'] >= pd.to_datetime(start_date).date()) & \
               (df['净值日期'] <= pd.to_datetime(end_date).date())
        return df[mask]
    except Exception as e:
        logger.error(f"Error getting data for {fund_code}: {e}")
        return pd.DataFrame()


def save_daily_data(data):
    """批量保存每日数据到数据库"""
    if data.empty:
        return
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            sql = f"""
                INSERT IGNORE INTO {TABLE_DAILY} 
                (fund_code, fund_name, date, net_value, accumulative_net_value)
                VALUES (%s, %s, %s, %s, %s)
            """
            data_tuples = [tuple(row) for row in data.itertuples(index=False)]
            cursor.executemany(sql, data_tuples)
            conn.commit()
            logger.info(f"Inserted {len(data_tuples)} records into {TABLE_DAILY}")
    except Exception as e:
        logger.error(f"Error saving daily data: {e}")
    finally:
        conn.close()


def process_fund(fund_code,fund_name):
    """处理单个基金数据"""
    try:
        logger.info(f"get {fund_code} - {fund_name} daily_data")

        df = get_fund_daily_data(fund_code)
        df['基金简称'] = fund_name
        df1=df[['基金代码', '基金简称', '净值日期', '单位净值', '日增长率']]
        print(df1)
        time.sleep(2)
        if not df1.empty:
            # 线程安全的数据保存
            with lock:
                save_daily_data(df1)
            return True
        return False
    except Exception as e:
        logger.error(f"Error processing {fund_code}: {e}")
        return False


def main_job():
    """定时任务主函数"""
    logger.info("Starting main job...")
    start_time = time.time()

    current_fundcount = read_funddatacount_to_dataframe()
    lens = len(current_fundcount)
    logger.info("There are {} funds in database fund_db ".format(lens))
    #如果数据库中存在历史数据，可以通过判断满足的历史数据的进行增量抓取；否则全新抓取
    #存在大于500个基金的数据，则增量
    if lens > 500:
        fund_codes = fund_need_getdata(current_fundcount, 58)
        logger.info(f"There are {len(fund_codes)} funds need to init get data ")
    else:
        # 获取基金列表
        fund_codes = get_fund_list()
        logger.info(f"Init get data firsttime")
    codelist=fund_codes['fund_code'].tolist()
    if not codelist:
        return

    success_count = 0
    # 使用线程池处理
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
       #futures = {executor.submit(process_fund, code): code for code in fund_codes}
         for index,row in fund_codes.iterrows():
            code = row['fund_code']
            name = row['fund_name']
            future = executor.submit(process_fund, code, name)
            if future.result():
                success_count += 1
                logger.info(f"Job completed. Success: {success_count}/{len(fund_codes)}. "
                    f"Time: {time.time() - start_time:.2f}s")
    #print("executor status : %s" % futures.result())




if __name__ == "__main__":
    # 初始化数据库
    create_database_tables()
    main_job()
    # 配置定时任务（每天18:00执行）
    #scheduler = BlockingScheduler()
    #scheduler.add_job(main_job, 'cron', hour=18, minute=0)

    #try:
    #    logger.info("Scheduler started...")
    #    scheduler.start()
    #except (KeyboardInterrupt, SystemExit):
    #    logger.info("Scheduler stopped.")