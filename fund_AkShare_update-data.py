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

# 全局变量
MAX_WORKERS = 1  # 最大线程数
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


def get_update_fund():
    """处理单个基金数据"""
    try:
        logger.info(f"get new daily data")

        df = ak.fund_open_fund_daily_em()
        #df   [基金代码 基金简称 2025-02-24-单位净值 2025-02-24-累计净值 2025-02-21-单位净值 2025-02-21-累计净值 日增长值 日增长率 申购状态 赎回状态 手续费]
        riqi = df.columns[2][0:10]
        jingzhi = df.columns[2]
        print("----riqi----")
        print(riqi)
        df1 = df.iloc[:, [0, 1, 2, 7]]
        df1['净值日期'] = riqi
        print("-----df1-----")
        print(df1)
        df1.rename(columns={jingzhi:'单位净值'}, inplace=True)

        df2 = df1[['基金代码', '基金简称', '净值日期', '单位净值', '日增长率']]
        print("-----df2-----")
        print(df2)
        time.sleep(5)
        if not df2.empty:
            # 线程安全的数据保存
            with lock:
                save_daily_data(df2)
            return True
        return False
    except Exception as e:
        logger.error(f"Error processing {df}: {e}")
        return False





if __name__ == "__main__":
    # 更新每天的新的基金数据
    # 配置定时任务（每天23:00执行）
    get_update_fund()
    scheduler = BlockingScheduler()
    scheduler.add_job(get_update_fund, 'cron', hour=23, minute=0)

    try:
        logger.info("Scheduler started...")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")