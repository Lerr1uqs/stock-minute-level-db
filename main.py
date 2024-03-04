
from datetime import datetime as Datetime
from datetime import time as dttime, timedelta
import time
import pandas as pd
from ashare import ashare
from typing import Dict, List, NoReturn, Optional, Union
from abc import abstractmethod
from loguru import logger
import collections
import pickle
import os 
from threading import Thread
from tqdm.rich import tqdm
from generic.main import StocksManager
from generic.main import trading_dates
import akshare as ak
import config

pd.set_option('display.unicode.east_asian_width', True) #设置输出右对齐
from warnings import simplefilter
simplefilter(action="ignore", category=FutureWarning)

# name2code = {}
# code2name = {}
# codes: List[str] = [] # 所有要入库的股票代码 e.g. 000001.SZ
# traderdate_isopen: Dict[Datetime, bool] = {}
# trading_dates: List[str] = [] # e.g. 20241231
manifested: List[str] = [] # 已经收录过的日期
DT_STDFMT = r"%Y%m%d" # datetime standard format 

MANIFEST : pd.DataFrame = pd.read_csv("manifest.csv")

for (idx, line) in MANIFEST.iterrows():
    manifested.append(str(line["trade_date"]))
        
# def load():
#     '''
#     生成所有所需要的全局变量
#     '''
#     NAME2CODE: pd.DataFrame = pd.read_csv("generic/name2code.csv")
#     CALANDER : pd.DataFrame = pd.read_csv("generic/calander.csv")

#     for (idx, line) in NAME2CODE.iterrows():
#         name2code[line["name"]] = line["ts_code"]
#         code2name[line["ts_code"]] = line["name"]
#         codes.append(str(line["ts_code"]))

#     for (idx, line) in CALANDER.iterrows():
#         if line["is_open"] == 1:
#             trading_dates.append(str(line["cal_date"]))

#     trading_dates.reverse() # 因为 CALANDER中是倒序
    
#     for (idx, line) in MANIFEST.iterrows():
#         if line["embodied"] == 1:
#             manifested.append(str(line["trade_date"]))
            
def found_lastone_trade_day(now: Union[Datetime, str] = Datetime.today()) -> Datetime:
    '''
    找到距离今天最近的最后一个交易日
    '''
    if type(now) == str:
        now = Datetime.strptime(now, DT_STDFMT)

    elif isinstance(now, Datetime):
        pass

    else:
        raise TypeError(type(now))

    counter = 0
    while now.strftime(DT_STDFMT) not in trading_dates:

        now = now - timedelta(days=1)
        counter += 1

        if counter > 20:
            # 不可能这么多天还找不到
            raise RuntimeError

    return now
    
def fetch(lastday: Datetime):
    '''
    拉取最近一天的数据
    '''

    def dump_data(data: pd.DataFrame, code: str, lastday: Datetime) -> None:
        '''存储data到本地格式化路径的csv中去'''
        year  = lastday.strftime(r"%Y")
        month = lastday.strftime(r"%m")
        day   = lastday.strftime(r"%d")
        # 创建例如 2020/5/1 这样的文件路径
        dirpath = os.path.join("./db", year, month, day)

        os.makedirs(dirpath, exist_ok=True)

        # e.g. 000523.SZ-20240301.csv
        filename = code + "-" + lastday.strftime(r"%Y%m%d") + ".csv"

        path = os.path.join(dirpath, filename)
        data.to_csv(path)

        
    # 如果这一天没有被记录
    if lastday.strftime(DT_STDFMT) not in manifested:
        
        for code in tqdm(StocksManager.gen_code_list()):

            nr, sfx = code.split(".")
            secu = sfx.lower() + nr
            
            data = ashare.api.query_data_in_day(
                security = secu,
                day      = lastday
            )
            '''
                                  open  close   high    low   volume
            time                                                    
            2024-03-01 09:30:00  10.59  10.59  10.59  10.59  28476.0
            2024-03-01 09:31:00  10.58  10.56  10.59  10.54  58499.0
            ...                    ...    ...    ...    ...      ...
            2024-03-01 14:59:00  10.49  10.49  10.49  10.49      0.0
            2024-03-01 15:00:00  10.49  10.49  10.49  10.49  23805.0
            '''

            if data.empty:
                logger.warning(f"{code} 也许停牌")
                continue
                # raise RuntimeError(f"empty data in {code}")
                
            # 个别股票会缺少 09:30:00 的数据 用31分的数据填充
            if data.index[0].strftime(r"%H:%M:%S") == "09:31:00":
                line = data.iloc[0]
                filler = data.index[0] - timedelta(minutes=1)
                data.loc[filler] = line
                data = data.sort_index() 
                
            if data.index[0].strftime(r"%H:%M:%S") != "09:30:00" or \
                data.index[-1].strftime(r"%H:%M:%S") != "15:00:00":
                # check: 只存放当天的数据
                logger.debug(code)
                logger.debug(data)
                raise RuntimeError(data.index[0].strftime(r"%H:%M:%S"), data.index[-1].strftime(r"%H:%M:%S"))

            dump_data(data, code, lastday)

    start = lastday.strftime(r"%Y-%m-%d") + " 09:30:00"
    end   = lastday.strftime(r"%Y-%m-%d") + " 15:00:00"

    for idx in config.index_codes:
        # TODO: 整合接口
        data: pd.DataFrame = ak.index_zh_a_hist_min_em(
            symbol     = idx, 
            start_date = start, 
            end_date   = end, 
            period     = '1', 
        )

        '''
                            时间     开盘     收盘     最高     最低    成交量        成交额    最新价
        0    2024-03-04 09:30:00  3026.61  3026.61  3026.61  3026.61   3721393  4.513564e+09  3030.638
        1    2024-03-04 09:31:00  3026.51  3029.73  3029.74  3026.32  10442587  1.284857e+10  3039.135
        2    2024-03-04 09:32:00  3030.15  3027.73  3031.97  3027.73   6237286  8.155432e+09  3033.416
        3    2024-03-04 09:33:00  3027.07  3024.25  3027.55  3024.25   5509647  6.985144e+09  3022.386
        4    2024-03-04 09:34:00  3023.69  3022.84  3023.84  3021.84   5271133  6.400221e+09  3013.396
        ..                   ...      ...      ...      ...      ...       ...           ...       ...
        236  2024-03-04 14:56:00  3039.44  3038.95  3039.44  3038.56   2207744  2.566998e+09  3031.136
        237  2024-03-04 14:57:00  3039.07  3038.54  3039.40  3038.34   2601513  2.947529e+09  3031.296
        238  2024-03-04 14:58:00  3038.74  3038.92  3038.92  3038.73    164766  2.042407e+08  3031.490
        239  2024-03-04 14:59:00  3038.92  3038.92  3038.92  3038.92         0  0.000000e+00  3031.490
        240  2024-03-04 15:00:00  3038.92  3039.31  3039.31  3038.92   4358445  4.930425e+09  3031.334
        '''
        data = data.rename(columns={
            "时间": "time",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
        })[["time", "open", "close", "high", "low", "volume"]]

        data.loc[:, "time"] = pd.to_datetime(data["time"])
        data = data.set_index("time").sort_index(ascending=True)

        if data.index[0].strftime(r"%H:%M:%S") != "09:30:00" or \
            data.index[-1].strftime(r"%H:%M:%S") != "15:00:00":
            # check: 只存放当天的数据
            logger.debug(code)
            logger.debug(data)
            raise RuntimeError(data.index[0].strftime(r"%H:%M:%S"), data.index[-1].strftime(r"%H:%M:%S"))

        data = data[data.index.day == lastday.day]
        
        dump_data(data, idx+".IDX", lastday)

if __name__ == "__main__":

    lastday = found_lastone_trade_day()
    
    if lastday.strftime(DT_STDFMT) not in manifested:
        fetch(lastday)
        manifested = manifested + [lastday.strftime(DT_STDFMT)]
        pd.DataFrame(manifested, columns=["trade_date"]) \
            .to_csv("manifest.csv")

    else:
        logger.warning("today manifested alreadly")

