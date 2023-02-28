import numpy as np
import pandas as pd
import tushare as ts

token = '88b107608484d9a833913c82b715e0a5076c6d7610ba61dd8204c47f'
ts.set_token(token)
pro = ts.pro_api('88b107608484d9a833913c82b715e0a5076c6d7610ba61dd8204c47f')
import warnings

warnings.filterwarnings("ignore")

# 全局变量
start_time = '20120101'
end_time = '20220831'
raw_data_path = r'C:/study/nudt_cs/课题/new'
# 调参周期 训练周期 预测周期
raw_data_path2 = r'D:/study/nudt_cs/课题/华泰/t_data/data2'
raw_data_path3 = r'D:/study/nudt_cs/课题/华泰/t_data/data3'


# reduce memory 减少数据内存
def reduce_mem_usage(df, verbose=True):
    start_mem = df.memory_usage().sum() / 1024 ** 2
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']

    for col in df.columns:
        col_type = df[col].dtypes
        if col_type in numerics:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)

    end_mem = df.memory_usage().sum() / 1024 ** 2
    print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
    print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))
    return df


# 查询当前所有正常上市交易的股票列表
data = pro.stock_basic(exchange='', list_status='L',
                       fields='ts_code,name,area,industry,market, list_status, list_date, delist_date')
data.isnull().sum()  # 各项缺失值总数
data.to_csv('./data/stock_basic/stock_basic.csv', index=False)
df = pro.stock_company(exchange='', fields='ts_code,reg_capital,city,employees')

# 前复权
n = 0
f = ts.pro_bar
adj = 'qfq'
for i in data.ts_code.values:
    if n == 0:
        df1 = f(ts_code=i, start_date=start_time, adj=adj, end_date=end_time)
        n += 1
    else:
        df2 = f(ts_code=i, start_date=start_time, adj=adj, end_date=end_time)
        df1 = df1.append(df2)
    break
df1 = reduce_mem_usage(df1, verbose=True)
df1.to_csv(raw_data_path + '/daily/daily_{}.csv'.format(adj), index=False)

# 后复权
import time

ts_list = data.ts_code.values
ts_len = data.ts_code.values.shape[0]
adj = 'hfq'
not_get_list = []
f = ts.pro_bar
n = 0
for i in range(ts_len):
    ts_code = ts_list[i]
    try:
        if n == 0:
            df1 = f(ts_code=ts_code, start_date=start_time, adj=adj, end_date=end_time)
            n += 1
        else:
            df2 = f(ts_code=ts_code, start_date=start_time, adj=adj, end_date=end_time)
            df1 = df1.append(df2)
            break
    except:
        time.sleep(1)
        print(i)
        not_get_list.append(i)
df1 = reduce_mem_usage(df1)
df1.to_csv(raw_data_path + '/daily/daily_{}.csv'.format(adj), index=False)
filepath = raw_data_path + '/daily/daily_{}.csv'.format(adj)
df1 = pd.read_csv(filepath)
df1 = reduce_mem_usage(df1)
df1.to_csv(filepath, index=False)

# 每日指标
fields = 'ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio,' \
         ' pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv'
n = 0
# list(data.ts_code.values).index(i)
a = list(data.ts_code.values)[4129:]
for i in a:
    if n == 0:
        df1 = pro.daily_basic(ts_code=i, start_date=start_time, end_date=end_time, fields=fields)
        n += 1
    else:
        df2 = pro.daily_basic(ts_code=i, start_date=start_time, end_date=end_time, fields=fields)
        df1 = df1.append(df2)
        # break
