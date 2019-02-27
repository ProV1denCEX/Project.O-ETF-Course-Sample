import asyncio
import pandas as pd
import yaml
from sqlalchemy import create_engine, exc, orm, MetaData
# from multiprocessing import Pool


config = yaml.load(open("config.yaml"))
s_user = config['database']['user']
s_password = config['database']['password']
s_url = config['database']['host']
s_port = config['database']['port']
s_db = 'Option_US'

DSNs = {
    'sql_server': 'mssql+pymssql://%s:%s@%s:%s/%s' % (s_user,
                                                      s_password,
                                                      s_url,
                                                      s_port,
                                                      s_db)}

dsn = DSNs['sql_server']
eng = create_engine(dsn)
d_list = pd.read_sql_query("select name from sysobjects where xtype='u'", eng)
data_raw = pd.read_csv("G:\\0.Download\\a392882e8ab976b7_csv\\Option_wrds.csv")

# try:
#     eng.connect()
# except exc.OperationalError:
#     con = pymssql.connect(
#         host=s_url,
#         port=int(s_port),
#         user=s_user,
#         password=s_password,
#         database='Master',
#         autocommit=True)
#     with con:
#         cur = con.cursor()
#         temp = "G:\\OneDrive\\1.Code\\1.Github\\Project.O\\0.Data\\" + s_db
#         sql = "Create database %s on(name='%s', filename='%s.mdf', size=5mb, maxsize=1000mb, filegrowth=5mb)" % (
#             s_db, s_db, temp)
#         cur.execute(sql)
#         cur.close()
#     eng = create_engine(dsn)
#     eng.connect()


# symbol = data_raw.symbol.unique()
# eng = create_engine(dsn)
# for i in symbol:
#     if i.find('SPX') >= 0:
#         temp = data_raw[data_raw.symbol == i]
#         s_table = "SP500_Option_" + i
#         try:
#             temp.to_sql(s_table, eng, if_exists="fail", method='multi')
#             print(s_table + "finished!")
#
#         except ValueError:
#             d_old = pd.read_sql_table(s_table, eng)
#             if not temp.reset_index().equals(d_old):
#                 temp.to_sql(s_table, eng, if_exists="replace", method='multi')
#                 print(s_table + "finished!")
#             else:
#                 print(s_table + " No need")


async def write(temp, s_table, eng):
    temp.to_sql(s_table, eng, if_exists="fail", method='multi')
    print(s_table + " finished!")


async def check_write(temp, s_table, eng):
    d_old = pd.read_sql_table(s_table, eng)
    if not temp.reset_index().equals(d_old):
        temp.to_sql(s_table, eng, if_exists="replace", method='multi')
        print(s_table + " finished!")
    else:
        print(s_table + " No need")


# async def go(i_symbol):
#     if i_symbol.find('SPX') >= 0:
#         eng = create_engine(dsn)
#         temp = data_raw[data_raw.symbol == i_symbol]
#         s_table = "SP500_Option_" + i_symbol
#         try:
#             await write(temp, s_table, eng)
#
#         except ValueError:
#             await check_write(temp, s_table, eng)

async def go(i_symbol):
    s_table = "SP500_Option_" + i_symbol
    if s_table not in d_list.name.values and i_symbol.find('SPX') >= 0:
        temp = data_raw[data_raw.symbol == i_symbol]

        try:
            await write(temp, s_table, eng)

        except ValueError:
            await check_write(temp, s_table, eng)


symbol = data_raw.symbol.unique()
task = [go(i_symbol) for i_symbol in symbol]

loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(task))
loop.close()


#
# symbol = data_raw.symbol.unique()
#
#
# def insert(i, temp):
#     eng = create_engine(dsn)
#     # temp = data_raw[data_raw.symbol == i]
#     s_table = "SP500_Option_" + i
#     try:
#         temp.to_sql(s_table, eng, if_exists="fail")
#         print(s_table + " finished!")
#
#     except ValueError:
#         d_old = pd.read_sql_table(s_table, eng)
#         if not temp.equals(d_old):
#             temp.to_sql(s_table, eng, if_exists="replace")
#             print(s_table + " finished!")
#         else:
#             print(s_table + " No need!")
#
#
# parfor = Pool(4)
# for i in symbol:
#     temp = data_raw[data_raw.symbol == i]
#     parfor.apply_async(print, args=temp.symbol)
#
# parfor.close()
# parfor.join()



# data_raw.to_sql("SP500_Option_raw", eng, if_exists="replace")


# session = orm.sessionmaker(bind=eng)
# session = session()
# metadata = MetaData(eng)

