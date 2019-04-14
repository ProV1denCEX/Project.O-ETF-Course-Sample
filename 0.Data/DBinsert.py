import threading
import yaml
from sqlalchemy import create_engine
import pandas as pd
# from multiprocessing import Pool


data_raw = pd.read_csv("G:\\0.Download\\a392882e8ab976b7_csv\\Option_wrds.csv")
seph = threading.Semaphore(4)


class DBinsert(threading.Thread):
    def __init__(self, symbol, eng):
        threading.Thread.__init__(self)
        self.symbol = symbol
        self.eng = eng

    def run(self):
        seph.acquire()
        s_table = "SP500_Option_" + self.symbol
        temp = data_raw[data_raw.symbol == self.symbol]

        # temp.to_sql(s_table, self.eng, if_exists="replace", method='multi')
        # print(s_table + " finished!")

        try:
            temp.to_sql(s_table, self.eng, if_exists="fail", method='multi')
            print(s_table + " finished!")

        except ValueError:
            d_old = pd.read_sql_table(s_table, self.eng)
            if not temp.reset_index().equals(d_old):
                temp.to_sql(s_table, self.eng, if_exists="replace", method='multi')
                print(s_table + " finished!")
            else:
                print(s_table + " No need")
        seph.release()


if __name__ == '__main__':
    config = yaml.load(open("config.yaml"))
    DSNs = {
        'sql_server': 'mssql+pymssql://%s:%s@%s:%s/%s' % (config['database']['user'],
                                                          config['database']['password'],
                                                          config['database']['host'],
                                                          config['database']['port'],
                                                          'Option_US')}
    dsn = DSNs['sql_server']
    eng = create_engine(dsn)

    symbol = data_raw.symbol.unique()

    conn = [DBinsert(i, eng) for i in symbol]
    # pool = Pool(2)
    # for i in conn:
    #     pool.apply_async(func=i.run)
    #
    # pool.close()
    # pool.join()
    [i.start() for i in conn]
    [i.join() for i in conn]
