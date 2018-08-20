#! Python3
# -*- coding: utf-8 -*-
import tushare as ts
import pandas as pd
import os.path
import logging


def store_stock_data(stock_code, skip_existed_file=True):
    stock_dir = 'E:\\gitroot\\runtime\\Output\\A'
    stock_file = os.path.join(stock_dir, stock_code + '.h5')

    # check file exists
    if skip_existed_file and os.path.exists(stock_file):
        return
    # get base data
    base_data = ts.get_k_data(stock_code, start='1990-01-01', ktype='D')
    if base_data.size == 0:
        logging.info('No data found of {0}'.format(stock_code))
        return
    # add indicators
    base_data['ma5'] = base_data['close'].rolling(5).mean()
    base_data['ma10'] = base_data['close'].rolling(10).mean()
    base_data['ma20'] = base_data['close'].rolling(20).mean()
    base_data['ma60'] = base_data['close'].rolling(60).mean()
    base_data['ma120'] = base_data['close'].rolling(120).mean()
    base_data['ma250'] = base_data['close'].rolling(250).mean()
    logging.debug(base_data.shape)

    # save as hdf5
    key = 'A' + stock_code
    base_data.to_hdf(stock_file, key, mode='w', format='table')
    logging.info('Saved as ' + stock_file)

    '''
    if os.path.isfile(stock_file):
        current_data = pd.read_hdf(stock_file, key)
        logging.debug(current_data.shape)
    '''


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.debug('TuShare v' + ts.__version__)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', 100)

    all_stock_code = ts.get_stock_basics().index
    logging.info('Total count: {0}'.format(len(all_stock_code)))
    for stock_code in all_stock_code:
        store_stock_data(stock_code)
    for stock_code in ['510050', '510300', '510180', '159902', '159915', '159903', '159902']:
        store_stock_data(stock_code)
    return


if __name__ == '__main__':
    main()
