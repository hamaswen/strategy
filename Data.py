#! Python3
# -*- coding: utf-8 -*-
import tushare as ts
import pandas as pd
import os.path
import logging


stock_dir = 'E:\\gitroot\\runtime\\Output\\A'


def store_stock_data(stock_code, output_dir, skip_existed_file=True):
    """
    Use TuShare lib to get day-level history data from 1990-01-01 to now
    Calculate additional MA5, MA10, MA20, MA60, MA120, MA250 and add to data
    Save data as HDF5
    :param stock_code: code of the stock or etf
    :param output_dir: dir to put the h5 file
    :param skip_existed_file: option to overwrite the existed file (default value is True)
    :return:
    """

    stock_file = os.path.join(output_dir, stock_code + '.h5')

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


def store_all_stock_data(output_dir):
    """
    Store all stock and part of etf
    :return:
    """
    all_stock_code = ts.get_stock_basics().index
    logging.info('Total count: {0}'.format(len(all_stock_code)))
    for stock_code in all_stock_code:
        store_stock_data(stock_code, output_dir)
    for stock_code in ['510050', '510300', '510180', '159902', '159915', '159903', '159902']:
        store_stock_data(stock_code, output_dir)


def get_train_test_data(input_days):
    """
    Data cleaning
    Select data for training and testing
    :param input_days: number of the trading days for input
    :return:
    """
    for file_name in os.listdir(stock_dir):
        stock_file_path = os.path.join(stock_dir, file_name)
        logging.debug('Reading "{0}" ...'.format(stock_file_path))
        data = pd.read_hdf(stock_file_path, key='A' + os.path.splitext(file_name)[0])
        logging.debug('Original shape: {0} Remove columns - "date" and "code"'.format(data.shape))
        del data['date']
        del data['code']
        logging.debug('Shape after columns removed: {0} Drop NaN rows'.format(data.shape))
        data.dropna(inplace=True)
        logging.debug('Final shape: {0}'.format(data.shape))
        logging.info('Translate to X Y data sets')
        for i in range(len(data)-input_days-1):
            print(data[i:i+input_days])
            print(data.iloc[i + input_days + 1]['close'])
            break
        # train data 90%
        # test data 10%
        break
    pass


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.debug('TuShare v' + ts.__version__)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', 100)
    # store_all_stock_data(stock_dir)
    get_train_test_data(20)
    return


if __name__ == '__main__':
    main()
