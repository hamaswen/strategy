#! Python3
# -*- coding: utf-8 -*-
import tushare as ts
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import os.path
import logging
import h5py


# stock_dir = 'E:\\gitroot\\runtime\\Output\\A'
stock_dir = 'E:\\gitroot\\runtime\\Output\\Demo'


def map_to_y(increase_percentage):
    """
    Give a class for given rate
    :param increase_percentage:
    :return: array with 0 and 1 which 1 represents the range of increase rate
    """
    y = np.zeros((14, 1), np.int8)
    if increase_percentage >= 10:
        y[0] = 1
    elif 7 <= increase_percentage < 10:
        y[1] = 1
    elif 4 <= increase_percentage < 7:
        y[2] = 1
    elif 2 <= increase_percentage < 4:
        y[3] = 1
    elif 1 <= increase_percentage < 2:
        y[4] = 1
    elif 0.5 <= increase_percentage < 1:
        y[5] = 1
    elif 0 <= increase_percentage < 0.5:
        y[6] = 1
    elif -0.5 < increase_percentage < 0:
        y[7] = 1
    elif -1 < increase_percentage <= -0.5:
        y[8] = 1
    elif -2 < increase_percentage <= -1:
        y[9] = 1
    elif -4 < increase_percentage <= -2:
        y[10] = 1
    elif -7 < increase_percentage <= -4:
        y[11] = 1
    elif -10 < increase_percentage <= -7:
        y[12] = 1
    else:
        y[13] = 1
    return y


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
    base_data['ma5'] = round(base_data['close'].rolling(5).mean(), 3)
    base_data['ma10'] = round(base_data['close'].rolling(10).mean(), 3)
    base_data['ma20'] = round(base_data['close'].rolling(20).mean(), 3)
    base_data['ma60'] = round(base_data['close'].rolling(60).mean(), 3)
    base_data['ma120'] = round(base_data['close'].rolling(120).mean(), 3)
    base_data['ma250'] = round(base_data['close'].rolling(250).mean(), 3)
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
    data_x_list = []
    data_y_list = []
    for file_name in os.listdir(stock_dir):
        stock_file_path = os.path.join(stock_dir, file_name)
        logging.info('Reading "{0}" ...'.format(stock_file_path))
        data = pd.read_hdf(stock_file_path, key='A' + os.path.splitext(file_name)[0])
        logging.debug('Original shape: {0} Remove columns - "date" and "code"'.format(data.shape))
        del data['date']
        del data['code']
        logging.debug('Shape after columns removed: {0} Drop NaN rows'.format(data.shape))
        data.dropna(inplace=True)
        logging.debug('Final shape: {0}'.format(data.shape))
        logging.info('Translate to X Y data sets')
        for i in range(len(data)-input_days-1):
            data_x = data[i:i+input_days]
            current_close = data.iloc[i + input_days - 1]['close']
            next_close = data.iloc[i + input_days]['close']
            increase_percentage = 100 * (next_close - current_close)/current_close
            data_y = increase_percentage
            data_x_list.append(data_x.values)
            data_y_list.append(data_y)
    data_X = np.stack(data_x_list)
    data_Y = np.stack(data_y_list)
    logging.debug(data_X.shape)
    logging.debug(data_Y.shape)
    # Split train data (85%) and test data (15%)
    # memory error
    X_train, X_test, Y_train, Y_test = train_test_split(data_X, data_Y, test_size=0.15)
    Y_train = Y_train.reshape(1, Y_train.shape[0])
    Y_test = Y_test.reshape(1, Y_test.shape[0])
    logging.debug(X_train.shape)
    logging.debug(X_test.shape)
    logging.debug(Y_train.shape)
    logging.debug(Y_test.shape)
    with h5py.File('datasets/train_stock.h5', 'w') as train_data_file:
        train_data_file.create_dataset('train_set_x', data=X_train)
        train_data_file.create_dataset('train_set_y', data=Y_train)
    with h5py.File('datasets/test_stock.h5', 'w') as test_data_file:
        test_data_file.create_dataset('test_set_x', data=X_test)
        test_data_file.create_dataset('test_set_y', data=Y_test)
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
