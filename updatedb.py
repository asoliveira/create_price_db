# -*- coding: utf-8 -*-
"""
Created on Sat Oct 26 09:47:20 2019

@author: Alex S. Oliveira

Description
-----------

Create a sqlite database of prices from the ab excel with dde link.


"""
import numpy as np
import pandas as pd
import pandas
import sys
new_path = '.'
if new_path not in sys.path:
    sys.path.append(new_path)
from in_var.input_variables import *

import os
import unidecode
import sqlite3
from datetime import datetime 



def update_prices(log_file):
    """
    Read the prices quotes of the sheet with a dde link.
    """
    today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    try:
        cotacao = pd.read_excel(sheet_of_live_prices, thousandas='.', decimal=',',
                                sheet_name=sheet_name_of_live_prices)
    except PermissionError as e:
        print('error ======================================')
        with open(log_file, 'a') as log:
            log.write(40*'=')
            log.write(f'Tivemos um erro e não atualizamos os dados em {today}')
            log.write(str(e))
            log.write(40*'=')
        return
        
    cotacao.dropna(axis=0, how='all', inplace=True)
    
    cotacao.columns = cotacao.loc[cotacao.index[0]]
    cotacao.drop(cotacao.index[0], inplace=True)
    cotacao.set_index(column_tick_name, inplace=True)
    cotacao.dropna(axis=1, how='all', inplace=True)
    
#    Transform all columns tho numeric types or drop, if it is not possible
    for c in cotacao.columns:  
        try:
            cotacao[c] = pd.to_numeric(cotacao[c], errors='raise', downcast='float')
        except ValueError:
            print(f'The column{c} raised a error when converting to numeric.\
                  As a workaround it was droped')
#            cotacao.drop(labels=c,axis=1, inplace=True)

      
    
    
    with sqlite3.connect(live_prices_db) as con:
        c = con.cursor()

        for tick, row in cotacao.iterrows():
            #creating the table
            print('Updating tick {}'.format(tick))
            cols='("date", '
            for col_name in row.index:
                create_table = 'CREATE TABLE IF NOT EXISTS {}'.format(tick)
                col_name = unidecode.unidecode(col_name).lower().\
                replace(' ','_').replace('.','')
                cols += '"{}", '.format(col_name)
            cols = cols[:-2] + ')'
            create_table += cols
            
            #populating the table
            insert_table = f'INSERT INTO {tick} {cols} VALUES'
            values = '("{}", '.format(today)
            for value in row:
                if (type(value) == float or type(value) == int) and not np.isnan(value):
                    values += '{0:.2f}, '.format(value)
                else:
                    values += '"{}", '.format(value)
            values = values[:-2] + ')'
            insert_table += values
            
            
            c.execute(create_table)
            c.execute(insert_table)
        
            con.commit()
    con.close()

def make_daytrade_db(asset, live_prices_db, daytrade_db, freq, volume,
                     quantity, last_price, col_names, log_file):
    """
    Create a database to daytrade operation with OHLC prices with a frequency 
    freq.
    """
    col_names = [unidecode.unidecode(c).lower() for c in col_names]
    today = datetime.today().strftime('%Y-%m-%d')

#    Reading and database of prices
    sql = f'SELECT * FROM {asset} WHERE date(date) == date("{today}")'
    
    
    try:
        with sqlite3.connect(live_prices_db) as con:
            df = pd.read_sql(sql, con, parse_dates=['date'], index_col='date')
    except pandas.io.sql.DatabaseError as e:
        if not os.path.exists(log_file):
            open_mode = 'w'
        else:
            open_mode = 'a'
        with open(log_file, open_mode) as f:
            f.write(str(e))
        print(str(e))
    
    df = df.drop_duplicates()
    df =  df.dropna()
    if len(df) <= 5:
        print(f'Os dados lidos da tabela {live_prices_db} são poucos para {asset}')
        return
    else:
        pass
    col_to_use = [volume, quantity, last_price]
    for c in col_to_use:
        df[c.lower()] = pd.to_numeric(df[c.lower()])
    qtd_vol = df[[volume.lower(), quantity.lower()]].resample(freq).sum()
    dfcandle = df[last_price.lower()].resample(freq).ohlc()

    
    new_cols_names = col_names
    cols_names = dfcandle.columns.tolist()
    cols_names[:4] = new_cols_names
    dfcandle.columns = cols_names
    
    
#    dfcandle['Hora'] = dfcandle.index.hour
    
    dfcandle = pd.concat((dfcandle, qtd_vol), axis=1)
    dfcandle = dfcandle.fillna(method='ffill')
    
    with sqlite3.connect(daytrade_db) as con_out:
        dfcandle.to_sql(asset, con_out, if_exists='replace')
    
    print(f'Writed in {daytrade_db} the data of {asset}')


def tables(con):
    """
    List of tables that are registered in database.

    Parameters
    ----------
    con : sqlite connection
        The connection to the database live price database.


    Returns
    ------
    dataframe of table : pandas dataframe.
        The dataframe of tables registered in conn.

    """
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'",
                         con)

    return tables.drop_duplicates(subset=['name']).iloc[:, 0].tolist()
 
def main():
    log_file = 'log.txt'
    count = 1
    while True:
        print('starting update the database'.format(live_prices_db))
        update_prices(log_file)
        print('Updated {}'.format(live_prices_db))
        if np.mod(count, 3) == 0: 
            number = count/3
            print(f'Updating the {daytrade_db}. Update number {number}')
            with sqlite3.connect(live_prices_db) as con:
                assets = tables(con)
            for asset in assets:
                print(f'starting update the data in {daytrade_db} of {asset}')
                make_daytrade_db(asset, live_prices_db, daytrade_db, freq, 
                        volume, quantity, last_price,col_names, log_file)               
#        time.sleep(time_interval)
        count += 1
        
        
            

if __name__ == '__main__':
    main()        