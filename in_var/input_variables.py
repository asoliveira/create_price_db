# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 02:36:30 2019

@author: alex_
"""

#excel path of quoted prices
sheet_of_live_prices = 'in_var/cotacao_test.xlsm'
sheet_name_of_live_prices = 'Cotacao'

#Column in the sheet_of_live_prices with asset/tick names
column_tick_name = 'Ativo'

#Database where historical live quotes readed from the sheet_file.
live_prices_db = 'C://Users/alex_/OneDrive/Base Dados/prices.db'
#live_prices_db = 'prices.db'

#Time in seconds to update the live_price_db
time_interval = 10

#Path of daytrade database
daytrade_db = 'C://Users/alex_/OneDrive/Base Dados/to_daytrade.db'

#Frequency of daytradade db.
freq = '5min'

#Please verify the name of the columns below in you dde sheet. These example 
#the name is in portugues.
volume = 'Volume'
quantity = 'Quantidade'
last_price = 'ultimo'
#col_names = ['Open', 'High', 'Low', 'Close']
col_names = ['Abertura', 'Máximo', 'Mínimo', 'Fechamento']