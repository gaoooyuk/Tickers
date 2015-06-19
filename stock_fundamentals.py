#!/usr/bin/env python

# Stock fundamentals data

import urllib2
import json
import datetime
import threading
import numpy as np
from numpy import loadtxt
from pprint import pprint
import time

__version__ = '1.0'
__author__ = 'chuckgao.cg@gmail.com'

head = 'https://www.quandl.com/api/v1/datasets/'
dataset_name = 'RAYMOND'
tokens = ['VXDEZygbKnvKfz9A_8KU', 'B6ugfw4xiUXqt7kaK5pD', 'om57y-vVfLPoJF3zTgzk', "d6Hm5HCCPUh4Vw7KasBJ"]

# load available RAYMOND_CODES
available_tifs = loadtxt("RAYMOND_CODES.txt", dtype='str', delimiter=',', unpack=False)

frequency = 'A'	# A = Annually, Q = Quarterly
file_format = 'json'

class myThread(threading.Thread):
	def __init__(self, threadID, name, token, tickers):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.name = name
		self.token = token
		self.tickers = tickers

	def run(self):
		print "Starting " + self.name + ' for ' + self.token

		for ticker in self.tickers:
			print self.name + ' start retrieving fundamentals of ticker ' + ticker
			try:
				generate_fundamentals_json(ticker, self.token)
			except Exception as e:
				print e

			time.sleep(3)

def get_indicator_data(ticker, indicator, frequency, file_format, token):
	tif = ticker + '_' + indicator + '_' + frequency
	data = {}
	if tif in available_tifs:
		req_url = head + dataset_name + '/' + tif + '.' + file_format + '?auth_token=' + token
		print 'Retrieve indicator ' + indicator + ' of ticker ' + ticker

		response = urllib2.urlopen(req_url)  
		data = json.loads(response.read())

		# pprint(data)

	return data

def get_fundamentals_by_indicators(ticker, indicators, token):
	obj = {}

	for indicator in indicators:
		ret = get_indicator_data(ticker, indicator, frequency, file_format, token)
		val_list = []

		if 'data' in ret.keys():
			for i in ret['data']:
				val_list.append(i[1])

			obj[indicator.lower()] = val_list

	return obj

def generate_fundamentals_json(ticker, token):
	result_json_obj = {}
	result_json_obj['symbol'] = ticker

	# 1) Income Statement data
	# eps, eps_changes, diluted_eps, net_income, depreciation_and_amortization, 
	# capital_expenditures, shareholder_returns, shareholder_returns_changes

	income_statement_indicators = ['NET_INCOME', 'DEPRECIATION_AMORTIZATION', 'CAPITAL_EXPENDITURES']
	is_obj = get_fundamentals_by_indicators(ticker, income_statement_indicators, token)
	result_json_obj['income_statement'] = is_obj

	# 2) Balance Sheet data
	# total_current_liabilities, total_current_assets, current_ratio, speed_ratio,
	# total_non_current_liabilities, net_income, years_to_repay, operating_profit,
	# interest_expense, interest_expense_operating_profit_ratio, 
	# has_preferred_stock, preferred_stock_percentage

	balance_sheet_indicators = ['TOTAL_CURRENT_LIABILITIES', 'TOTAL_CURRENT_ASSETS']
	bs_obj = get_fundamentals_by_indicators(ticker, balance_sheet_indicators, token)
	result_json_obj['balance_sheet'] = bs_obj

	# 3) Shareholders Equity
	# total_liabilities, total_equity, return_on_equity, debt_to_equity_ratio
	shareholders_equity_indicators = ['TOTAL_LIABILITIES', 'TOTAL_EQUITY']
	se_obj = get_fundamentals_by_indicators(ticker, shareholders_equity_indicators, token)

	# calculate ROE and Debt/Equity Ratio
	l_net_income = np.array(is_obj[str('NET_INCOME').lower()], dtype=np.float)
	l_total_liabilities = np.array(se_obj[str('TOTAL_LIABILITIES').lower()], dtype=np.float)
	l_total_equity = np.array(se_obj[str('TOTAL_EQUITY').lower()], dtype=np.float)

	len_n_i = len(l_net_income)
	len_t_l = len(l_total_liabilities)
	len_t_e = len(l_total_equity)

	l_n_i = l_net_income
	l_t_e = l_total_equity

	if len_n_i != len_t_e:
		if len_n_i > len_t_e:
			l_n_i = l_net_income[:-(len_n_i-len_t_e)]
		else:
			l_t_e = l_total_equity[:-(len_t_e-len_n_i)]

	se_obj['return_on_equity'] = (l_n_i / l_t_e).tolist()

	l_t_l = l_total_liabilities
	l_t_e = l_total_equity

	if len_t_l != len_t_e:
		if len_t_l > len_t_e:
			l_t_l = l_total_liabilities[:-(len_t_l-len_t_e)]
		else:
			l_t_e = l_total_equity[:-(len_t_e-len_t_l)]

	se_obj['debt_to_equity_ratio'] = (l_t_l / l_t_e).tolist()

	# print se_obj['return_on_equity']
	# print se_obj['debt_to_equity_ratio']


	result_json_obj['shareholders_equity'] = se_obj	



	# 4) Profits
	# operating_profit, ebit, gross_profit, ebit_margin
	profits_indicators = ['OPERATING_INCOME', 'GROSS_PROFIT']
	p_obj = {}

	for indicator in profits_indicators:
		ret = get_indicator_data(ticker, indicator, frequency, file_format, token)

		val_list = []

		if 'data' in ret.keys():
			for i in ret['data']:
				val_list.append(i[1])

			if ('operating_profit' == indicator.lower()):
				p_obj['operating_profit'] = val_list
			else:
				p_obj[indicator.lower()] = val_list

	result_json_obj['profits'] = p_obj





	# 4) Capital Investment
	# total_assets, roa, roic, raroc
	capital_investment_indicators = ['TOTAL_ASSETS']
	ci_obj = get_fundamentals_by_indicators(ticker, capital_investment_indicators, token)
	result_json_obj['capital_investment'] = ci_obj




	# 5) Intangible Assets
	# accounts_receivable_trade_net
	intangible_assets_indicators = ['ACCOUNTS_RECEIVABLE_TRADE_NET']
	ia_obj = get_fundamentals_by_indicators(ticker, intangible_assets_indicators, token)
	result_json_obj['intangible_assets'] = ia_obj


	# 6) Inventory
	# total_inventory
	inventory_indicators = ['TOTAL_INVENTORY']
	i_obj = get_fundamentals_by_indicators(ticker, inventory_indicators, token)
	result_json_obj['inventory'] = i_obj



	# 7) Stock Repurchase
	# total_common_shares_outstanding 
	stock_repurchase_indicators = ['TOTAL_COMMON_SHARES_OUTSTANDING', 'TREASURY_STOCK_COMMON']
	sr_obj = get_fundamentals_by_indicators(ticker, stock_repurchase_indicators, token)
	result_json_obj['stock_repurchase'] = sr_obj



	result_json_obj['releaseDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

	file_name = ticker + '.json'
	with open(file_name, 'w') as outfile:
	    json.dump(result_json_obj, outfile)

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

        
start_time = time.time()

threads = []
tickers = loadtxt("tickers.txt", dtype='str', delimiter=',', unpack=False)

for index, t in enumerate(tokens):
	thread_idx = index + 1
	sub_tickers = list( chunks(tickers, len(tickers) / len(tokens)) )[index]
	thread = myThread(thread_idx, "Thread-" + str(thread_idx), t, sub_tickers)
	thread.start()

	threads.append(thread)

for t in threads:
    t.join()

elapsed_time = time.time() - start_time

print "Finished, elapsed time: " + str(round(elapsed_time, 3)) + " seconds"