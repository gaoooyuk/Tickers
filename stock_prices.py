#!/usr/bin/env python

# Stock price history data

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
dataset_name = 'WIKI'
tokens = ['VXDEZygbKnvKfz9A_8KU', 'B6ugfw4xiUXqt7kaK5pD', 'om57y-vVfLPoJF3zTgzk', "d6Hm5HCCPUh4Vw7KasBJ"]

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
			print self.name + ' start retrieving price history of ticker ' + ticker
			try:
				get_price_history_json(ticker, self.token)
			except Exception as e:
				print e

			time.sleep(3)

def get_price_history_json(ticker, token):
	req_url = head + dataset_name + '/' + ticker + '.' + file_format + '?auth_token=' + token
	response = urllib2.urlopen(req_url)  
	data = json.loads(response.read())

	file_name = ticker + '.json'
	with open(file_name, 'w') as outfile:
		json.dump(data, outfile)

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