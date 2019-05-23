#!/usr/bin/python3
import json
import urllib3
import getopt
import sys
import readline
from prettytable import PrettyTable


def main(argv):
	host, port, user, passwd = None, None, None, None
	try:
		opts, args = getopt.getopt(argv, "hH:P:u:p:", [])
	except getopt.GetoptError:
		print('presto.py -H <host> -P <port> -u <user> -p <password>')
		sys.exit(2)
	if len(opts) < 1:
		print('presto.py -H <host> -P <port> -u <user> -p <password>')
		sys.exit()
	
	for opt, arg in opts:
		if opt == '-h':
			print('presto.py -H <host> -P <port> -u <user> -p <password>')
			sys.exit()
		elif opt == '-H':
			host = arg
		elif opt == '-P':
			port = arg
		elif opt == "-u":
			user = arg
		elif opt == "-p":
			passwd = arg
	
	if host is None or port is None or user is None or passwd is None:
		print('presto.py -H <host> -P <port> -u <user> -p <password>')
		sys.exit()
	
	while True:
		user_input = input('>>>presto: ')
		
		if len(user_input) < 1:
			continue
		
		if user_input.lower() == 'exit':
			print('bye-bye')
			break
		fetch(host, port, user, passwd, user_input)


def fetch(host, port, user, passwd, sql):
	urlopen_kw = {
		'body': sql
	}
	url = 'http://'+host+':'+port+'/v1/statement'
	st = urllib3.PoolManager().request('POST', url, None, {
		'X-Presto-User': user,
		'X-Presto-Password': passwd,
		'X-Presto-Catalog': 'hive',
	}, **urlopen_kw)

	st_data = json.loads(st.data, encoding='utf-8')
	if 'error' in st_data.keys():
		print("error\n")
		print(st_data['error']['message'])
		print("\n")
		return

	if 'nextUri' not in st_data.keys():
		print("fetch end\n")
		print("nextUri not exists\n")
		return
		
	data_des = urllib3.PoolManager().request('GET', st_data['nextUri'])
	ori_data = json.loads(data_des.data, encoding='utf-8')
	if 'error' in ori_data.keys():
		print("error\n")
		print(ori_data['error']['message'])
		print("\n")
		return

	columns = []
	for oneColumn in ori_data['columns']:
		columns.append(oneColumn['name'])

	if 'data' not in ori_data.keys():
		print("data empty\n")
		return

	x = PrettyTable(columns)

	for oneData in ori_data['data']:
		x.add_row(oneData)

	print(x)


if __name__ == '__main__':
	main(sys.argv[1:])
