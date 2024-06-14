import requests
import time

headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
	AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
	
def request_dom(url,headers=headers,cookies=None,max_try=3,params=None,proxy=None,timeout=None):
	'''请求url，返回dom文件'''
	try:
		response = requests.get(url,headers=headers,cookies=cookies,proxies=proxy,params=params,timeout=timeout)
		if response.status_code == 200:
			return response
		else:
			print('-'*30,'request doc error:',response.status_code)
			try:
				print(response.json())
			except Exception:
				pass
			return response
	except (requests.exceptions.ConnectionError,requests.exceptions.Timeout):
		if max_try>0:
			print(f'wait and try again...\nmax_try={max_try-1}')
			time.sleep(10)
			return request_dom(url=url,headers=headers,cookies=cookies,max_try=max_try-1)
		print('-'*30,'request doc ConnectionError')
		return None







