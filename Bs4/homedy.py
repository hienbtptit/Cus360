import multiprocessing
from multiprocessing import Process
import requests
import csv
import os
import pandas
import re
import datetime
import sys
from bs4 import BeautifulSoup
from pathlib import Path
sys.path.append('../')
from CustomLibs import single_thread_leveldb
from CustomLibs import test_leveldb
baseUrl = "https://homedy.com"
url = "https://homedy.com/ban-nha-dat-tp-ho-chi-minh"
def writeFieldNameToFile(file):
    field_name = []
    field_name.append({'prid': 'prid', 'title': 'title', 'des': 'des', 'phone': 'phone'})
    df = pandas.DataFrame(field_name)
    df.to_csv(file, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)
def crawlDataFirstTime(start, end):
    Path("/tmp/leveldb/homedy").mkdir(parents=True, exist_ok=True)
    file_path = os.getcwd()+"/homedy/"+"homedy.csv"
    final = 200
    if end >= final:  end = final + 1
    for i in range(start, end):
        print('start')
        l = []
        try:
            r = requests.get(url + '/p' + str(i))
        except:
            print("URL page error: " + url + '/page-' + str(i))
            continue
        soup = BeautifulSoup(r.content, 'html5lib')
        links = soup.find_all('div',class_='product-item-top')

        for link in links:
            d = {}
            href = link.findChild('a')['href']
            try:
                r = requests.get(baseUrl + href)
            except:
                print("URL post error: " + url + '/page-' + str(i))
                continue
            test_leveldb.insert_link(baseUrl + href, start, '/tmp/leveldb/homedy/')
            soup = BeautifulSoup(r.content, 'html5lib')
            try:
                d['prid'] = soup.find('span', class_='code').text.strip()
                d['title'] = soup.find('div', class_='product-detail-top-left').findChild('h1').text.strip()
                d['des'] = soup.find('div', id='readmore').text.strip()
                d['phone'] = soup.find('div', class_='mobile-number-des mobile-counter')['data-mobile'].strip()
            except:
                print(str(i) + ": " + href)
                print("Get atribute value error")
                continue
            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)

def crawlBySchedule():
    stop = 0 # stop while loop when stop = 1
    page = 1
    now = re.split("\s",str(datetime.datetime.now()))[0]
    now = re.split("-",now)
    file_path = os.getcwd() + "/homedy/" + "homedy-" + now[0] + now[1] + now[2] + ".csv"
    writeFieldNameToFile(file_path)
    iterator = 0
    while stop == 0:
        l = []
        try:
            r = requests.get(url + '/p' + str(page))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Page Url Error: ")
            print(url + '?page=' + str(page))
            continue
        list_link = soup.find_all('div',class_='product-item-top')
        for link in list_link:
            d = {}
            href = link.findChild('a')['href']
            try:
                request = requests.get(baseUrl + href)
            except:
                print("URL error: " + baseUrl + href)
                continue
            soup = BeautifulSoup(request.content, 'html5lib')
            if single_thread_leveldb.check_exist(baseUrl + href, '/tmp/leveldb/homedy/') == 1:
                if iterator == 3 :
                    stop = 1
                    break
                iterator = iterator+1
            else:
                iterator = 0
                single_thread_leveldb.insert_link(baseUrl + href, '/tmp/leveldb/homedy/')
            try:
                d['prid'] = soup.find('span', class_='code').text.strip()
                d['title'] = soup.find('div', class_='product-detail-top-left').findChild('h1').text.strip()
                d['des'] = soup.find('div', id='readmore').text.strip()
                d['phone'] = soup.find('div', class_='mobile-number-des mobile-counter')['data-mobile'].strip()
            except:
                print(str(page) + ": " + href)
                print("Get atribute value error")
                continue
            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)
        page = page + 1


keys = sys.argv[1::2]
values = sys.argv[2::2]
args = {k: v for k, v in zip(keys, values)}
Path(os.getcwd() + "/homedy").mkdir(parents=True, exist_ok=True)
first_time = args.get('--first-time')
if first_time == '1':
    writeFieldNameToFile(os.getcwd()+"/homedy/"+"homedy.csv")
    print("crawlDataFirstTime")
    final = 200
    numProcess = 2  # number process
    ### Multiprocessing with Process
    processes = [Process(target=crawlDataFirstTime, args=(i, i + int(final / numProcess))) for i in
                 range(1, final, int(final / numProcess))]  # init numProcess process
    # Run processes
    for p in processes: p.start()
    # Exit the completed processes
    for p in processes: p.join()
else:
    print("crawlBySchedule")
    crawlBySchedule()
