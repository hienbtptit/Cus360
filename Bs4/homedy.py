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
from CustomLibs import Csv
baseUrl = "https://homedy.com"
url = "https://homedy.com/ban-nha-dat-tp-ho-chi-minh"
Path(os.getcwd() + "/"+datetime.datetime.now().strftime('%Y%m%d')).mkdir(parents=True, exist_ok=True)
PATH_FILE_LOG = os.getcwd() + "/"+datetime.datetime.now().strftime('%Y%m%d')

def writeFieldNameToFile(file_path):
    field_name = []
    field_name.append({'prid': 'prid', 'title': 'title', 'des': 'des', 'phone': 'phone', 'time': 'time'})
    df = pandas.DataFrame(field_name)
    df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaT")

def crawlDataFirstTime(start, end):
    Path("/tmp/leveldb/homedy").mkdir(parents=True, exist_ok=True)
    file_path = PATH_FILE_LOG + "/" + "homedy.csv"
    final = 200
    if end >= final:  end = final + 1
    for i in range(start, end):
        l = []
        try:
            r = requests.get(url + '/p' + str(i))
        except Exception as err:
            print("Page Url error: " + url + '/p' + str(i))
            Csv.write_log(PATH_FILE_LOG, "homedy-"+datetime.datetime.now().strftime('%Y%m%d'),
                          'homedy -' + str(err) + '-' + url + '/p' + str(i))
            continue
        soup = BeautifulSoup(r.content, 'html5lib')
        links = soup.find_all('div',class_='product-item-top')

        for link in links:
            d = {}
            href = link.findChild('a')['href']
            try:
                r = requests.get(baseUrl + href)
            except Exception as err:
                print("Post Url Error: ")
                print(baseUrl + href)
                Csv.write_log(PATH_FILE_LOG, "homedy-"+datetime.datetime.now().strftime('%Y%m%d'),
                              'homedy -' + str(err) + '-' + baseUrl + href)
                continue
            test_leveldb.insert_link(baseUrl + href, start, '/tmp/leveldb/homedy/')
            soup = BeautifulSoup(r.content, 'html5lib')
            try:
                d['prid'] = soup.find('span', class_='code').text
                d['title'] = soup.find('div', class_='product-detail-top-left').findChild('h1').text
                d['des'] = soup.find('div', id='readmore').text
                d['phone'] = soup.find('div', class_='mobile-number-des mobile-counter')['data-mobile']
                d['time'] = datetime.datetime.now()
            except Exception as err:
                print(str(i) + ": " + href)
                print("Get atribute value error")
                Csv.write_log(PATH_FILE_LOG, "homedy-"+datetime.datetime.now().strftime('%Y%m%d'),
                              'homedy -' + str(err) + '-' + baseUrl + href)
                continue
            d = Csv.processData(d)
            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaT")
def crawlBySchedule():
    stop = 0 # stop while loop when stop = 1
    page = 0
    file_path = PATH_FILE_LOG + "/" + "homedy-" + datetime.datetime.now().strftime('%Y%m%d') + ".csv"
    writeFieldNameToFile(file_path)
    Path("/tmp/leveldb/homedy/").mkdir(parents=True, exist_ok=True)
    iterator = 0
    while stop == 0:
        page = page + 1
        l = []
        try:
            r = requests.get(url + '/p' + str(page))
            soup = BeautifulSoup(r.content, 'html5lib')
        except Exception as err:
            print("Page Url Error: ")
            print("Page Url error: " + url + '/p' + str(page))
            Csv.write_log(PATH_FILE_LOG, "homedy-"+datetime.datetime.now().strftime('%Y%m%d'),
                          'homedy -' + str(err) + '-' + url + '/p' + str(page))
            continue
        list_link = soup.find_all('div',class_='product-item-top')
        for link in list_link:
            d = {}
            href = link.findChild('a')['href']
            try:
                request = requests.get(baseUrl + href)
            except Exception as err:
                print("URL error: " + baseUrl + href)
                Csv.write_log(PATH_FILE_LOG, "homedy-" + datetime.datetime.now().strftime('%Y%m%d'),
                              'homedy -' + str(err) + '-' + baseUrl + href)
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
                d['prid'] = soup.find('span', class_='code').text
                d['title'] = soup.find('div', class_='product-detail-top-left').findChild('h1').text
                d['des'] = soup.find('div', id='readmore').text
                d['phone'] = soup.find('div', class_='mobile-number-des mobile-counter')['data-mobile']
                d['time'] = datetime.datetime.now()
            except:
                print(str(page) + ": " + href)
                print("Get atribute value error")
                Csv.write_log(PATH_FILE_LOG, "homedy-" + datetime.datetime.now().strftime('%Y%m%d'),
                              'homedy -' + str(err) + '-' + baseUrl + href)
                continue
            d = Csv.processData(d)
            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaT")



keys = sys.argv[1::2]
values = sys.argv[2::2]
args = {k: v for k, v in zip(keys, values)}

first_time = args.get('--first-time')
if first_time == '1':
    writeFieldNameToFile(PATH_FILE_LOG + "/"+"homedy.csv")
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
