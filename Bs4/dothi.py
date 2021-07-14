from multiprocessing import Process
import multiprocessing
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
BaseUrl='https://dothi.net'
Url='https://dothi.net/nha-dat-ban-tp-hcm'
def writeFieldNameToFile(file):
    field_name = []
    field_name.append({'prid': 'prid', 'title': 'title', 'des': 'des', 'phone': 'phone', 'time': 'time'})
    df = pandas.DataFrame(field_name)
    df.to_csv(file, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)
def getFinalPage():
    r = requests.get('https://dothi.net/nha-dat-ban-tp-hcm/p50000.htm')
    soup = BeautifulSoup(r.content, 'lxml')
    page = soup.find('div', class_ = 'pager_controls')
    final = page.findChildren('a')[-1]['title'].split('P')[-1]
    return int(final)
def crawlDataFirstTime(start, end, final):
    Path("/tmp/leveldb/dothi").mkdir(parents=True, exist_ok=True)
    file_path = os.getcwd() + "/dothi/" + "dothi.csv"
    if end >= final: end = final+1
    for i in range(start, end):
        r = requests.get(Url+'/p'+str(i)+'.htm')
        soup = BeautifulSoup(r.content, 'lxml')
        list_link = soup.find_all('a', class_='vip2')
        l = []
        for link in list_link:
            r = requests.get(BaseUrl+link['href'])
            test_leveldb.insert_link(link, start, '/tmp/leveldb/dothi/')
            soup = BeautifulSoup(r.content, 'lxml')
            d = {}
            d['prid'] = soup.find('td', text='Mã số').findNextSibling().text.strip()
            d['title'] = soup.find('div', class_='product-detail').findChild('h1').text.strip()
            d['des'] = soup.find('div', class_='pd-desc-content').text.strip()
            d['phone'] = soup.find('td', text='Di động').findNextSibling().text.strip()
            time = soup.find('td', text='Ngày đăng tin').findNextSibling().text.strip()
            time = time.split('/')
            d['time'] = datetime.datetime(int(time[2]), int(time[1]), int(time[0]))
            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)

def crawlBySchedule():
    stop = 0 # stop while loop when stop = 1
    page = 1
    now = re.split("\s",str(datetime.datetime.now()))[0]
    now = re.split("-",now)
    file_path = os.getcwd() + "/dothi/" + "dothi-" + now[0] + now[1] + now[2] + ".csv"
    writeFieldNameToFile(file_path)
    iterator = 0
    while stop == 0:
        l = []
        try:
            r = requests.get(Url + '/p' + str(page) + '.htm')
            soup = BeautifulSoup(r.content, 'lxml')

        except:
            print("Page Url Error: ")
            print(Url + '/p' + str(page) + '.htm')
            continue
        list_link = soup.find_all('a', class_='vip2')
        for link in list_link:
            r = requests.get(BaseUrl + link['href'])
            soup = BeautifulSoup(r.content, 'lxml')
            if single_thread_leveldb.check_exist(BaseUrl + link['href'], '/tmp/leveldb/dothi/') == 1:
                if iterator == 3:
                    stop = 1
                    break
                iterator = iterator + 1
            else:
                iterator = 0
                single_thread_leveldb.insert_link(BaseUrl + link['href'], '/tmp/leveldb/dothi/')
            d = {}
            d['prid'] = soup.find('td', text='Mã số').findNextSibling().text.strip()
            d['title'] = soup.find('div', class_='product-detail').findChild('h1').text.strip()
            d['des'] = soup.find('div', class_='pd-desc-content').text.strip()
            d['phone'] = soup.find('td', text='Di động').findNextSibling().text.strip()
            time = soup.find('td', text='Ngày đăng tin').findNextSibling().text.strip()
            time = time.split('/')
            d['time'] = datetime.datetime(int(time[2]), int(time[1]), int(time[0]))

            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)
        page = page + 1


keys = sys.argv[1::2]
values = sys.argv[2::2]
args = {k: v for k, v in zip(keys, values)}
Path(os.getcwd() + "/dothi").mkdir(parents=True, exist_ok=True)
first_time = args.get('--first-time')
if first_time == '1':
    writeFieldNameToFile(os.getcwd()+"/dothi/"+"dothi.csv")
    print("crawlDataFirstTime")
    final = getFinalPage()
    numProcess = 2  # number process
    ### Multiprocessing with Process
    processes = [Process(target=crawlDataFirstTime, args=(i, i + int(final / numProcess), final)) for i in
                 range(1, final, int(final / numProcess))]  # init numProcess process
    # Run processes
    for p in processes: p.start()
    # Exit the completed processes
    for p in processes: p.join()
else:
    print("crawlBySchedule")
    crawlBySchedule()