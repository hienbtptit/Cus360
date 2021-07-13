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
BaseUrl = 'https://bonbanh.com/'
ListUrl=['https://bonbanh.com/tp-hcm/oto', 'https://bonbanh.com/ha-noi/oto']
def writeFieldNameToFile(file):
    field_name = []
    field_name.append({'prid': 'prid', 'title': 'title', 'des': 'des', 'phone': 'phone', 'time': 'time'})
    df = pandas.DataFrame(field_name)
    df.to_csv(file, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)
def getFinalPage(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')
    page = soup.find_all('div', class_ = 'navpage')
    final = page[2].findChildren('span')[-1]['url'].split(',')[-1]
    return int(final)
def crawlDataFirstTime(start, end):
    Path("/tmp/leveldb/bonbanh").mkdir(parents=True, exist_ok=True)
    file_path = os.getcwd() + "/bonbanh/" + "bonbanh.csv"
    for url in ListUrl:
        final = getFinalPage(url)
        if start >= final : continue
        if end >= final : end = final
        for i in range (start, end+1):
            l = []
            r = requests.get(url+'/page,'+str(i))
            soup = BeautifulSoup(r.content, 'lxml')
            listPost = soup.find_all('li', class_='car-item')
            for post in listPost:
                d={}
                d['prid'] = post.findChild('span', class_='car_code').text.split(" ")[-1].strip()
                d['title'] = post.findChild('a')['title'].strip()
                d['des'] = post.findChild('div', itemprop='description').text.strip()
                d['phone'] = post.findChild('div', class_='cb7').text.split('ĐT:')[-1].strip()
                link = BaseUrl+post.findChild('a')['href']
                r = requests.get(link)
                test_leveldb.insert_link(link, start, '/tmp/leveldb/bonbanh/')
                soup = BeautifulSoup(r.content, 'lxml')
                time = soup.find('div', class_='notes').text.split('\t')
                for t in time:
                    if  '/' in t :
                        if ' ' in t : t = t.split(' ')[-1]
                        tmp = t.strip().split('/')
                        d['time'] = datetime.datetime(int(tmp[2]), int(tmp[1]), int(tmp[0]))
                        break
                l.append(d)
            df = pandas.DataFrame(l)
            df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)

def crawlBySchedule():
    stop = 0 # stop while loop when stop = 1
    now = re.split("\s",str(datetime.datetime.now()))[0]
    now = re.split("-",now)
    file_path = os.getcwd() + "/bonbanh/" + "bonbanh-" + now[0] + now[1] + now[2] + ".csv"
    writeFieldNameToFile(file_path)
    iterator = 0
    for url in ListUrl:
        page = 1
        while stop == 0:
            l = []
            try:
                r = requests.get(url+'/page,'+str(page))
                soup = BeautifulSoup(r.content, 'html5lib')
            except:
                print("Page Url Error: ")
                print(url + '?page=' + str(page))
                continue
            listPost = soup.find_all('li', class_='car-item')
            for post in listPost:
                d={}
                link = BaseUrl + post.findChild('a')['href']
                if single_thread_leveldb.check_exist(link, '/tmp/leveldb/bonbanh/') == 1:
                    if iterator == 3 :
                        stop = 1
                        break
                    iterator = iterator+1
                else:
                    iterator = 0
                    single_thread_leveldb.insert_link(link, '/tmp/leveldb/bonbanh/')
                d['prid'] = post.findChild('span', class_='car_code').text.split(" ")[-1].strip()
                d['title'] = post.findChild('a')['title'].strip()
                d['des'] = post.findChild('div', itemprop='description').text.strip()
                d['phone'] = post.findChild('div', class_='cb7').text.split('ĐT:')[-1].strip()
                r = requests.get(link)
                soup = BeautifulSoup(r.content, 'lxml')
                time = soup.find('div', class_='notes').text.split('\t')
                for t in time:
                    if '/' in t:
                        if ' ' in t: t = t.split(' ')[-1]
                        tmp = t.strip().split('/')
                        d['time'] = datetime.datetime(int(tmp[2]), int(tmp[1]), int(tmp[0]))
                        break
                l.append(d)
            df = pandas.DataFrame(l)
            df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)
            page = page + 1

keys = sys.argv[1::2]
values = sys.argv[2::2]
args = {k: v for k, v in zip(keys, values)}
Path(os.getcwd() + "/bonbanh").mkdir(parents=True, exist_ok=True)
first_time = args.get('--first-time')
if first_time == '1':
    writeFieldNameToFile(os.getcwd()+"/bonbanh/"+"bonbanh.csv")
    print("crawlDataFirstTime")
    numProcess = 2  # number process
    final = 0
    for url in ListUrl:
        f = getFinalPage(url)
        if f > final : final = f
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