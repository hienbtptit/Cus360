from multiprocessing import Process
import multiprocessing
import requests
import csv
import os
import pandas
import re
import datetime
import sys
from pathlib import Path
from bs4 import BeautifulSoup
sys.path.append('../')
from CustomLibs import single_thread_leveldb
from CustomLibs import test_leveldb
from CustomLibs import Csv
baseUrl='https://sosanhnha.com'
URL='https://sosanhnha.com/search?iCit=30'
Path(os.getcwd() + "/"+datetime.datetime.now().strftime('%Y%m%d')).mkdir(parents=True, exist_ok=True)
PATH_FILE_LOG = os.getcwd() + "/"+datetime.datetime.now().strftime('%Y%m%d')
def getFinalPage(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html5lib')
    div = soup.find_all('strong')
    total = re.sub(',','',div[1].text)
    return int(total)
def writeFieldNameToFile(file_path):
    field_name = []
    field_name.append({'prid': 'prid', 'title': 'title', 'des': 'des', 'phone': 'phone', 'time': 'time'})
    df = pandas.DataFrame(field_name)
    df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaT")
def crawlDataFirstTime(start, end):
    Path("/tmp/leveldb/sosanhnha").mkdir(parents=True, exist_ok=True)
    final = getFinalPage(URL)
    if end >= final: end = final + 1
    file_path = PATH_FILE_LOG + "/" + "sosanhnha.csv"
    for i in range(start, end):
        l = []
        try:
            r = requests.get(URL+'&page='+str(i))
            soup = BeautifulSoup(r.content, 'html5lib')
        except Exception as err:
            print("Page Url Error: ")
            print(URL + '?page=' + str(i))
            Csv.write_log(PATH_FILE_LOG, "sosanhnha-" + datetime.datetime.now().strftime('%Y%m%d'),
                          'sosanhnha -' + str(err) + '-' + URL + '&page=' + str(i))
            continue
        list_link = soup.find_all('a', class_='name')
        for  link in  list_link:
                d = {}
                href = link['href']
                try:
                    request = requests.get(baseUrl + href)
                except Exception as err:
                    print("Post Url Error: ")
                    print(baseUrl + href)
                    Csv.write_log(PATH_FILE_LOG, "sosanhnha-" + datetime.datetime.now().strftime('%Y%m%d'),
                                  'sosanhnha -' + str(err) + '-' + baseUrl + href)
                    continue
                test_leveldb.insert_link(baseUrl + href, start, '/tmp/leveldb/sosanhnha/')
                try:
                    soup = BeautifulSoup(request.content, 'html5lib')
                    infor = soup.find('div', class_='info')
                    d['prid'] = infor.findChildren('span')[0].text
                    d['title'] = soup.find('h1', class_='title').text
                    d['des'] = soup.find('div', class_='description').text
                    d['phone'] = re.split(":",soup.find('a', class_='user_phone')['href'])[-1]
                    #print("phone: ", d['phone'] )
                    dayPost = infor.findChildren('span')[3].text
                except Exception as err:
                    print("Get atribute value error")
                    Csv.write_log(PATH_FILE_LOG, "sosanhnha-" + datetime.datetime.now().strftime('%Y%m%d'),
                                  'sosanhnha -' + str(err) + '-' + baseUrl + href)
                    continue
                #format time
                dayPost = re.split("/", dayPost)
                d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]))
                d=Csv.processData(d)
                l.append(d)

        df= pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaT")

def crawlBySchedule(): #crawl data after day : day-month-year
    stop = 0 # stop while loop when stop = 1
    page = 0
    file_path = PATH_FILE_LOG + "/" + "sosanhnha-" + datetime.datetime.now().strftime('%Y%m%d') + ".csv"
    writeFieldNameToFile(file_path)
    iterator = 0
    while stop == 0:
        page = page + 1
        l = []
        try:
            r = requests.get(URL + '&page=' + str(page))
            soup = BeautifulSoup(r.content, 'html5lib')
        except Exception as err:
            print("Page Url Error: ")
            print(URL + '?page=' + str(page))
            Csv.write_log(PATH_FILE_LOG, "sosanhnha-" + datetime.datetime.now().strftime('%Y%m%d'),
                          'sosanhnha -' + str(err) + '-' + URL + '&page=' + str(page))
            continue
        list_link = soup.find_all('a', class_='name')
        for link in list_link:
            d = {}
            href = link['href']
            try:
                request = requests.get(baseUrl + href)
            except Exception as err:
                print("Post Url Error: ")
                print(baseUrl + href)
                Csv.write_log(PATH_FILE_LOG, "sosanhnha-" + datetime.datetime.now().strftime('%Y%m%d'),
                              'sosanhnha -' + str(err) + '-' + baseUrl + href)
                continue
            if single_thread_leveldb.check_exist(baseUrl + href, '/tmp/leveldb/sosanhnha/') == 1:
                if iterator == 3:
                    stop = 1
                    break
                iterator = iterator + 1
            else:
                iterator = 0
                single_thread_leveldb.insert_link(baseUrl + href, '/tmp/leveldb/sosanhnha/')
            try:
                soup = BeautifulSoup(request.content, 'html5lib')
                infor = soup.find('div', class_='info')
                d['prid'] = infor.findChildren('span')[0].text
                d['title'] = soup.find('h1', class_='title').text
                d['des'] = soup.find('div', class_='description').text
                d['phone'] = re.split(":", soup.find('a', class_='user_phone')['href'])[-1]
                dayPost = infor.findChildren('span')[3].text
            except Exception as err:
                print("Get atribute value error")
                Csv.write_log(PATH_FILE_LOG, "sosanhnha-" + datetime.datetime.now().strftime('%Y%m%d'),
                              'sosanhnha -' + str(err) + '-' + baseUrl + href)
                continue
            # format time
            dayPost = re.split("/", dayPost)
            d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]))
            d=Csv.processData(d)
            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaT")

keys = sys.argv[1::2]
values = sys.argv[2::2]

print(keys)
print(values)

args = {k: v for k, v in zip(keys, values)}
print(args)

first_time = args.get('--first-time')
if first_time == '1':
    print("crawlDataFirstTime")
    writeFieldNameToFile(PATH_FILE_LOG + "/"+"sosanhnha.csv")
    final = getFinalPage(URL)
    numProcess = 2 #multiprocessing.cpu_count() * 2 - 1  # run process
    ## Multiprocessing with Process
    processes = [Process(target=crawlDataFirstTime, args=(i, i + int(final / numProcess))) for i in
                 range(1, final, int(final / numProcess))]  # init numProcess process
    # Run processes
    for p in processes: p.start()
    # Exit the completed processes
    for p in processes: p.join()
else:
    print("crawlBySchedule")
    crawlBySchedule()




