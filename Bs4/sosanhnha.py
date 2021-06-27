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
sys.path.append('../')
from CustomLibs import single_thread_leveldb
from CustomLibs import test_leveldb

baseUrl='https://sosanhnha.com'
URL='https://sosanhnha.com/search?iCit=30'

def getFinalPage(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html5lib')
    div = soup.find_all('strong')
    total = re.sub(',','',div[1].text)
    return int(total)
def writeFieldNameToFile(file):
    field_name = []
    field_name.append({'prid': 'prid', 'title': 'title', 'des': 'des', 'phone': 'phone', 'time': 'time'})
    df = pandas.DataFrame(field_name)
    df.to_csv(file, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)
def crawlDataFirstTime(start, end):
    now = datetime.datetime.now()
    starttime= now.strftime("%H:%M:%S")
    print("start Time =", starttime)
    count = 0
    if(end >= getFinalPage(URL)): end = getFinalPage(URL) + 1
    print("run from " + str(start) + " to " + str(end))
    file_path = os.getcwd()+"/"+"sosanhnha2.csv"
    for i in range(start, end):
        l = []
        try:
            r = requests.get(URL+'&page='+str(i))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Page Url Error: ")
            print(URL + '?page=' + str(i))
            continue
        list_link = soup.find_all('a', class_='name')
        for  link in  list_link:
                d = {}
                href = link['href']
                try:
                    request = requests.get(baseUrl + href)
                    test_leveldb.insert_link(baseUrl + href, start)
                except:
                    print("Post Url Error: ")
                    print(baseUrl + href)
                    continue
                try:
                    soup = BeautifulSoup(request.content, 'html5lib')
                    infor = soup.find('div', class_='info')
                    d['prid'] = infor.findChildren('span')[0].text
                    d['title'] = soup.find('h1', class_='title').text
                    d['des'] = soup.find('div', class_='description').text
                    d['phone'] = re.split(":",soup.find('a', class_='user_phone')['href'])[-1]
                    dayPost = infor.findChildren('span')[3].text
                except:
                    print("Get atribute value error")
                    continue
                #format time
                dayPost = re.split("/", dayPost)
                d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]))
                l.append(d)
                count = count+1
        df= pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)
    print("total :"+str(count))
    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("starttime= ", starttime)
    print("end Time =", current_time)

def crawlBySchedule(): #crawl data after day : day-month-year
    stop = 0 # stop while loop when stop = 1
    page = 1
    now = re.split("\s",str(datetime.datetime.now()))[0]
    now = re.split("-",now)

    file_path = os.getcwd() + "/" + "sosanhnha-" + now[0] + now[1] + now[2] + ".csv"
    writeFieldNameToFile(file_path)
    iterator = 0
    while stop == 0:
        l = []
        try:
            r = requests.get(URL + '&page=' + str(page))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Page Url Error: ")
            print(URL + '?page=' + str(page))
            continue
        list_link = soup.find_all('a', class_='name')
        for link in list_link:
            d = {}
            href = link['href']
            try:
                request = requests.get(baseUrl + href)
            except:
                print("Post Url Error: ")
                print(baseUrl + href)
                continue
            if single_thread_leveldb.check_exist(baseUrl + href) == 1:
                if iterator == 3:
                    stop = 1
                    break
                iterator = iterator + 1
            else:
                iterator = 0
                single_thread_leveldb.insert_link(baseUrl + href)
            try:
                soup = BeautifulSoup(request.content, 'html5lib')
                infor = soup.find('div', class_='info')
                d['prid'] = infor.findChildren('span')[0].text
                d['title'] = soup.find('h1', class_='title').text
                d['des'] = soup.find('div', class_='description').text
                d['phone'] = re.split(":", soup.find('a', class_='user_phone')['href'])[-1]
                dayPost = infor.findChildren('span')[3].text
            except:
                print("Get atribute value error")
                continue
            # format time
            dayPost = re.split("/", dayPost)
            d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]))
            l.append(d)

        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)


keys = sys.argv[1::2]
values = sys.argv[2::2]

print(keys)
print(values)

args = {k: v for k, v in zip(keys, values)}
print(args)

first_time = args.get('--first-time')
if first_time == '1':
    print("crawlDataFirstTime")
    writeFieldNameToFile(os.getcwd()+"/"+"sosanhnha2.csv")
    final = getFinalPage(URL)
    numProcess = multiprocessing.cpu_count() * 2 - 1  # run process
    print(numProcess)
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




