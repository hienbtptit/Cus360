import multiprocessing
from multiprocessing import Process
from threading import Thread
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
def getFinalPage(url):
    request = requests.get(url)
    soup = BeautifulSoup(request.content, 'html5lib')
    num_post = soup.find('span', class_='blue').text
    num_post = num_post.replace(",", "")
    num_post_each_page = 24
    #print('Number post: ' + num_post + "/ Number page: " + str(int(num_post) / num_post_each_page));
    return int(int(num_post) / num_post_each_page)
def writeFieldNameToFile(file):
    field_name = []
    field_name.append({'prid': 'prid', 'title': 'title', 'des': 'des', 'phone': 'phone', 'time': 'time'})
    df = pandas.DataFrame(field_name)
    df.to_csv(file, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)

def crawlDataFirstTime(start, end):
    baseUrl = "https://meeyland.com"#/mua-ban-nha-dat/giay-to-day-du-ban-can-ho-gia-de-cu-chi-2-05-ty-ngay-tren-quan-9-ho-chi-minh-dt-la-65-m2-1603716314861
    url = "https://meeyland.com/mua-ban-nha-dat/ho-chi-minh"
    #make folder store CSV files
    Path("/tmp/leveldb/meeyland").mkdir(parents=True, exist_ok=True)
    file_path = os.getcwd()+"/meeyland/"+"meeyland.csv"
    writeFieldNameToFile(file_path)
    final = getFinalPage('https://meeyland.com/mua-ban-nha-dat/ho-chi-minh')
    if end >= final :  end = final+1
    for i  in range  (start,end):
        l = []
        try:
            r = requests.get(url+'/page-'+str(i))
        except:
            print("URL page error: "+url+'/page-'+str(i))
            continue
        soup = BeautifulSoup(r.content, 'html5lib')
        list_link = soup.find_all('div', class_='col-sm-6 col-md-4')
        for link in list_link:
            d={}
            href = link.findChild('a')['href']
            if(re.search(baseUrl, href)):
                continue
            else:
                try:
                    request = requests.get(baseUrl + href)
                    test_leveldb.insert_link(baseUrl + href, start,'/tmp/leveldb/meeyland/')
                except:
                    print("URL error: "+baseUrl + href)
                    continue
            soup = BeautifulSoup(request.content, 'html5lib')
            # print(soup.prettify())
            try:
                d['prid'] = soup.find('div', id='nav-tabContent')['data-id']
                #print("prid: "+d['prid'])
                d['title'] = soup.find('h1', id='nav-home').text
                #print("title: "+d['title'])
                d['des'] = soup.find('div', class_='des-detail').text
                #print("des: "+d['des'])
                d['phone'] = soup.find('div', class_='phone')['data-phone']
                #print("phone: "+d['phone'])
                d['time'] = soup.find('div', text=re.compile('^Ngày đăng')).text
                #print("time: "+d['time'])
            except:
                print(str(i)+ ": "+href)
                print("Get atribute value error")
                continue
            dayPost = re.split("\s", d['time'])[-1]
            dayPost = re.split("/", dayPost)
            d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]))
            d = Csv.processData(d)
            l.append(d)
        df= pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)

def crawlBySchedule():
    baseUrl = "https://meeyland.com"#/mua-ban-nha-dat/giay-to-day-du-ban-can-ho-gia-de-cu-chi-2-05-ty-ngay-tren-quan-9-ho-chi-minh-dt-la-65-m2-1603716314861
    url = "https://meeyland.com/mua-ban-nha-dat/ho-chi-minh"
    stop = 0 # stop while loop when stop = 1
    page = 1
    now = re.split("\s",str(datetime.datetime.now()))[0]
    now = re.split("-",now)
    #make folder store CSV files
    Path("/tmp/leveldb/meeyland").mkdir(parents=True, exist_ok=True)
    file_path = os.getcwd() + "/meeyland/" + "meeyland-" + now[0] + now[1] + now[2] + ".csv"
    writeFieldNameToFile(file_path)
    iterator = 0
    while stop == 0:
        l = []
        try:
            r = requests.get(url + '?created=desc&page=' + str(page))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Page Url Error: ")
            print(url + '?page=' + str(page))
            continue
        list_link = soup.find_all('div', class_='col-sm-6 col-md-4')
        for link in list_link:
            d = {}
            href = link.findChild('a')['href']
            if (re.search(baseUrl, href)):
                continue
            else:
                try:
                    request = requests.get(baseUrl + href)
                except:
                    print("URL error: " + baseUrl + href)
                    continue
            soup = BeautifulSoup(request.content, 'html5lib')
            if single_thread_leveldb.check_exist(baseUrl + href,'/tmp/leveldb/meeyland/') == 1:
                if iterator == 3 :
                    stop = 1
                    break
                iterator = iterator+1
            else:
                iterator = 0
                single_thread_leveldb.insert_link(baseUrl + href,'/tmp/leveldb/meeyland/')
            try:
                d['prid'] = soup.find('div', id='nav-tabContent')['data-id']
                #print("prid: "+d['prid'])
                d['title'] = soup.find('h1', id='nav-home').text
                #print("title: "+d['title'])
                d['des'] = soup.find('div', class_='des-detail').text
                #print("des: "+d['des'])
                d['phone'] = soup.find('div', class_='phone')['data-phone']
                #print("phone: "+d['phone'])
                d['time'] = soup.find('div', text=re.compile('^Ngày đăng')).text
                #print("time: "+d['time'])
            except:
                print(str(page) + ": " + href)
                print("Get atribute value error")
                continue
            dayPost = re.split("\s", d['time'])[-1]
            dayPost = re.split("/", dayPost)
            d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]))
            d = Csv.processData(d)
            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)
        page = page + 1


keys = sys.argv[1::2]
values = sys.argv[2::2]

print(keys)
print(values)

args = {k: v for k, v in zip(keys, values)}
print(args)

first_time = args.get('--first-time')
Path(os.getcwd() + "/meeyland").mkdir(parents=True, exist_ok=True)
if first_time == '1':
    print("crawlDataFirstTime")
    writeFieldNameToFile(os.getcwd()+"/meeyland/"+"meeyland.csv")
    final = getFinalPage('https://meeyland.com/mua-ban-nha-dat/ho-chi-minh')
    numProcess = 2#multiprocessing.cpu_count() * 2 - 6  # run process
    # print("Final page: "+str(final)+" / " + str(final/numProcess))
    ### Multiprocessing with Process
    processes = [Process(target=crawlDataFirstTime, args=(i, i + int(final / numProcess))) for i in
                 range(1, final, int(final / numProcess))]  # init numProcess process

    # multithreading
    # threads=[Thread(target=crawlDataFirstTime,args=(i,i+int(final/numProcess))) for i in range(1, final, int(final/numProcess))]

    # Run processes
    for p in processes: p.start()
    # Exit the completed processes
    for p in processes: p.join()
else:
    print("crawlBySchedule")
    crawlBySchedule()
