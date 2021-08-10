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
sys.path.append('../')
from CustomLibs import single_thread_leveldb
from CustomLibs import test_leveldb
from CustomLibs import Csv

baseUrl = "https://bds123.vn"
url = "https://bds123.vn/nha-dat-ban-ho-chi-minh.html"

Path(os.getcwd() + "/"+datetime.datetime.now().strftime('%Y%m%d')).mkdir(parents=True, exist_ok=True)
PATH_FILE_LOG = os.getcwd() + "/"+datetime.datetime.now().strftime('%Y%m%d')

from bs4 import BeautifulSoup
def writeFieldNameToFile(file_path):
    field_name = []
    field_name.append({'prid': 'prid', 'title': 'title', 'des': 'des', 'phone': 'phone', 'time': 'time'})
    df = pandas.DataFrame(field_name)
    df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaT")
def getFinalPage(urlPara):
    r = requests.get(urlPara)
    soup = BeautifulSoup(r.content, 'html5lib')
    final_page = re.split("=", soup.find_all('a', class_='page-link')[-1]['href'])[-1]
    return int(final_page)
def crawlDataFirstTime(start, end):
    Path("/tmp/leveldb/bds123").mkdir(parents=True, exist_ok=True)
    final = getFinalPage(url)
    if end >= final : end = final + 1

    file_path = PATH_FILE_LOG+"/"+"bds123.csv"
    for i in range(start, end):
        l = []
        try:
            r = requests.get(url+'?created=desc&page='+str(i))
            soup = BeautifulSoup(r.content, 'html5lib')
        except Exception as err:
            print("Page Url Error: ")
            print(url + '?page=' + str(i))
            Csv.write_log(PATH_FILE_LOG, datetime.now().strftime('%Y%m%d'),
                          'BDS123 -' + str(err) + '-' + url + '?page=' + str(i))
            continue
        list_link = soup.find_all('a', class_='link')
        for  link in  list_link:
                d = {}
                href = link['href']
                test_leveldb.insert_link(baseUrl + href, start,'/tmp/leveldb/bds123/')
                try:
                    request = requests.get(baseUrl + href)
                except Exception as err:
                    print("Post Url Error: ")
                    print(baseUrl + href)
                    Csv.write_log(PATH_FILE_LOG, datetime.now().strftime('%Y%m%d'), 'BDS123 -' + str(err) + '-' +baseUrl + href)
                    continue
                try:
                    soup = BeautifulSoup(request.content, 'html5lib')
                    d['prid'] = soup.find('section', class_='section-post-detail')['data-id']
                    d['title'] = soup.find('h1', class_='post-h1').text
                    d['des'] = soup.find('div', class_='post-description').text
                    d['phone'] = soup.find('button', class_='author-phone').text
                    time = soup.find('div', class_='item published')
                    d['time'] = time.findChild('span')['title']
                except Exception as err:
                    print("Get atribute value error")
                    Csv.write_log(PATH_FILE_LOG, datetime.now().strftime('%Y%m%d'),
                                  'BDS123 -' + str(err) + '-' + baseUrl + href)

                    continue
                #format time
                dayPost = re.split("\s", d['time'])[-1]
                dayPost = re.split("/", dayPost)
                timePost = re.split("\s", d['time'])[-2]
                timePost = re.split(":", timePost)
                d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]))
                d=Csv.processData(d)
                l.append(d)
        df= pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaT")

def crawlBySchedule(): #crawl data after day : day-month-year
    stop = 0 # stop while loop when 4 url is duplicate
    page = 1
    file_path = PATH_FILE_LOG + "/" + "bds123-" + datetime.datetime.now().strftime('%Y%m%d') + ".csv"
    writeFieldNameToFile(file_path)
    Path("/tmp/leveldb/bds123/").mkdir(parents=True, exist_ok=True)
    # os.mkdir('/tmp/leveldb/bds123/')
    iterator = 0
    while stop == 0:
        l = []
        try:
            r = requests.get(url + '?created=desc&page=' + str(page))
            soup = BeautifulSoup(r.content, 'html5lib')
        except Exception as err:
            print("Page Url Error: ")
            print(url + '?page=' + str(page))
            Csv.write_log(PATH_FILE_LOG, datetime.now().strftime('%Y%m%d'),
                          'BDS123 -' + str(err) + '-' + url + '?page=' + str(page))
            continue
        list_link = soup.find_all('a', class_='link')
        for link in list_link:
            href = link['href']
            d = {}
            if single_thread_leveldb.check_exist(baseUrl + href,'/tmp/leveldb/bds123/') == 1:
                if iterator == 3 :
                    stop = 1
                    break
                iterator = iterator+1
            else:
                iterator = 0
                single_thread_leveldb.insert_link(baseUrl + href,'/tmp/leveldb/bds123/')

            try:
                request = requests.get(baseUrl + href)
            except Exception as err:
                print("Post Url Error: ")
                print(baseUrl + href)
                Csv.write_log(PATH_FILE_LOG, datetime.now().strftime('%Y%m%d'),
                              'BDS123 -' + str(err) + '-' + baseUrl + href)
                continue
            try:
                soup = BeautifulSoup(request.content, 'html5lib')
                d['prid'] = soup.find('section', class_='section-post-detail')['data-id']
                d['title'] = soup.find('h1', class_='post-h1').text
                d['des'] = soup.find('div', class_='post-description').text
                d['phone'] = soup.find('button', class_='author-phone').text
                time = soup.find('div', class_='item published')
                d['time'] = time.findChild('span')['title']
            except Exception as err:
                print("Get atribute value error")
                Csv.write_log(PATH_FILE_LOG, datetime.now().strftime('%Y%m%d'),
                              'BDS123 -' + str(err) + '-' + baseUrl + href)

                continue
            dayPost = re.split("\s",d['time'])[-1]
            dayPost = re.split("/", dayPost)
            # timePost = re.split("\s", d['time'])[-2]
            # timePost = re.split(":", timePost)
            d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]))
            d = Csv.processData(d)
            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaT")
        page = page + 1


'''if __name__ == '__main__':
    #crawlBySchedule(2021,6,16)
    final = getFinalPage('https://bds123.vn/nha-dat-ban-ho-chi-minh.html')
    numProcess = multiprocessing.cpu_count()*2 - 4 # run process

    print(numProcess)
    #print("Final page: "+str(final)+" / " + str(final/numProcess))
    ### Multiprocessing with Process
    processes=[Process(target=crawlDataFirstTime,args=(i,i+int(final/numProcess))) for i in range(1, final, int(final/numProcess))] #init numProcess process

    #multithreading
    #threads=[Thread(target=crawlDataFirstTime,args=(i,i+int(final/numProcess))) for i in range(1, final, int(final/numProcess))]

    # Run processes
    for p in processes:p.start()
    # Exit the completed processes
    for p in processes:p.join()'''



keys = sys.argv[1::2]
values = sys.argv[2::2]

print(keys)
print(values)

args = {k: v for k, v in zip(keys, values)}
print(args)

first_time = args.get('--first-time')

if first_time == '1':
    print("crawlDataFirstTime")
    writeFieldNameToFile(PATH_FILE_LOG+"/"+"bds123.csv")
    final = getFinalPage('https://bds123.vn/nha-dat-ban-ho-chi-minh.html')
    numProcess = 1 #multiprocessing.cpu_count() * 2 - 4  # run process
    print(numProcess)
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


