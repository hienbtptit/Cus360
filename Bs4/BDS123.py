from multiprocessing import Process
from threading import Thread
import requests
import csv
import os
import multiprocessing
import pandas
import re
import datetime

baseUrl = "https://bds123.vn"
url = "https://bds123.vn/nha-dat-ban-ho-chi-minh.html"

from bs4 import BeautifulSoup
def getFinalPage(urlPara):
    r = requests.get(urlPara)
    soup = BeautifulSoup(r.content, 'html5lib')
    final_page = re.split("=", soup.find_all('a', class_='page-link')[-1]['href'])[-1]
    return int(final_page)

def crawlDataFirstTime(start, end):
    if(end >= getFinalPage(url)): end = getFinalPage(url) + 1
    print("run from " + str(start) + " to " + str(end))
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Start"+current_time);
    file_path = os.getcwd()+"\\"+"bds123.csv"

    for i in range(start, end):
        l = []
        try:
            r = requests.get(url+'?created=desc&page='+str(i))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Page Url Error: ")
            print(url + '?page=' + str(i))
            continue
        list_link = soup.find_all('a', class_='link')
        for  link in  list_link:
                d = {}
                try:
                    href = link['href']
                    request = requests.get(baseUrl + href)
                except:
                    print("Post Url Error: ")
                    print(baseUrl + href)
                    continue
                ''' if test_leveldb.check_exist(baseUrl+href) == 1: continue
                else:
                    test_leveldb.insert_link(baseUrl+href)'''
                try:
                    soup = BeautifulSoup(request.content, 'html5lib')
                    d['prid'] = soup.find('section', class_='section-post-detail')['data-id']
                    d['title'] = soup.find('h1', class_='post-h1').text
                    d['des'] = soup.find('div', class_='post-description').text
                    d['phone'] = soup.find('button', class_='author-phone').text
                    time = soup.find('div', class_='item published')
                    d['time'] = time.findChild('span')['title']
                except:
                    print("Get atribute value error")
                    continue
                l.append(d)
        df= pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)

def crawlBySchedule(year, month, day): #crawl data after day : day-month-year
    stop = 0 # stop while loop when stop = 1
    page = 1
    file_path = os.getcwd() + "\\" + "bds123-"+str(year)+str(month)+str(day)+".csv"
    #now = re.split("\s",str(datetime.datetime.now()))[0]
    #now = re.split("-",date)
    #now = datetime.datetime(int(now[0]), int(now[1]), int(now[2]))
    now = datetime.datetime(year,month,day)
    while stop == 0:
        l = []
        try:
            r = requests.get(url + '?created=desc&page=' + str(page))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Page Url Error: ")
            print(url + '?page=' + str(page))
            continue
        list_link = soup.find_all('a', class_='link')
        for link in list_link:
            d = {}
            try:
                href = link['href']
                request = requests.get(baseUrl + href)
            except:
                print("Post Url Error: ")
                print(baseUrl + href)
                continue
            ''' if test_leveldb.check_exist(baseUrl+href) == 1: continue
            else:
                test_leveldb.insert_link(baseUrl+href)'''
            try:
                soup = BeautifulSoup(request.content, 'html5lib')
                d['prid'] = soup.find('section', class_='section-post-detail')['data-id']
                d['title'] = soup.find('h1', class_='post-h1').text
                d['des'] = soup.find('div', class_='post-description').text
                d['phone'] = soup.find('button', class_='author-phone').text
                time = soup.find('div', class_='item published')
                d['time'] = time.findChild('span')['title']
                timePost = re.split("\s",d['time'])[-1]
                timePost = re.split("/", timePost)
                timePost = datetime.datetime(int(timePost[2]), int(timePost[1]), int(timePost[0]))
                #check if timepost < date then break the loop
                if (timePost < now) :
                    stop = 1
                    break
            except:
                print("Get atribute value error")
                continue
            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)
        page = page + 1


if __name__ == '__main__':
    crawlBySchedule(2021,6,16)
    '''final = getFinalPage('https://bds123.vn/nha-dat-ban-ho-chi-minh.html')
    numProcess = multiprocessing.cpu_count()*2 - 4 # run 13 process

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


