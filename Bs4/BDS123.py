from multiprocessing import Process
import requests
import csv
import os
import multiprocessing
import pandas
import re

from datetime import datetime
from bs4 import BeautifulSoup
def getFinalPage(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html5lib')
    final_page = re.split("=", soup.find_all('a', class_='page-link')[-1]['href'])[-1]
    return int(final_page)
def crawlDataFirstTime(start, end):
    l = []
    baseUrl = "https://bds123.vn"
    url = "https://bds123.vn/nha-dat-ban-ho-chi-minh.html"
    if(end >= getFinalPage(url)): end = getFinalPage(url) + 1
    print("run from " + str(start) + " to " + str(end))
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Start"+current_time);
    file_path = os.getcwd()+"\\"+"bds1234.csv"

    for i in range(start, end):
        try:
            r = requests.get(url+'?page='+str(i))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Url Error: ")
            print(url + '?page=' + str(i))
            continue
        list_link = soup.find_all('a', class_='link')
        for  link in  list_link:
                d = {}
                try:
                    href = link['href']
                    request = requests.get(baseUrl+href)
                except:
                    print("Url Error: ")
                    print(baseUrl + href)
                    continue
                try:
                    soup = BeautifulSoup(request.content, 'html5lib')
                    d['prid'] = soup.find('section', class_='section-post-detail')['data-id']
                    d['title'] = soup.find('h1', class_='post-h1').text
                    d['des'] = soup.find('div', class_='post-description').text
                    d['phone'] = soup.find('button', class_='author-phone').text
                    time = soup.find('div', class_='item published')
                    d['time'] = time.findChild('span')['title']
                    l.append(d)
                except:
                    print("Get atribute value error")
                    continue
    df= pandas.DataFrame(l)
    df.to_csv("bds123.csv", mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)

if __name__ == '__main__':
    final = getFinalPage('https://bds123.vn/nha-dat-ban-ho-chi-minh.html')
    numProcess = 13
    #print("Final page: "+str(final)+" / " + str(final/numProcess))
    ### Multiprocessing with Process
    processes=[Process(target=crawlDataFirstTime,args=(i,i+int(final/numProcess))) for i in range(1, final, int(final/numProcess))]
    # Run processes
    for p in processes:p.start()
    # Exit the completed processes
    for p in processes:p.join()

