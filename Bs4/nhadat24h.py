import datetime
from selenium import webdriver
from multiprocessing import Process

import re
import pandas
import os
import csv
from pathlib import Path
from bs4 import BeautifulSoup
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys
sys.path.append('../')
from CustomLibs import single_thread_leveldb
from CustomLibs import test_leveldb
DRIVER_PATH = '/usr/bin/chromedriver'
def getFinalPage(url):
    request = requests.get(url)
    soup = BeautifulSoup(request.content, 'html5lib')
    num_post = soup.find('a', id='txtnumketquatimkiem').findChild('strong').text
    num_post = num_post.replace(" ", "")
    num_post_each_page = 20
    return int(int(num_post) / num_post_each_page)
def writeFieldNameToFile(file):
    field_name = []
    field_name.append({'prid': 'prid', 'title': 'title', 'des': 'des', 'phone': 'phone', 'time': 'time'})
    df = pandas.DataFrame(field_name)
    df.to_csv(file, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)
def crawlDataFirstTime(start, end, final):
    Path("/tmp/leveldb/nhadat24h").mkdir(parents=True, exist_ok=True)
    options = webdriver.ChromeOptions()
    prefs = {"profile.default_content_setting_values.notifications": 2}
    options.add_argument('--headless')
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(executable_path=DRIVER_PATH, options=options)
    baseUrl = "https://nhadat24h.net"
    url = "https://nhadat24h.net/nha-dat-ban-thanh-pho-ho-chi-minh"
    file_path = os.getcwd() + "/nhadat24h" + "/nhadat24h.csv"
    if end >= final :  end = final+1
    print("run from " + str(start) + " to " + str(end))
    for i in range(start, end):
        l = []
        list_link = []
        while(list_link == []):
            try:
                driver.get(url + '/page' + str(i))
                driver.maximize_window()
            except:
                print("Page Url Error: ")
                print(url + '?page=' + str(i))
                continue
            soup = BeautifulSoup(driver.page_source, 'html5lib')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.XPATH, "//div[@class='dv-item']")))
            list_link = soup.find_all('div', class_ = 'dv-item')
            for link in list_link:
                d = {}
                href = link.findChild('a')['href']
                time = link.findChild('p').text
                r = requests.get(baseUrl+href)
                test_leveldb.insert_link(baseUrl + href, start, '/tmp/leveldb/nhadat24h/')
                soup = BeautifulSoup(r.content, 'html5lib')
                #print("link:",baseUrl+href)
                d['prid'] = href.split('ID')[-1]
                #print("id:", d['prid'])
                d['title'] = soup.find('a', id='txtcontenttieudetin').text.strip()
                #print(d['title'])
                d['des'] = soup.find('div', id='ContentPlaceHolder1_ctl00_divContent').text.strip()
                #print(d['des'])
                d['phone'] = soup.find('div', class_='panelActionContent').findChild('a').text.strip()
                #print(d['phone'])
                if "/" in time:
                    #print(time)
                    time = time.split(',')[0]
                    time = time.split('/')
                    d['time'] = datetime.datetime(int(time[2]), int(time[1]), int(time[0]))
                    #print(d['time'])
                else:
                    d['time'] = datetime.datetime.now()
                l.append(d)


        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)
    driver.quit()

def crawlBySchedule():
    stop = 0  # stop while loop when stop = 1
    page = 1
    now = re.split("\s", str(datetime.datetime.now()))[0]
    now = re.split("-", now)
    file_path = os.getcwd()  + "/nhadat24h" + "/nhadat24h-" + now[0] + now[1] + now[2] + ".csv"
    writeFieldNameToFile(file_path)
    options = webdriver.ChromeOptions()
    prefs = {"profile.default_content_setting_values.notifications": 2}
    options.add_argument('--headless')
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(executable_path=DRIVER_PATH, options=options)
    baseUrl = "https://nhadat24h.net"
    url = "https://nhadat24h.net/nha-dat-ban-thanh-pho-ho-chi-minh"
    iterator = 0
    while stop == 0:
        l = []
        list_link = []
        while(list_link == []):
            try:
                driver.get(url + '/page' + str(page))
                driver.maximize_window()
            except:
                print("Page Url Error: ")
                print(url + '?page=' + str(page))
                continue
            soup = BeautifulSoup(driver.page_source, 'html5lib')
            WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.XPATH, "//div[@class='dv-item']")))
            list_link = soup.find_all('div', class_ = 'dv-item')
            for link in list_link:
                d = {}
                href = link.findChild('a')['href']
                time = link.findChild('p').text
                r = requests.get(baseUrl+href)
                if single_thread_leveldb.check_exist(baseUrl + href, '/tmp/leveldb/nhadat24h/') == 1:
                    if iterator == 3:
                        stop = 1
                        break
                    iterator = iterator + 1
                else:
                    iterator = 0
                    single_thread_leveldb.insert_link(baseUrl + href, '/tmp/leveldb/nhadat24h/')
                soup = BeautifulSoup(r.content, 'html5lib')
                d['prid'] = href.split('ID')[-1]
                #print("id:", d['prid'])
                d['title'] = soup.find('a', id='txtcontenttieudetin').text.strip()
                #print(d['title'])
                d['des'] = soup.find('div', class_='dv-m-ct-dt').text.strip()
                #print(d['des'])
                d['phone'] = soup.find('div', class_='panelActionContent').findChild('a').text.strip()
                #print(d['phone'] )
                if "/" in time:
                    time = time.split(',')[0]
                    time = time.split('/')
                    d['time'] = datetime.datetime(int(time[2]), int(time[1]), int(time[0]))
                    #print(d['time'])
                else:
                    d['time'] = datetime.datetime.now()
                    #print(d['time'])
                l.append(d)

        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)
    driver.quit()

keys = sys.argv[1::2]
values = sys.argv[2::2]

print(keys)
print(values)

args = {k: v for k, v in zip(keys, values)}
print(args)

first_time = args.get('--first-time')
Path(os.getcwd() + "/nhadat24h").mkdir(parents=True, exist_ok=True)
if first_time == '1':
    print("crawlDataFirstTime")
    writeFieldNameToFile(os.getcwd() + "/nhadat24h" + "/nhadat24h.csv")
    final = getFinalPage('https://nhadat24h.net/nha-dat-ban-thanh-pho-ho-chi-minh')
    numProcess = 2 #multiprocessing.cpu_count() * 2 - 1  # run process
    print(numProcess)
    ## Multiprocessing with Process
    processes = [Process(target=crawlDataFirstTime, args=(i, i + int(final / numProcess), final)) for i in
                 range(1, final, int(final / numProcess))]  # init numProcess process
    # Run processes
    for p in processes: p.start()
    # Exit the completed processes
    for p in processes: p.join()
else:
    print("crawlBySchedule")
    crawlBySchedule()
