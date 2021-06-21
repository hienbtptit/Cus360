import datetime

from selenium import webdriver
from multiprocessing import Process
import time
import re
import pandas
import os
import csv
from selenium.webdriver.chrome.options import Options
import multiprocessing
import sys
sys.path.append('../')
from CustomLibs import single_thread_leveldb
from CustomLibs import test_leveldb
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


baseUrl = "https://batdongsan.com.vn"
URL = "https://batdongsan.com.vn/nha-dat-ban-tp-hcm"
DRIVER_PATH = 'D:\\bin\\chromedriver.exe'

def getFinalPage(url):
    driver = webdriver.Chrome(executable_path=DRIVER_PATH)
    driver.get(URL)
    driver.maximize_window()
    final_page = driver.find_element_by_xpath("/html/body/div[1]/div[8]/div[1]/div[4]/div[21]/div/a[6]").get_attribute('pid')
    driver.quit()
    return int(final_page)
def crawlDataFirstTime(start, end, final):
    if(end >= final): end = final +1
    file_path = os.getcwd() + "\\" + "batdongsan.csv"
    driver = webdriver.Chrome(executable_path=DRIVER_PATH)
    for page in range(start, end):
        driver.get(URL + '/p' + str(page))
        #print(URL + '/p' + str(page))
        driver.maximize_window()
        wait = WebDriverWait(driver, 60)
        try:
            wait.until(EC.visibility_of_all_elements_located((By.XPATH, "//a[@class='wrap-plink']")))
            list_link = driver.find_elements_by_xpath("//a[@class='wrap-plink']")
        except:
            print("Page link error or time out")
            continue
        links = []
        list_data = []
        for l in list_link:
            links.append(l.get_attribute('href'))
        for post_link in links:
            #print(post_link)
            try:
                driver.get(post_link)
                driver.maximize_window()
                test_leveldb.insert_link(post_link, start)
            except:
                continue
            try:
                d = {}
                d['prid'] = wait.until(EC.visibility_of_element_located((By.XPATH, "//div[@id='product-detail-web']"))).find_element_by_xpath("//div[@id='product-detail-web']").get_attribute('prid')
                d['title'] =  wait.until(EC.visibility_of_element_located((By.XPATH, "//div[@class='containerTitle']"))).find_element_by_xpath("//div[@class='containerTitle']").text
                d['des'] =  wait.until(EC.visibility_of_element_located((By.XPATH, "//div[@class='des-product']"))).find_element_by_xpath("//div[@class='des-product']").text
                d['phone'] =  wait.until(EC.visibility_of_element_located((By.XPATH, "//span[@class='phoneEvent']"))).find_element_by_xpath("//span[@class='phoneEvent']").get_attribute('raw')
                time=  wait.until(EC.visibility_of_element_located((By.XPATH, "//span[@class='sp1' and text()='Ngày đăng:']/following-sibling::span"))).find_element_by_xpath("//span[@class='sp1' and text()='Ngày đăng:']/following-sibling::span").text
            except:
                print("Get atribute error", post_link)
                continue
            dayPost = re.split("/", time)
            d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]))
            #print("time:",d['time'])
            list_data.append(d)
        df = pandas.DataFrame(list_data)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)

    driver.quit()
    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("end Time =", current_time)
def crawlBySchedule(): #crawl data after day : day-month-year

    driver = webdriver.Chrome(executable_path=DRIVER_PATH)
    stop = 0 # stop while loop when stop = 1
    page = 1
    now = re.split("\s",str(datetime.datetime.now()))[0]
    now = re.split("-",now)
    file_path = os.getcwd() + "\\" + "batdongsan-" + now[0] + now[1] + now[1] + ".csv"
    iterator = 0
    while stop == 0:
        driver.get(URL + '/p' + str(page))
        # print(URL + '/p' + str(page))
        driver.maximize_window()
        wait = WebDriverWait(driver, 60)
        try:
            wait.until(EC.visibility_of_all_elements_located((By.XPATH, "//a[@class='wrap-plink']")))
            list_link = driver.find_elements_by_xpath("//a[@class='wrap-plink']")
        except:
            print("Page link error or time out")
            continue
        links = []
        list_data = []
        for l in list_link:
            links.append(l.get_attribute('href'))
        for post_link in links:
            # print(post_link)
            try:
                driver.get(post_link)
                driver.maximize_window()
                if single_thread_leveldb.check_exist(post_link) == 1:
                    if iterator == 3:
                        stop = 1
                        break
                    iterator = iterator + 1
                else:
                    iterator = 0
                    single_thread_leveldb.insert_link(post_link)
            except:
                continue
            try:
                d = {}
                d['prid'] = wait.until(EC.visibility_of_element_located(
                    (By.XPATH, "//div[@id='product-detail-web']"))).find_element_by_xpath(
                    "//div[@id='product-detail-web']").get_attribute('prid')
                d['title'] = wait.until(EC.visibility_of_element_located(
                    (By.XPATH, "//div[@class='containerTitle']"))).find_element_by_xpath(
                    "//div[@class='containerTitle']").text
                d['des'] = wait.until(
                    EC.visibility_of_element_located((By.XPATH, "//div[@class='des-product']"))).find_element_by_xpath(
                    "//div[@class='des-product']").text
                d['phone'] = wait.until(
                    EC.visibility_of_element_located((By.XPATH, "//span[@class='phoneEvent']"))).find_element_by_xpath(
                    "//span[@class='phoneEvent']").get_attribute('raw')
                time = wait.until(EC.visibility_of_element_located((By.XPATH,
                                                                    "//span[@class='sp1' and text()='Ngày đăng:']/following-sibling::span"))).find_element_by_xpath(
                    "//span[@class='sp1' and text()='Ngày đăng:']/following-sibling::span").text
            except:
                print("Get atribute error", post_link)
                continue
            dayPost = re.split("/", time)
            d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]))
            list_data.append(d)
        df = pandas.DataFrame(list_data)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)
        page = page + 1

keys = sys.argv[1::2]
values = sys.argv[2::2]

print(keys)
print(values)

args = {k: v for k, v in zip(keys, values)}
print(args)

first_time = args.get('--first-time')
if first_time == '1':
    print("crawlDataFirstTime")
    final = getFinalPage(URL)
    numProcess = multiprocessing.cpu_count()  # run process

    print(numProcess)
    # print("Final page: "+str(final)+" / " + str(final/numProcess))
    ### Multiprocessing with Process
    processes = [Process(target=crawlDataFirstTime, args=(i, i + int(final / numProcess), final)) for i in
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






