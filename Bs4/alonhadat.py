
import lxml
import requests
import csv
import os
from datetime import datetime
from bs4 import BeautifulSoup

baseUrl = "https://alonhadat.com.vn/"
url = "https://alonhadat.com.vn/nha-dat/can-ban/nha-dat/2/ho-chi-minh.html"
#https://meeyland.com/mua-ban-nha-dat/ho-chi-minh/page-31785
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("start Time =", current_time)
file_path = os.getcwd()+"\\"+"meeyland.csv"
for i  in range  (1,2):
    r = requests.get(url+'/page-'+str(i))
    soup = BeautifulSoup(r.content, 'html5lib')
    list_link = soup.find_all('div', class_='col-sm-6 col-md-4')
    for  link in  list_link:
        child = link.findChild()
        href = link['href']
        print(baseUrl + href)
        request = requests.get(baseUrl+href)
        print(baseUrl+href)
        soup = BeautifulSoup(request.content, 'html5lib')



