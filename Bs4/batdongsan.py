
import lxml
import requests
import csv
import os
from datetime import datetime
from bs4 import BeautifulSoup

baseUrl = "https://batdongsan.com.vn/"
url = "https://batdongsan.com.vn/nha-dat-ban-tp-hcm"
#https://meeyland.com/mua-ban-nha-dat/ho-chi-minh/page-31785
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("start Time =", current_time)
file_path = os.getcwd()+"\\"+"batdongsan.csv"
header = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36" ,
    'referer':'https://batdongsan.com.vn/'
}
r = requests.get(baseUrl, headers=header)
soup = BeautifulSoup(r.content, 'html5lib')
print(soup.prettify())
for i  in range  (1,3):
    r = requests.get(url+'/p'+str(i))
    print(url+'/p'+str(i))
    soup = BeautifulSoup(r.content, 'html5lib')

    list_link = soup.find_all('a', class_='wrap-plink')
    for  link in  list_link:
        print("each link:")
        href = link['href']
        print(baseUrl + href)
        request = requests.get(baseUrl+href)
        soup = BeautifulSoup(request.content, 'html5lib')



