import requests 
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from tqdm import tqdm



class BooktopiaScraper:

    def __init__(self) -> None:
        self.api_url = "https://www.booktopia.com.au/_next/data/BiLaGwnyd3BPwc2WwI_bZ/search.json?keywords={ISBN}&productType=917504&pn=1"
        self.headers = {
            "User-Agent":'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        }
        self.dicts = []
        self.failed_inputs = []

    
    def __get_isbn_url(self, isbn):
        endpoint = self.api_url.replace('{ISBN}',str(isbn))
        resp = requests.get(endpoint, headers=self.headers)
        if resp.status_code == 200:
            resp = resp.json()
            resp = resp['pageProps']
            if '__N_REDIRECT' in resp:
                url = resp['__N_REDIRECT']
                splitted = url.split('/')
                name = splitted[1]
                book_type = splitted[2]
                data_api = "https://www.booktopia.com.au/_next/data/BiLaGwnyd3BPwc2WwI_bZ" + url + f".json?productName={name}&type={book_type}"
                return data_api

    def scrape_by_isbn(self, isbn):
        data_dict = dict()
        data_api = self.__get_isbn_url(isbn=isbn)
        if data_api != None:
            resp = requests.get(data_api, headers=self.headers)
            if resp.status_code == 200:
                resp = resp.json()
                if 'product' in resp['pageProps']:
                    resp = resp['pageProps']['product']
                    data_dict['title'] = resp['displayName']
                    data_dict['author(s)'] = '|'.join([i['name'] for i in resp['contributors'] if i['role'] == 'Author'])
                    data_dict['retail_price'] = resp['retailPrice']
                    data_dict['discounted/sale_price'] = resp['salePrice']
                    data_dict['isbn10'] = resp['isbn10']
                    data_dict['book_type'] = resp['bindingFormat']
                    data_dict['number_of_pages'] = resp['numberOfPages']
        
        if data_dict != {}:
            self.dicts.append(data_dict)
        else:
            self.failed_inputs.append(isbn)
            
            
    
    def bulk_isbn(self, isbns):
        with ThreadPoolExecutor() as exe:
            list(tqdm(exe.map(self.scrape_by_isbn, isbns), total=len(isbns)))

        df = pd.DataFrame(self.dicts)
        failed = pd.DataFrame({"isbn":self.failed_inputs})
        return df, failed



if __name__ == '__main__':
    bs = BooktopiaScraper()
    isbns = pd.read_csv('input_list.csv')
    isbns = list(isbns['ISBN13'])
    fin = bs.bulk_isbn(isbns)
    print('Parsed results')
    fin[0].to_csv('fin.csv', index=False)
    print(fin[0])
    print("Didn't work for the following inputs")
    fin[1].to_csv('failed.csv', index=False)
    print(fin[1])