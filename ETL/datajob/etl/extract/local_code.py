import time
import pandas as pd
from bs4 import BeautifulSoup
from infra.hdfs_client import get_client
from infra.util import execute_rest_api


class LocalCode:
    URL = 'http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList'
    SERVICE_KEY = 'NvoJpM4nyxzXkb5F8hffSDHLrfuCyIcVBqBSDCgTa+/7CtQnsrFwE8y/a0lLPVxN1AESAPkiMkfoS7KYrck13A=='

    @classmethod
    def extract_data(cls):
        params = {'serviceKey': cls.SERVICE_KEY,
                'pageNo': '1',
                'numOfRows': '1000',
                'type': 'xml'
        }
        # <totalcount>20550</totalcount>
        # 최대 numofrows = 1000
        # pageno -> 21페이지
        max_page_no = 21
        data = []
        for page_no in range(1, max_page_no + 1):  # 
            # set params
            params['pageNo'] = str(page_no)
            params['numOfRows'] = '1000'

            # execute rest api
            res = execute_rest_api('get', cls.URL, {}, params)
        
            # get data from tag
            soup = BeautifulSoup(res, 'html.parser')
            region_cds = soup.findAll('region_cd')
            sido_cds = soup.findAll('sido_cd')
            sgg_cds = soup.findAll('sgg_cd')
            umd_cds = soup.findAll('umd_cd')
            ri_cds = soup.findAll('ri_cd')
            locatadd_nms = soup.findAll('locatadd_nm')
            locathigh_cds = soup.findAll('locathigh_cd')
            locallow_nms = soup.findAll('locallow_nm')

            #time.sleep(3)

            # construct list and append to datalist
            for i in range(len(region_cds)):
                data.append([region_cds[i].text, sido_cds[i].text, sgg_cds[i].text, umd_cds[i].text, ri_cds[i].text, 
                            locatadd_nms[i].text, locathigh_cds[i].text, locallow_nms[i].text])
            print(page_no, len(data))
            
            
        # write to csv file
        df = pd.DataFrame(data)
        with get_client().write('/real_estate/local_code/local_code.csv', overwrite=True, encoding='CP949') as writer:
            df.to_csv(writer, header=['지역코드', '시도코드', '시군구코드',  '읍면동코드', '리코드', '지역주소명',
            '상위지역코드', '최하위지역명'], index=False)