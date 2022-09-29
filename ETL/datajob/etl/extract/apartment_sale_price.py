import json
from infra.hdfs_client import get_client
from infra.logger import get_logger
from infra.util import cal_std_month, execute_rest_api


class ApartmentSalePrice:
    URL = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev?"
    SERVICE_KEY = 'NvoJpM4nyxzXkb5F8hffSDHLrfuCyIcVBqBSDCgTa+/7CtQnsrFwE8y/a0lLPVxN1AESAPkiMkfoS7KYrck13A=='
    FILE_DIR = '/realestate_data/apartment_price/'

    @classmethod
    def extract_data(cls, before_cnt=1):
        for i in range(1, before_cnt + 1):
            params = {
                    'ServiceKey':cls.SERVICE_KEY
                    ,'pageNo':'1'
                    ,'numOfRows':'10'
                    ,'LAWD_CD':'11110'
                    ,'DEAL_YMD':cal_std_month(i)
            }
                    
            log_dict = {
                "is_success": "Fail",
                "type": "apartment_sale_price",
                "std_day": params['DEAL_YMD'],
                "params": params
            }
            print(params)

            try:
                res = execute_rest_api('get', cls.URL, {}, params)
                file_name = 'apartment_sale_price_' + params['DEAL_YMD'] 
                get_client().write(hdfs_path=cls.FILE_DIR + file_name, data=res, overwrite=True, encoding='utf-8')

            except Exception as e:
                log_dict['err_msg'] = e.__str__()
                log_json = json.dumps(log_dict, ensure_ascii=False) # python 딕셔너리를 json문자열로 만들기
                print(log_dict['err_msg'])
                get_logger('apartment_sale_price').error(log_json)
