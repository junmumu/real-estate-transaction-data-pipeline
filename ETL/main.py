#!/usr/bin/env python3
"""
Module Docstring
"""
import sys
from datajob.datamart.monthly_apt_prc import MonthlyAptPrc
from datajob.datamart.seoul_gu_regist import SeoulGuRegist
from datajob.datamart.sido_regist import SidoRegist
from datajob.etl.extract.apartment_sale_price import ApartmentSalePrice

from datajob.etl.extract.local_code import LocalCode
from datajob.etl.extract.own_transfer_by_address import OwnTransferByAddress
from datajob.etl.extract.own_transfer_by_gender_age import OwnTransferByGenderAge
from datajob.etl.extract.own_transfer_by_local_foreigner_corp import OwnTransferByLocalForeignerCorp
from datajob.etl.extract.own_transfer_by_nationality import OwnTransferByNationality
from datajob.etl.transform.apartment_sale_price import ApartmentSalePriceTransformer
from datajob.etl.transform.local_code import LocalCodeTransformer
from datajob.etl.transform.own_sex_age import OwnSexAgeTransformer


def transform_execute():
    LocalCodeTransformer.transform()
    ApartmentSalePriceTransformer.transform()
    OwnSexAgeTransformer.transform()

def datamart_execute():
    MonthlyAptPrc.save()
    SeoulGuRegist.save()
    SidoRegist.save

# 모듈이름과, 모듈에서 호출하는 함수를 key-value로 매칭
def main(transform_execute, datamart_execute):
    works = {
        'extract': {
            'local_code': LocalCode.extract_data,  # 함수객체 저장
            'apartment_sale_price': ApartmentSalePrice.extract_data,
            'own_transfer_by_address': OwnTransferByAddress.extract_data,
            'own_transfer_by_gender_age': OwnTransferByGenderAge.extract_data,
            'own_transfer_by_local_foreigner_corp': OwnTransferByLocalForeignerCorp.extract_data,
            'own_transfer_by_nationality': OwnTransferByNationality.extract_data
        },
        'transform': {
            'execute': transform_execute,
            'local_code': LocalCodeTransformer.transform,  # 함수객체 저장
            'apartment_sale_price': ApartmentSalePriceTransformer.transform,
            'own_sex_age': OwnSexAgeTransformer.transform
        },
        'datamart': {
            'execute': datamart_execute,
            'monthly_apt_prc': MonthlyAptPrc.save,
            'seoul_gu_regist': SeoulGuRegist.save,
            'sido_regist': SidoRegist.save
        }
    }
    return works

works = main(transform_execute, datamart_execute)


if __name__ == "__main__":
    # python3 main.py arg1 arg2
    # 값을 받을 인자는 2개임
    # 인자1 : 작업(extract, transform, datamart)
    # 인자2 : 저장할 위치(테이블)
    # ex) python3 main.py extract corona_api

    args = sys.argv
    
    if len(args) != 3:
        raise Exception('2개의 전달인자가 필요합니다')
    
    if args[1] not in works.keys():
        raise Exception('첫번째 전달인자가 이상함 >> ' + str(works.keys()))

    if args[2] not in works[args[1]].keys():
        raise Exception('두번째 전달인자가 이상함 >> ' + str(works[args[1]].keys()))

    # work = works[args[1]][args[2]]
    # work() # 함수객체를 이용해 함수 호출
    
