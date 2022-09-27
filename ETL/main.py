#!/usr/bin/env python3
"""
Module Docstring
"""
import sys
# from datajob.datamart.co_popu_density import CoPopuDensity
# from datajob.datamart.co_vaccine import CoVaccine
# from datajob.etl.extract.corona_api import CoronaApiExtractor
# from datajob.etl.extract.corona_vaccine import CoronaVaccineExtractor
# from datajob.etl.transform.corona_patient import CoronaPatientTransformer
# from datajob.etl.transform.corona_vaccine import CoronaVaccineTransformer
# from datajob.etl.transform.loc import LocTransformer

# def transform_execute():
#     CoronaPatientTransformer.transform()
#     CoronaVaccineTransformer.transform()

# def datamart_execute():
#     CoPopuDensity.save()
#     CoVaccine.save()

# # 모듈이름과, 모듈에서 호출하는 함수를 key-value로 매칭
# def main(transform_execute, datamart_execute):
#     works = {
#         'extract': {
#             'corona_api': CoronaApiExtractor.extract_data,  # 함수객체 저장
#             'corona_vaccine': CoronaVaccineExtractor.extract_data
#         },
#         'transform': {
#             'execute' : transform_execute,
#             'corona_patient': CoronaPatientTransformer.transform,  # 함수객체 저장
#             'corona_vaccine': CoronaVaccineTransformer.transform
#         },
#         'datamart':{
#             'execute' : datamart_execute,
#             'co_popu_density': CoPopuDensity.save,
#             'co_vaccine': CoVaccine.save
#         }
#     }
#     return works

# works = main(transform_execute, datamart_execute)


if __name__ == "__main__":
    # python3 main.py arg1 arg2
    # 값을 받을 인자는 2개임
    # 인자1 : 작업(extract, transform, datamart)
    # 인자2 : 저장할 위치(테이블)
    # ex) python3 main.py extract corona_api

    # args = sys.argv
    
    # if len(args) != 3:
    #     raise Exception('2개의 전달인자가 필요합니다')
    
    # if args[1] not in works.keys():
    #     raise Exception('첫번째 전달인자가 이상함 >> ' + str(works.keys()))

    # if args[2] not in works[args[1]].keys():
    #     raise Exception('두번째 전달인자가 이상함 >> ' + str(works[args[1]].keys()))

    # work = works[args[1]][args[2]]
    # work() # 함수객체를 이용해 함수 호출