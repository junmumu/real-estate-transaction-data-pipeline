import json
from pyspark.sql.types import *
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from infra.logger import get_logger
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day



class OwnAddrTransform:

    @classmethod
    def transform(cls): 
        # DW에서 지역코드 불러오기
        df_loc = find_data(DataWarehouse, 'LOC')
        loc_code = df_loc.select(['SIDO','LOC_CODE']).filter(df_loc.SIGUNGU.isNull()).collect()
        df_loc_code = get_spark_session().createDataFrame(loc_code)

        for i in range(14,15):
            file_name = '/realestate_data/address/address_data_'+cal_std_day(i)+'.json'
            tmp = get_spark_session().read.json(file_name, encoding='UTF-8')
            tmp2 = tmp.select('result').first()
            df = get_spark_session().createDataFrame(tmp2)
            tmp3 = df.select('items').first()
            tmp4 = get_spark_session().createDataFrame(tmp3).first()
            df2 = get_spark_session().createDataFrame(tmp4['item'])

            df_addr = df2.select(df2.adminRegn1Name.alias('SIDO'),df2.resDate.alias('RES_DATE'),df2.adminRegn1NamePerson.alias('SIDO2'),df2.tot.alias('TOT'))

            own_addr = df_addr.join(df_loc_code, on='SIDO').drop(col('SIDO'))
            own_addr = own_addr.select(col('LOC_CODE').alias('RES_REGN_CODE'),col('SIDO2').alias('SIDO'),col('TOT').cast('int'),col('RES_DATE').cast(DateType()) )
            own_addr = own_addr.join(df_loc_code, on='SIDO').drop(col('SIDO'))
            own_addr = own_addr.select(col('LOC_CODE').alias('BUYER_REGN_CODE'),col('TOT'),col('RES_REGN_CODE'),col('RES_DATE'))
            own_addr = own_addr.withColumn('OA_IDX', row_number().over(Window.orderBy(monotonically_increasing_id())))

            save_data(DataWarehouse, own_addr, "OWN_ADDR")

    # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('own_addr_transform').error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls):
        log_dict = {
                "is_success": "Fail",
                "type": "own_addr_transform"
            }
        return log_dict
