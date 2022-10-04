import json
from pyspark.sql.types import *
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from infra.logger import get_logger
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day

class OwnSexAgeTransform:

    @classmethod
    def transform(cls): 
        # DW에서 지역코드 불러오기
        df_loc = find_data(DataWarehouse, 'LOC')
        loc_code = df_loc.select(['SIDO','LOC_CODE']).filter(df_loc.SIGUNGU.isNull()).collect()
        df_loc_code = get_spark_session().createDataFrame(loc_code)

        # extract데이터 불러오기
        path = '/realestate_data/gender_age/gender_age_data_'+cal_std_day(11)+'.json'
        tmp = get_spark_session().read.json(path, encoding='UTF-8')
        tmp2 = tmp.select('result').first()
        df = get_spark_session().createDataFrame(tmp2)
        tmp3 = df.select('items').first()
        tmp4 = get_spark_session().createDataFrame(tmp3).first()
        df2 = get_spark_session().createDataFrame(tmp4['item'])

        df_sex_age = df2.select(df2.adminRegn1Name.alias('SIDO'),df2.resDate.alias('RES_DATE'),df2.sex.alias('BUYER_SEX'),df2.bdata_age.alias('BUYER_AGES'),df2.tot.alias('TOT'))

        # sido명으로 조인
        own_sex_age = df_sex_age.join(df_loc_code, on='SIDO')
        own_sex_age = own_sex_age.select(col('LOC_CODE').alias('RES_REGN_CODE'),col('TOT').cast('int'),col('BUYER_AGES'),col('BUYER_SEX'),col('RES_DATE').cast(DateType()))
        own_sex_age = own_sex_age.withColumn('OSA_IDX', row_number().over(Window.orderBy(monotonically_increasing_id())))

        # save in DW
        save_data(DataWarehouse, own_sex_age, 'OWN_SEX_AGE')

    # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('own_sex_age_transform').error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls):
        log_dict = {
                "is_success": "Fail",
                "type": "own_sex_age_transform"
            }
        return log_dict