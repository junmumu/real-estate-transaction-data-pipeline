
import json
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import *
from infra.jdbc import DataWarehouse, find_data, overwrite_data, save_data
from infra.logger import get_logger
from infra.spark_context import get_spark_context
from infra.spark_session import get_spark_session


class ApartmentSalePriceTransformer:
    FILE_DIR = '/real_estate/apartment_price/'

    @classmethod
    def transform(cls):
        # DW LOC테이블에서 지역코드정보 가져옴
        df_loc = find_data(DataWarehouse, 'LOC')
        loc_codes = df_loc.select('LOC_CODE').collect()

        for i in range(2, 3):  # len(loc_codes)
            try:
                # 지역코드를 이용해 csv파일 읽기
                loc_code = loc_codes[i][0]
                file_name = 'apart_price_data_' + loc_code + '.csv'
                df_apt_prc = get_spark_session().read.csv(cls.FILE_DIR + file_name, encoding='CP949', header=True)
                df_apt_prc.show(3)
                print(df_apt_prc.dtypes)
                
                # 거래금액을 리스트로 받아서 int형으로 formatting 진행
                prc_list = df_apt_prc.select(col('거래금액(만원)').alias('amount')).collect()
                prc_list_trans = []
                for prc in prc_list:
                    prc_split = prc.amount.split(',')
                    tmp = ''
                    for s in prc_split:
                        tmp += s
                    tmp = int(tmp)
                    prc_list_trans.append(tmp)
                print(prc_list_trans[:3], type(prc_list_trans[0]))
                
                # 데이터프레임으로 생성하기 전 rdd이용해 transform
                rdd_prc_list_trans = get_spark_context().parallelize(c=prc_list_trans)
                rdd_prc_list_trans = rdd_prc_list_trans.map(lambda x: [x])  # transform the rdd

                # 거래금액 리스트를 데이터프레임으로 생성
                schema = StructType([StructField("amount", IntegerType(), False)])
                df_prc = get_spark_session().createDataFrame(data=rdd_prc_list_trans, schema=schema)

                # select columns and cast type
                df_apt_prc = df_apt_prc.select(col('거래날짜').cast(DateType()).alias('res_date'), col('전용면적').cast('float').alias('area'), col('지역코드').alias('regn_code'))

                # 두 데이터프레임을 합하기 위해서 두 DF에서 가상으로 idx만든다음, 그 idx로 join
                df_prc = df_prc.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
                df_apt_prc = df_apt_prc.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
                df_apt_prc = df_apt_prc.join(df_prc, on=["row_index"]).drop("row_index", "name")
                df_apt_prc.show(3)
                print(df_apt_prc.dtypes)

                # DW에 Load
                save_data(DataWarehouse, df_apt_prc, 'REAL_PRC_APT')
                #overwrite_data(DataWarehouse, df_apt_prc, 'REAL_PRC_APT')
            except Exception as e:
                log_dict = cls.__create_log_dict()
                cls.__dump_log(log_dict, e)
            
    # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('apartment_sale_price_transform').error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls):
        log_dict = {
                "is_success": "Fail",
                "type": "apartment_sale_price_transform"
            }
        return log_dict