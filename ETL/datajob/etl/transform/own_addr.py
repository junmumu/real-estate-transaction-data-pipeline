from pyspark.sql.functions import col
from infra.hdfs_client import get_client
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
import pandas as pd

class OwnAddrTransformer:
    FILE_DIR = '/real_estate/address/'

    @classmethod
    def transform(cls, before_cnt):

        for i in range(before_cnt, before_cnt + 1):
            # address_data_20161001.json
            file_name = 'address_data_' + cal_std_day(i) + '.json'
            print(cls.FILE_DIR + file_name)

            data = get_spark_session().read.json(cls.FILE_DIR + file_name)

            data = data.select('result').first()
            data = data['result']['items']['item']

            df_own_addr = get_spark_session().createDataFrame(data).select(col('adminRegn1Name'), col('adminRegn1NamePerson'), col('resDate'), col('tot'))


            df_loc = find_data(DataWarehouse, "LOC")
            df_loc = df_loc.select(col('LOC_CODE'), col('SIDO')).where(col('SIGUNGU_CODE') == '000')
            df_loc.show(5)

            df_own_addr.show()
            df_own_addr = df_own_addr.join(df_loc, df_own_addr.adminRegn1Name == df_loc.SIDO) \
                                        .select(col('adminRegn1Name'), col('adminRegn1NamePerson'), col('resDate'), col('tot'), col('LOC_CODE').alias('RES_REGN_CODE'))
            df_own_addr.show()
            df_own_addr = df_own_addr.join(df_loc, df_own_addr.adminRegn1NamePerson == df_loc.SIDO) \
                                        .select(col('resDate').alias('RES_DATE'), col('RES_REGN_CODE'), col('LOC_CODE').alias('BUYER_REGN_CODE'), col('tot').alias('TOT'))
            df_own_addr.show()

            #save_data(DataWarehouse, df_own_addr, "OWN_ADDR")

            #df_own_addr.write.csv(cls.FILE_DIR + 'tmp.csv')
