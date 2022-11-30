from pyspark.sql.functions import col, broadcast, sum, rand, floor
from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, overwrite_trunc_data, save_data
from infra.spark_session import get_spark_session

# 전국 17개 광역시도별 등기 구분 수 집계
class SidoRegist:

    @classmethod
    def save(cls):
        df_re_own = find_data(DataWarehouse, "REALESTATE_OWN")
        df_re_own.createOrReplaceTempView('REALESTATE_OWN')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_fin = get_spark_session().sql('''SELECT LOC.SIDO AS REGN,
                                                SUM(TOT) AS TOT,
                                                ROUND((SUM(TOT) / (SELECT SUM(TOT) FROM REALESTATE_OWN) * 100), 1)  AS RATE
                                            FROM REALESTATE_OWN RO INNER JOIN LOC ON LOC.LOC_CODE = RO.REGN_CODE
                                            GROUP BY SIDO''')  # 735.101s

        # df_fin = get_spark_session().sql('''
        # SELECT LO.SIDO AS REGN,
        # SUM(TOT) AS TOT,
        # ROUND((SUM(TOT) / (SELECT SUM(TOT) FROM REALESTATE_OWN) * 100), 1)  AS RATE
        # FROM (SELECT REGN_CODE, TOT FROM REALESTATE_OWN) RO INNER JOIN (SELECT LOC_CODE, SIDO FROM LOC) LO ON LO.LOC_CODE = RO.REGN_CODE
        # GROUP BY SIDO''')  # 753.827s


        #get_spark_session().conf.set('spark.sql.shuffle.partitions', 24)

        # 디멘젼 줄이기
        # df_re_own = df_re_own.select(col('REGN_CODE'), col('TOT'))
        # df_loc = df_loc.select(col('LOC_CODE'), col('SIDO'))

        #sum_real = df_re_own.agg(sum('TOT'))

        # df_re_own = df_re_own.groupBy(col('REGN_CODE')) \
        #                     .agg(sum('TOT').alias('TOT'))
        
        # broadcast를 이용해 조인
        #df_fin = df_loc.join(df_re_own, df_re_own.REGN_CODE == df_loc.LOC_CODE)
        # df_fin = df_re_own.join(broadcast(df_loc), df_re_own.REGN_CODE == df_loc.LOC_CODE)

        # df_fin.show()  # broadcast와 함께 조인하면 6.893s / broadcast 없이 조인하면 320.024s

        # df_fin.createOrReplaceTempView('FIN')

        # df_fin = get_spark_session().sql('''SELECT SIDO AS REGN,
        #                                         SUM(TOT) AS TOT,
        #                                         ROUND((SUM(TOT) / (SELECT SUM(TOT) FROM REALESTATE_OWN) * 100), 1) AS RATE
        #                                     FROM FIN
        #                                     GROUP BY SIDO''')
        
        # df_fin = df_fin.groupBy([col('SIDO')]).agg(sum("TOT"))  # 383.196s 조인한 다음 groupBy하는거
        # df_fin.show()
        # df_fin.explain()

        overwrite_trunc_data(DataMart, df_fin, "SIDO_REGIST")