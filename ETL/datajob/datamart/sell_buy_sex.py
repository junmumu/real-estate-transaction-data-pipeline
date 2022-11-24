from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, overwrite_trunc_data, save_data
from infra.spark_session import get_spark_session

# 성별 누적매도매수량 집계
class AccSellBuySex:
    @classmethod
    def save(cls):
        sex_ages = find_data(DataWarehouse, 'OWN_SEX_AGE')
        sex_ages.createOrReplaceTempView("sex_ages")
        sex = get_spark_session().sql("""select BUYER_SEX as SEX, sum(TOT) as BUY_TOT ,
                                        round((sum(TOT)/(select sum(TOT) from sex_ages)*100),1) as BUY_RATE
                                        from sex_ages group by BUYER_SEX""")
        overwrite_trunc_data(DataMart, sex, "ACC_SELL_BUY_SEX")

# 성별 연도별 매도매수량 집계
class SellBuySexYear:
    @classmethod
    def save(cls):
        sex_ages = find_data(DataWarehouse, 'OWN_SEX_AGE')
        sex_ages.createOrReplaceTempView("sex_ages")
        sex_year = get_spark_session().sql("""select BUYER_SEX as SEX , DATE_FORMAT(RES_DATE,'y') AS YEAR , SUM(TOT) AS BUY_TOT
                                            from sex_ages 
                                            GROUP BY DATE_FORMAT(RES_DATE,'y'), BUYER_SEX""")

        overwrite_trunc_data(DataMart, sex_year, "SELL_BUY_SEX_YEAR")

# 성별 광역시도별 누적매도매수량 집계
class AccSellBuySexSido:
    @classmethod
    def save(cls):
        sex_ages = find_data(DataWarehouse, 'OWN_SEX_AGE')
        sex_ages.createOrReplaceTempView("sex_ages")

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        sex_sido = get_spark_session().sql("""select BUYER_SEX as SEX, sum(TOT) as BUY_TOT, SIDO as REGN
                                                from sex_ages inner join LOC on sex_ages.RES_REGN_CODE = LOC.LOC_CODE
                                                group by BUYER_SEX, SIDO 
                                                order by sex""")
        overwrite_trunc_data(DataMart, sex_sido, "ACC_SELL_BUY_SEX_SIDO")