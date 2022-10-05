from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, save_data
from infra.spark_session import get_spark_session

class AccSellBuyAges:
    @classmethod
    def save(cls):
        sex_ages = find_data(DataWarehouse, 'OWN_SEX_AGE')
        sex_ages.createOrReplaceTempView("sex_ages")
        ages = get_spark_session().sql("""select BUYER_AGES as AGES, sum(TOT) as BUY_TOT ,
                                            round((sum(TOT)/(select sum(TOT) from sex_ages)*100),1) as BUY_RATE
                                            from sex_ages group by BUYER_AGES""")
        #save_data(DataMart, ages, "ACC_SELL_BUY_AGES")
        overwrite_data(DataMart, ages, "ACC_SELL_BUY_AGES")


class SellBuyAgesYear:
    @classmethod
    def save(cls):
        sex_ages = find_data(DataWarehouse, 'OWN_SEX_AGE')
        sex_ages.createOrReplaceTempView("sex_ages")
        ages_year = get_spark_session().sql("""select BUYER_AGES as AGES, SUM(TOT) AS BUY_TOT,
                                                (select year(res_date) from sex_ages group by year(res_date)) as YEAR
                                                from sex_ages group by BUYER_AGES""")
        #save_data(DataMart, ages_year, "SELL_BUY_AGES_YEAR")
        overwrite_data(DataMart, ages_year, "SELL_BUY_AGES_YEAR")


class AccSellBuyAgesSido:
    @classmethod
    def save(cls):
        sex_ages = find_data(DataWarehouse, 'OWN_SEX_AGE')
        sex_ages.createOrReplaceTempView("sex_ages")
        ages_sido = get_spark_session().sql("""select BUYER_AGES as AGES, sum(TOT) as BUY_TOT , RES_REGN_CODE as REGN
                                                from sex_ages group by BUYER_AGES, RES_REGN_CODE order by BUYER_AGES""")
        #save_data(DataMart, ages_sido, "ACC_SELL_BUY_AGES_SIDO")
        overwrite_data(DataMart, ages_sido, "ACC_SELL_BUY_AGES_SIDO")




