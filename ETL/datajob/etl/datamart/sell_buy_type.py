from infra.jdbc import DataMart, DataWarehouse, find_data, overwrite_data, save_data
from infra.spark_session import get_spark_session

class AccSellBuyType:
    @classmethod
    def save(cls):
        types = find_data(DataWarehouse, 'OWN_TYPE')
        types.createOrReplaceTempView("types")
        own_type = get_spark_session().sql("""select OWNER_CLS as CLS, sum(TOT) as BUY_TOT ,
                                                round((sum(TOT)/(select sum(TOT) from types)*100),1) as BUY_RATE
                                                from types group by OWNER_CLS""")
        save_data(DataMart, own_type, "ACC_SELL_BUY_TYPE")

class SellBuyTypeYear:
    @classmethod
    def save(cls):
        types = find_data(DataWarehouse, 'OWN_TYPE')
        types.createOrReplaceTempView("types")
        types_year = get_spark_session().sql("""select OWNER_CLS as CLS, SUM(TOT) AS BUY_TOT,
                                                (select year(res_date) from types group by year(res_date)) as YEAR
                                                from types group by OWNER_CLS""")
        save_data(DataMart, types_year, "SELL_BUY_TYPE_YEAR")


class AccSellBuyTypeSido:
    @classmethod
    def save(cls):
        types = find_data(DataWarehouse, 'OWN_TYPE')
        types.createOrReplaceTempView("types")
        type_sido = get_spark_session().sql("""select OWNER_CLS as CLS, sum(TOT) as BUY_TOT , RES_REGN_CODE as REGN
                                                from types group by OWNER_CLS, RES_REGN_CODE order by CLS""")
        save_data(DataMart, type_sido, "ACC_SELL_BUY_TYPE_SIDO")
