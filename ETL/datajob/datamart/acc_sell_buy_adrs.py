from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session


class AccSellBuyAdrs:
    @classmethod
    def save(cls):
        df_own_addr = find_data(DataWarehouse, "OWN_ADDR")
        df_own_addr.createOrReplaceTempView('OWN_ADDR')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_fin = get_spark_session().sql('''SELECT S1.REGN, SELL_TOT, SELL_RATE, BUY_TOT, BUY_RATE
                                            FROM
                                                (SELECT SIDO AS REGN, SUM(TOT) AS SELL_TOT, ROUND((SUM(TOT) / (SELECT SUM(TOT) FROM OWN_ADDR) * 100), 2) AS SELL_RATE
                                                FROM OWN_ADDR INNER JOIN LOC ON OWN_ADDR.RES_REGN_CODE = LOC.LOC_CODE
                                                GROUP BY SIDO) S1,
                                                (SELECT SIDO AS REGN, SUM(TOT) AS BUY_TOT, ROUND((SUM(TOT) / (SELECT SUM(TOT) FROM OWN_ADDR) * 100), 2) AS BUY_RATE
                                                FROM OWN_ADDR INNER JOIN LOC ON OWN_ADDR.BUYER_REGN_CODE = LOC.LOC_CODE
                                                GROUP BY SIDO) S2
                                            WHERE S1.REGN = S2.REGN''')
        df_fin.show()

        save_data(DataMart, df_fin, "ACC_SELL_BUY_ADRS")