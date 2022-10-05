from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session


class SellBuySudoYear:
    @classmethod
    def save(cls):
        df_own_addr = find_data(DataWarehouse, "OWN_ADDR")
        df_own_addr.createOrReplaceTempView('OWN_ADDR')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_fin = get_spark_session().sql('''SELECT S1.SIDO AS REGN, S1.YEAR, SELL_TOT, BUY_TOT
                                        FROM (SELECT SIDO, EXTRACT(YEAR FROM RES_DATE) AS YEAR, SUM(TOT) AS SELL_TOT
                                            FROM OWN_ADDR INNER JOIN LOC ON OWN_ADDR.RES_REGN_CODE = LOC.LOC_CODE
                                            WHERE SIDO IN ('서울특별시', '인천광역시', '경기도')
                                            GROUP BY SIDO, EXTRACT(YEAR FROM RES_DATE)) S1,
                                            (SELECT SIDO, EXTRACT(YEAR FROM RES_DATE) AS YEAR, SUM(TOT) AS BUY_TOT
                                            FROM OWN_ADDR INNER JOIN LOC ON OWN_ADDR.BUYER_REGN_CODE = LOC.LOC_CODE
                                            WHERE SIDO IN ('서울특별시', '인천광역시', '경기도')
                                            GROUP BY SIDO, EXTRACT(YEAR FROM RES_DATE)) S2
                                        WHERE S1.SIDO = S2.SIDO AND S1.YEAR = S2.YEAR''')
    
        df_fin.show()

        save_data(DataMart, df_fin, "SELL_BUY_SUDO_YEAR")