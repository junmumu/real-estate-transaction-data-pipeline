

from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.spark_session import get_spark_session

class OwnRegistAge:

    @classmethod
    def save(cls):
        df = find_data(DataWarehouse, "REALESTATE_OWN")
        df.createOrReplaceTempView('REALESTATE_OWN')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')        

        df_age=get_spark_session().sql('''select OWNER_AGES as ages,count(OWNER_AGES)as tot
                                                ,round((count(OWNER_AGES)/(select count(OWNER_AGES) from REALESTATE_OWN)*100),2) as rate
                                                from REALESTATE_OWN
                                                group by OWNER_AGES''')

        df_seoul_age=get_spark_session().sql('''select OWNER_AGES as ages,count(OWNER_AGES)as tot
                                            ,round((count(OWNER_AGES)/(select count(OWNER_AGES) from REALESTATE_OWN)*100),2) as rate
                                            from REALESTATE_OWN ro
                                            inner join loc l
                                            on l.loc_code=ro.regn_code
                                            where sido='서울특별시'
                                            group by OWNER_AGES''')
                                            
        save_data(DataMart,df_age,'AGES_REGIST')
        save_data(DataMart,df_seoul_age,'SEOUL_AGES_REGIST')
