

from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.spark_session import get_spark_session

class OwnRegistType:

    @classmethod
    def save(cls):
        df = find_data(DataWarehouse, "REALESTATE_OWN")
        df.createOrReplaceTempView('REALESTATE_OWN')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        #전국
        df_regi_type=get_spark_session().sql('''select owner_cls as cls,count(owner_cls)as tot
                                            ,round((count(owner_cls)/(select count(owner_cls) from REALESTATE_OWN)*100),2) as rate
                                            from REALESTATE_OWN
                                            group by owner_cls''')
        #서울
        df_seoul_regi_type=get_spark_session().sql('''select owner_cls as cls,count(owner_cls)
                                            as tot,round((count(owner_cls)/(select count(owner_cls) from REALESTATE_OWN)*100),2) as rate
                                            from REALESTATE_OWN ro
                                            inner join loc l
                                            on l.loc_code=ro.regn_code
                                            where sido='서울특별시'
                                            group by owner_cls''')
        

                                            
        save_data(DataMart,df_regi_type,'OWN_REGIST_TYPE')
        save_data(DataMart,df_seoul_regi_type,'SEOUL_OWN_REGIST_TYPE')
