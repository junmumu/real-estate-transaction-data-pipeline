

from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.spark_session import get_spark_session

class OwnRegistSex:

    @classmethod
    def save(cls):
        df = find_data(DataWarehouse, "REALESTATE_OWN")
        df.createOrReplaceTempView('REALESTATE_OWN')

        df_loc = find_data(DataWarehouse, "LOC")
        df_loc.createOrReplaceTempView('LOC')

        df_sex=get_spark_session().sql('''select OWNER_SEX as sex,count(OWNER_SEX)as tot
                                                ,round((count(OWNER_SEX)/(select count(OWNER_SEX) from REALESTATE_OWN)*100),2) as rate
                                                from REALESTATE_OWN
                                                group by OWNER_SEX''')
            #서울
        df_seoul_sex_type=get_spark_session().sql('''select OWNER_SEX as sex,
                                                    count(OWNER_SEX)as tot,round((count(OWNER_SEX)/(select count(OWNER_SEX) from REALESTATE_OWN)*100),2) as rate
                                                    from REALESTATE_OWN ro
                                                    inner join loc l
                                                    on l.loc_code=ro.regn_code
                                                    where sido='서울특별시'
                                                    group by OWNER_SEX''')
                
                                            
        save_data(DataMart,df_sex,'SEX_REGIST')
        save_data(DataMart,df_seoul_sex_type,'SEOUL_SEX_REGIST')
