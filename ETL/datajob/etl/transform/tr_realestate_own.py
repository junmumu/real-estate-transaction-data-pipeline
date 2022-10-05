import json
from pyspark.sql.types import *
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from infra.logger import get_logger
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day,cal_std_month

#월 리스트 생성
deal_y = ['201'+str(i) for i in range(7,10)]
deal_y.append('2020')
deal_y.append('2021')
deal_y.append('2022')

deal_ymd = []
for year in deal_y:
        for m in range(1,13):
            if m < 10:
                m = str(0) + str(m)
            if year+str(m) > '202209' :
                break
            deal_ymd.append(year+str(m))


print(deal_ymd)

class TrRealEstateOwnTransformer:
    FILE_DIR ='/project/'
    @classmethod
    def transform(cls):

        # DW에서 지역코드 불러오기
        df_loc = find_data(DataWarehouse, 'LOC')
        loc_code = df_loc.select(['SIDO','LOC_CODE']).collect()
        df_loc_code = get_spark_session().createDataFrame(loc_code)

        for i in range(len(deal_ymd)):
            try:
                # extract데이터 불러오기
                path='/project/realestate_own'+ deal_ymd[i] + '.json'
                realestate=get_spark_session().read.json(path,encoding='utf-8')
                tmp2 = realestate.first()
                realestate_df=get_spark_session().createDataFrame(tmp2[0])

                #칼럼명 dw와 동일하게 변경
                df2_realestate=realestate_df.select(
                realestate_df.admin_regn1_name.alias('REGN_CODE1'),
                realestate_df.admin_regn2_name.alias('REGN_CODE2'),
                realestate_df.age .alias('OWNER_AGES'),
                realestate_df.appl_nomprs_num.alias('TOT'),
                realestate_df.bs_ym.alias('RES_DATE'),
                realestate_df.cd_name.alias('PROPERTY_TYPE'),
                realestate_df.enr_no_cls_cd_name.alias('OWNER_CLS'),
                realestate_df.rgs_aim_cd.alias('PURPOSE'),
                realestate_df.sex.alias('OWNER_SEX'),)

                # sido명으로 조인
                #LOC테이블과 join
                cond = [df2_realestate.REGN_CODE1 == df_loc.SIDO, df2_realestate.REGN_CODE2 == df_loc.SIGUNGU]
                df_real_loc=df2_realestate.join(df_loc, cond,'inner') \
                .select(df2_realestate.RES_DATE,df_loc.LOC_CODE.alias('REGN_CODE'),df2_realestate.PROPERTY_TYPE,df2_realestate.PURPOSE,df2_realestate.OWNER_CLS,df2_realestate.OWNER_SEX,df2_realestate.OWNER_AGES,df2_realestate.TOT.cast('int'))


                # save in DW
                save_data(DataWarehouse, df_real_loc, 'REALESTATE_OWN')

            except Exception as e:
                    log_dict = cls.__create_log_dict()
                    cls.__dump_log(log_dict, e)

    # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('realestate_own').error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls):
        log_dict = {
                "is_success": "Fail",
                "type": "realestate_own"
            }
        return log_dict       \


