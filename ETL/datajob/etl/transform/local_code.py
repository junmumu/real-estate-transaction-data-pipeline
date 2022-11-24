import json
from pyspark.sql.functions import col, split, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session
from infra.logger import get_logger


class LocalCodeTransformer:
    FILE_NAME = '/real_estate/local_code/local_code.csv'

    @classmethod
    def transform(cls):
        try:
            df_local = cls.__read_csv_file()   
    
            df_local = cls.__select_columns(df_local)

            localnames = df_local.select('name').collect()  # collect를 이용해 데이터프레임 한 열을 리스트로 생성
            
            sido_list, sigungu_list, i = cls.__process_address_data(localnames)

            df_localnames = cls.__list_to_df(sido_list, sigungu_list)

            df_local = cls.__join_df(df_local, df_localnames)

            df_local.show()
            print(df_local.count())
            
            # DW에 저장
            save_data(DataWarehouse, df_local, 'LOC')
        except Exception as e:
            log_dict = cls.__create_log_dict()
            cls.__dump_log(log_dict, e)

    # 두 데이터프레임을 합하기 위해서 두 DF에서 가상으로 idx만든다음, 그 idx로 join
    @classmethod
    def __join_df(cls, df_local, df_localnames):
        df_local = df_local.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
        df_localnames = df_localnames.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
        df_local = df_local.join(df_localnames, on=["row_index"]).drop("row_index", "name")
        df_local = df_local.distinct()
        return df_local

    #  시도, 시군구 리스트를 데이터프레임으로 생성
    @classmethod
    def __list_to_df(cls, sido_list, sigungu_list):
        sido_sigungu_list = zip(sido_list, sigungu_list)
        columns = ["sido", "sigungu"]
        df_localnames = get_spark_session().createDataFrame(data=sido_sigungu_list, schema=columns)
        df_localnames.show(3)
        return df_localnames


    # 전체주소명데이터를 가공해서 시도, 시군구 리스트 각각에 집어넣음
    @classmethod
    def __process_address_data(cls, localnames):
        sido_list = []
        sigungu_list = []
        for i in range(len(localnames)):
            if localnames[i][0][-1][-1] in ('읍', '면', '동', '가', '로'):
                tmp = localnames[i][0][:-1]
                cls.__construct_list_sido_sigungu(sido_list, sigungu_list, tmp)
            elif localnames[i][0][-1][-1] in ('리', ')'):
                tmp = localnames[i][0][:-2]
                cls.__construct_list_sido_sigungu(sido_list, sigungu_list, tmp)
            elif localnames[i][0][-1][-1] in ('군', '구', '시', '도'):
                tmp = localnames[i][0]
                cls.__construct_list_sido_sigungu(sido_list, sigungu_list, tmp)
        return sido_list, sigungu_list,i

    # 컬럼 선택
    @classmethod
    def __select_columns(cls, df_local):
        df_local = df_local.select(col('지역코드').substr(0, 5).alias('loc_code'), col('시도코드').alias('sido_code'), 
                                        col('시군구코드').alias('sigungu_code'), split(col('지역주소명'), '[\s]', -1).alias('name'))                    
        return df_local

    # HDFS에서 CSV파일 읽어오기
    @classmethod
    def __read_csv_file(cls):
        df_local = get_spark_session().read.csv(cls.FILE_NAME, encoding='CP949', header=True)
        return df_local

    # 시도, 시군구 리스트에 집어넣기
    @classmethod
    def __construct_list_sido_sigungu(cls, sido_list, sigungu_list, tmp):
        if len(tmp) == 3:
            sido_list.append(tmp[0])
            sigungu_list.append(' '.join([tmp[1], tmp[2]]))
        elif len(tmp) == 2:
            sido_list.append(tmp[0])
            sigungu_list.append(tmp[1])
        elif len(tmp) == 1:
            sido_list.append(tmp[0])
            sigungu_list.append('')

    # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('local_code_transform').error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls, loc_code):
        log_dict = {
                "is_success": "Fail",
                "type": "local_code_transform",
                "loc_code": loc_code
            }
        return log_dict
