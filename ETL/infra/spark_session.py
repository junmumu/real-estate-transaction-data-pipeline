import findspark  # python 실행 시 spark객체를 알아서 찾기 위한 라이브러리
from pyspark.sql import SparkSession
from pyspark import SparkConf

def get_spark_session():
    findspark.init()  # 앱에서 spark를 찾기 위한 진입점 호출
    conf = SparkConf().setAppName("real_estate")\
                    .setMaster("local[*]") \
                    .set('spark.default.parallelism', '12') \
                    .set('spark.sql.shuffle.partitions', '12')
    return SparkSession.builder.config(conf=conf).getOrCreate() # SparkSession객체 반환 / 이미 있으면 그냥 가져오고(get), 없으면 새로만들어서(create) 반환
