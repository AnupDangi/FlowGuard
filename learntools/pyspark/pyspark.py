## Basics
from pyspark.sql import SparkSession

## we create a spark session which is entrypoint with session name,master to use all the cores of cpu.
spark=SparkSession.builder.appName("Basics")\
    .master("local[*]")\
    .getOrCreate()


## create the dateframe 
data=[
    (1,"anup",23),
    (2,"Bishal",24),
    (3,"Ram",25)
]

columns=["id","name","age"]

df=spark.createDataFrame(data,columns)

