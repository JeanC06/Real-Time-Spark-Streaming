import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F

def asignarCita(df):
    df.createOrReplaceTempView("vResultado")
    citas_asignadas = spark.sql("""
        SELECT paciente, edad, estado_afiliacion, tipo_afiliacion, 
            CASE 
                WHEN estado_afiliacion = 'Afiliado' THEN 
                    CASE 
                        WHEN tipo_afiliacion = 'Premium' THEN 'Cita asignada será mañana'
                        WHEN tipo_afiliacion = 'Tipo A' THEN 'Cita asignada será en 2 dias'
                        WHEN tipo_afiliacion = 'Tipo B' THEN 'Cita asignada será en 4 dias'
                        WHEN tipo_afiliacion = 'Tipo C' THEN 'Cita asignada sera en 5 dias'
                    END
                ELSE 'Cita denegada'
            END AS estado_cita
        FROM vResultado
    """)
    return citas_asignadas

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaIntegration") \
        .master("local[3]") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    tiposStreamingDF = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092") \
        .option("subscribe", "citas_medicas") \
        .option("startingOffsets", "earliest") \
        .load()

    esquema = StructType([
        StructField("paciente", StringType()),
        StructField("edad", StringType()),
        StructField("estado_afiliacion", StringType()),
        StructField("tipo_afiliacion", StringType())
    ])

    parsedDF = tiposStreamingDF \
        .select("value") \
        .withColumn("value", F.col("value").cast(StringType())) \
        .withColumn("input", F.from_json(F.col("value"), esquema)) \
        .withColumn("paciente", F.col("input.paciente")) \
        .withColumn("edad", F.col("input.edad")) \
        .withColumn("estado_afiliacion", F.col("input.estado_afiliacion")) \
        .withColumn("tipo_afiliacion", F.col("input.tipo_afiliacion"))

    citasAsignadasDF = asignarCita(parsedDF)


    query = citasAsignadasDF.writeStream \
        .queryName("query_citas_kafka") \
        .format("console") \
        .outputMode("append") \
        .start()
    

    query.awaitTermination()

    from time import sleep
    for x in range(50):
        spark.sql("select * from query_citas_kafka").show(1000, False)
        sleep(1)

    
    print('llego hasta aqui')
