
import sys; sys.path.insert(0, '.')
from streaming.spark_config import get_spark_session
from pyspark.sql import functions as F
spark = get_spark_session('CheckSilver')

df = spark.read.format('delta').load('/tmp/pulsetrack-lakehouse/silver/sensor_readings')

print('=== Schema ===')
df.printSchema()

print('\n=== Rows per device type ===')
df.groupBy('device_type').count().show()

print('\n=== Rows per metric ===')
df.groupBy('metric_name').count().orderBy('count', ascending=False).show()

print('\n=== Invalid readings ===')
df.groupBy('is_valid').count().show()

print('\n=== Late arriving ===')
df.groupBy('is_late_arriving').count().show()

print('\n=== Sample smartwatch explosion ===')
df.filter(F.col('device_type') == 'smartwatch') \
  .select('device_id','metric_name','metric_value','is_valid') \
  .show(5, truncate=False)

spark.stop()
