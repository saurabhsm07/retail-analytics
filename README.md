**RUN below command to submit the spark job:**

spark-submit --master local[1] --py-files packages.zip --files config/preprocess_config.json main.py 

**RUN below command to submit the HD insight:**

spark-submit --master yarn --deploy-mode client --py-files packages.zip  --files config/preprocess_config.json main.py


ex:
spark-submit --master yarn-cluster --class com.microsoft.spark.application --num-executors 4 --executor-memory 4g --executor-cores 2 --driver-memory 8g --driver-cores 4 /home/user/spark/sparkapplication.jar

