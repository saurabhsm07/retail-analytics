**RUN below command to submit the spark job:**

spark-submit --master local[1] --py-files packages.zip --files config/preprocess_config.json main.py 
