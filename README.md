**RUN below command to submit the spark job:**

steps to submit the spark job
build build_dependencies.sh

spark-submit --master local[1] --py-files packages.zip --files config/preprocess_config.json main.py 
