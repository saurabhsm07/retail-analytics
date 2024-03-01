# retail-app

#### Analytics App designed majorly to implement and test Data engineering concepts (mostly big data) components and tech (mostly spark).



### SETUP:

- Install Docker, for windows installation follow:
    - docker installation steps: https://docs.docker.com/desktop/install/windows-install/
    - Windows subsystem for Linux: https://learn.microsoft.com/en-us/windows/wsl/install
 
  
  
### INITIALIZATION:  

- Run command `docker-compose up --build -d` or any of its variant approaches to build and start the spark cluster and other services.
- Run a module script using command `docker-compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 <python_script_path>.py`
  - Where `python_script_path.py` is relative to src directory in the repository.
- To start a PYSPARK shell `docker-compose exec spark-master /opt/spark/bin/pyspark`

### Running Test Suite:  
N/A

### ISSUES:
- Only submit jobs without any dependencies.
- Support for additional JARs, python packages not implemented.
- Only modules present in `src/etl/` package are runnable at this point. Other modules are not migrated. 
- Legacy project migration in progress. Some components will fail. For not this is just a simple cluster setup with postgres and S3 support.

### UPDATES:

N/A

### Workarounds:

 N/A