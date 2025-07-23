# Retail Analytics - YARN-based Spark Cluster

A comprehensive data analytics platform built with Apache Spark on YARN, featuring HDFS storage, S3A integration, Apache Iceberg, and PostgreSQL catalog support.

## 🏗️ Architecture

This project uses a **YARN-based Spark cluster** instead of standalone Spark for better resource management and enterprise-grade scalability.

### Core Components

- **🐘 Hadoop HDFS**: Distributed file system for data storage
- **🧶 YARN**: Resource manager for dynamic resource allocation
- **⚡ Apache Spark**: Distributed computing engine
- **🗄️ PostgreSQL**: Iceberg catalog and metadata storage
- **☁️ LocalStack**: S3-compatible storage for development
- **🧊 Apache Iceberg**: Table format for analytics workloads

### Container Services

| Service | Container | Port | Description |
|---------|-----------|------|-------------|
| NameNode | `namenode` | 9870, 9000 | HDFS primary node |
| DataNode | `datanode` | 9864 | HDFS data storage |
| ResourceManager | `resourcemanager` | 8088, 8032 | YARN resource management |
| NodeManager | `nodemanager` | 8042 | YARN node management |
| HistoryServer | `historyserver` | 8188 | Spark job history |
| Spark Client | `spark-client` | - | Spark job submission |
| PostgreSQL | `postgres` | 5432 | Iceberg catalog |
| LocalStack | `localstack` | 4566 | S3-compatible storage |

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.10+ with pipenv
- Make utility

### 1. Start the Cluster

```bash
# Automated setup (recommended)
./setup-yarn.sh

# Or manual setup
make up
```

### 2. Initialize HDFS

```bash
make init-hdfs
```

### 3. Build and Deploy Your Application

```bash
# Build Python package
make build-package

# Upload to HDFS
make build-and-upload
```

### 4. Submit a Spark Job

```bash
# Submit to YARN cluster
make spark-submit-yarn MODULE_NAME=src/examples/sample.py

# Or submit with local files (client mode)
make spark-submit-yarn-local MODULE_NAME=src/examples/sample.py
```

## 📊 Monitoring & UI Access

| Service | URL | Description |
|---------|-----|-------------|
| YARN ResourceManager | http://localhost:8088 | Cluster resources & applications |
| HDFS NameNode | http://localhost:9870 | File system browser |
| Spark History Server | http://localhost:8188 | Spark job history |
| PostgreSQL | localhost:5432 | Database (user: root_ai, password: root_pwd) |

## 🛠️ Available Commands

### Cluster Management
```bash
make up                      # Start all containers
make down                    # Stop all containers
make build                   # Build Docker images
```

### Development
```bash
make build-package          # Build Python package
make build-and-upload       # Build package and upload to HDFS
make init-hdfs              # Initialize HDFS directories
```

### Job Submission
```bash
make spark-submit-yarn      # Submit job to YARN (cluster mode)
make spark-submit-yarn-local # Submit job to YARN (client mode)
```

### Monitoring
```bash
make check-hdfs             # Check HDFS status and files
make check-yarn             # Check YARN cluster status
make yarn-logs APPLICATION_ID=<app_id> # Get application logs
make yarn-ui                # Display monitoring URLs
```

### Interactive Sessions
```bash
make start-client-session   # Start Spark client shell
make start-yarn-session     # Start YARN ResourceManager shell
make start-namenode-session # Start NameNode shell
make sql-shell              # Start Spark SQL shell
```

### Utilities
```bash
make help                   # Show all available commands
```

## 📁 Project Structure

```
retail-analytics/
├── config/
│   ├── hadoop/             # Hadoop/YARN configuration
│   │   ├── core-site.xml
│   │   ├── hdfs-site.xml
│   │   ├── yarn-site.xml
│   │   └── mapred-site.xml
│   └── spark/              # Spark configuration
│       └── spark-defaults.conf
├── src/
│   ├── examples/           # Sample Spark applications
│   ├── utilities/          # Spark utilities and helpers
│   ├── jobs/               # ETL and data processing jobs
│   └── config/             # Application configuration
├── dist/                   # Built packages and archives
├── scripts/                # Database and setup scripts
├── docker-compose.yml      # YARN cluster definition
├── Dockerfile.spark-client # Custom Spark client image
├── Makefile               # Build and deployment commands
├── setup-yarn.sh          # Automated cluster setup
└── requirements.txt       # Python dependencies
```

## ⚙️ Configuration

### Spark Configuration

Key Spark settings in `config/spark/spark-defaults.conf`:

```properties
# YARN Configuration
spark.master yarn
spark.submit.deployMode cluster
spark.yarn.am.memory 1g
spark.yarn.am.cores 1

# Resource Allocation
spark.executor.memory 1g
spark.executor.cores 1
spark.executor.instances 2

# Python Environment
spark.yarn.appMasterEnv.PYSPARK_PYTHON ./venv/bin/python
spark.yarn.executorEnv.PYSPARK_PYTHON ./venv/bin/python

# S3A Configuration (LocalStack)
spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint http://localstack:4566
spark.hadoop.fs.s3a.access.key test
spark.hadoop.fs.s3a.secret.key test

# Iceberg Catalog (PostgreSQL)
spark.sql.catalog.data_catalog org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.data_catalog.catalog-impl org.apache.iceberg.jdbc.JdbcCatalog
spark.sql.catalog.data_catalog.warehouse s3a://my-bucket/iceberg_catalog/warehouse/
spark.sql.catalog.data_catalog.uri jdbc:postgresql://postgres:5432/iceberg_catalog
```

### Environment Variables

Key variables in `.env`:

```properties
# Container Names
NAMENODE_CONTAINER_NAME=namenode
RESOURCEMANAGER_CONTAINER_NAME=resourcemanager
SPARK_CLIENT_CONTAINER_NAME=spark-client

# Database Configuration
POSTGRES_USER=root_ai
POSTGRES_PASSWORD=root_pwd
POSTGRES_DB=retail_analytics

# YARN Configuration
YARN_RESOURCEMANAGER_HOSTNAME=resourcemanager
HADOOP_NAMENODE_HOSTNAME=namenode
```

## 🔄 Migration from Standalone Spark

If migrating from standalone Spark:

### What Changed
- ❌ **Removed**: `spark-master`, `spark-worker-*` containers
- ✅ **Added**: Hadoop ecosystem (NameNode, DataNode, ResourceManager, NodeManager)
- ✅ **Updated**: Spark client with YARN support
- ✅ **Added**: HDFS for distributed storage

### Code Changes Required
- Update SparkSession creation to use YARN
- File paths now use HDFS URLs (`hdfs://namenode:9000/...`)
- Job submission through YARN instead of Spark standalone

## 📦 Package Management

### Building Virtual Environments

```bash
# Create virtual environment with pyenv
pyenv local 3.10.18
python -m venv venv --clear
./venv/bin/pip install -r requirements.txt

# Create distributable archive
cd venv && zip -r ../dist/venv.zip . -x "*.pyc" "__pycache__/*"
```

### Deploying to YARN

Archives are automatically distributed to all executors:

```bash
--archives hdfs://namenode:9000/shared/dist/venv.tar.gz#venv
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv/bin/python
```

## 🧪 Development Workflow

1. **Develop locally** using the Spark client container
2. **Test with small datasets** using client mode submission
3. **Deploy to cluster** using cluster mode for production jobs
4. **Monitor progress** through YARN UI and Spark History Server

### Example Development Session

```bash
# Start development environment
make up
make start-client-session

# Inside container - interactive development
pyspark --master yarn --deploy-mode client

# Test job submission
make spark-submit-yarn-local MODULE_NAME=src/examples/test.py

# Deploy to cluster
make spark-submit-yarn MODULE_NAME=src/jobs/production_etl.py
```

## 🔍 Troubleshooting

### Common Issues

1. **HDFS Connection Errors**
   ```bash
   make check-hdfs  # Verify HDFS is running
   ```

2. **YARN Resource Issues**
   ```bash
   make check-yarn  # Check available resources
   ```

3. **Application Logs**
   ```bash
   make yarn-logs APPLICATION_ID=application_xxx
   ```

4. **Container Health**
   ```bash
   docker-compose ps  # Check container status
   ```

### Performance Tuning

- Adjust executor memory: `spark.executor.memory`
- Tune parallelism: `spark.executor.instances`
- Configure S3A: `spark.hadoop.fs.s3a.*`
- YARN resources: `yarn.nodemanager.resource.*`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Test with the YARN cluster
4. Submit a pull request

## 📜 License

This project is licensed under the MIT License.

---

**🎯 Next Steps:**
- Explore the example jobs in `src/examples/`
- Configure additional data sources
- Set up CI/CD pipelines
- Scale the cluster by adding more NodeManagers

For detailed documentation on specific components, see the individual configuration files and inline comments.