#!/bin/bash

echo "🚀 Starting YARN-based Spark cluster setup..."

# Step 1: Start the containers
echo "📦 Starting containers..."
docker-compose up -d

# Step 2: Wait for services to be ready
echo "⏳ Waiting for services to initialize..."
sleep 30

# Step 3: Check if NameNode is ready
echo "🔍 Checking NameNode status..."
for i in {1..10}; do
    if docker exec namenode hdfs dfs -ls / >/dev/null 2>&1; then
        echo "✅ NameNode is ready"
        break
    fi
    echo "⏳ Waiting for NameNode... (attempt $i/10)"
    sleep 10
done

# Step 4: Check if ResourceManager is ready
echo "🔍 Checking ResourceManager status..."
for i in {1..10}; do
    if docker exec resourcemanager yarn node -list >/dev/null 2>&1; then
        echo "✅ ResourceManager is ready"
        break
    fi
    echo "⏳ Waiting for ResourceManager... (attempt $i/10)"
    sleep 10
done

# Step 5: Initialize HDFS directories
echo "📁 Initializing HDFS directories..."
make init-hdfs

# Step 6: Show cluster status
echo "📊 Cluster Status:"
echo "NameNode UI: http://localhost:9870"
echo "ResourceManager UI: http://localhost:8088"
echo "History Server UI: http://localhost:8188"

echo "🎉 YARN cluster setup complete!"
echo ""
echo "Next steps:"
echo "1. Build your package: make build-package"
echo "2. Upload to HDFS: make init-hdfs"
echo "3. Submit a job: make spark-submit-yarn"
echo ""
echo "For help: make help"
