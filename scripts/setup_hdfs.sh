#!/bin/bash

# HDFS Setup Script for Procurement Pipeline

echo "========================================="
echo "HDFS Setup for Procurement Pipeline"
echo "========================================="

# Wait for namenode to be ready
echo "Waiting for HDFS namenode to be ready..."
sleep 10

# Create HDFS directory structure
echo "Creating HDFS directory structure..."

docker exec hadoop_client hadoop fs -mkdir -p /procurement/raw/orders
docker exec hadoop_client hadoop fs -mkdir -p /procurement/raw/stock
docker exec hadoop_client hadoop fs -mkdir -p /procurement/processed/aggregated_orders
docker exec hadoop_client hadoop fs -mkdir -p /procurement/processed/net_demand
docker exec hadoop_client hadoop fs -mkdir -p /procurement/output/supplier_orders
docker exec hadoop_client hadoop fs -mkdir -p /procurement/logs/exceptions

echo "✓ HDFS directories created"

# Set permissions
echo "Setting HDFS permissions..."
docker exec hadoop_client hadoop fs -chmod -R 777 /procurement

echo "✓ Permissions set"

# Verify structure
echo ""
echo "HDFS directory structure:"
docker exec hadoop_client hadoop fs -ls -R /procurement

echo ""
echo "========================================="
echo "✓ HDFS setup complete!"
echo "========================================="
