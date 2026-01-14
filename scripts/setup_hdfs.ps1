# HDFS Setup Script for Procurement Pipeline (Windows)

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "HDFS Setup for Procurement Pipeline" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

# Wait for namenode to be ready
Write-Host "Waiting for HDFS namenode to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Create HDFS directory structure
Write-Host "Creating HDFS directory structure..." -ForegroundColor Yellow

$directories = @(
    "/procurement/raw/orders",
    "/procurement/raw/stock",
    "/procurement/processed/aggregated_orders",
    "/procurement/processed/net_demand",
    "/procurement/output/supplier_orders",
    "/procurement/logs/exceptions"
)

foreach ($dir in $directories) {
    docker exec hadoop_client hadoop fs -mkdir -p $dir
    Write-Host "  Created: $dir" -ForegroundColor Green
}

Write-Host "`n✓ HDFS directories created" -ForegroundColor Green

# Set permissions
Write-Host "Setting HDFS permissions..." -ForegroundColor Yellow
docker exec hadoop_client hadoop fs -chmod -R 777 /procurement

Write-Host "✓ Permissions set" -ForegroundColor Green

# Verify structure
Write-Host "`nHDFS directory structure:" -ForegroundColor Cyan
docker exec hadoop_client hadoop fs -ls -R /procurement

Write-Host "`n=========================================" -ForegroundColor Cyan
Write-Host "✓ HDFS setup complete!" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Cyan
