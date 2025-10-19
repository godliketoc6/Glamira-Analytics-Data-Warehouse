#!/bin/bash

# Go to your project directory
cd /home/duyng/finalprj

# Wait until PostgreSQL (in Docker) is ready
echo "⏳ Waiting for PostgreSQL to start..."
until docker exec postgres pg_isready -U postgres > /dev/null 2>&1; do
  sleep 3
done

echo "✅ PostgreSQL is up! Running crawler..."

# Activate your virtual environment
source venv/bin/activate

# Run the crawler script using your venv Python
python /home/duyng/finalprj/helper/crawl.py >> crawl_log.txt 2>&1

echo "🎉 Crawler finished running!"
