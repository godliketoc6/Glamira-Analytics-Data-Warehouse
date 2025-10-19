#!/bin/bash

# Go to project folder
cd /home/duyng/finalprj

# Wait until PostgreSQL (in Docker) is ready
echo "⏳ Waiting for PostgreSQL to start..." >> merge_log.txt
until docker exec postgres pg_isready -U postgres > /dev/null 2>&1; do
  sleep 3
done

echo "✅ PostgreSQL is up! Running merge script..." >> merge_log.txt

# Activate Python virtual environment
source venv/bin/activate

# Run merge script
python /home/duyng/finalprj/helper/merge_tables.py >> merge_log.txt 2>&1

echo "🎉 Merge job completed at $(date)" >> merge_log.txt
