#!/usr/bin/env python3
import sys
import os
import gzip
import re
from datetime import datetime
import uuid
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

# ---------- Regular expression for log parsing ----------
LOG_PATTERN = re.compile(
    r'(?P<host>\S+) - - \[(?P<datetime>[^\]]+)\] "(?:GET|POST) (?P<path>\S+) [^"]*" \d+ (?P<bytes>\S+)'
)

# ---------- Function to convert datetime string ----------
def parse_datetime(dt_str):
    # "01/Jul/1995:00:00:01 -0400" → convert to a Python datetime object
    return datetime.strptime(dt_str.split()[0], "%d/%b/%Y:%H:%M:%S")

# ---------- Function to convert bytes field ----------
def parse_bytes(b):
    # Handle "-" or zero values
    try:
        return int(b)
    except ValueError:
        return 0

# ---------- Main function ----------
def main():
    input_dir, keyspace, table = sys.argv[1], sys.argv[2], sys.argv[3]

    # Connect to the Cassandra cluster (reliable cluster)
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)

    # Prepare the INSERT statement
    insert_stmt = session.prepare(
        f"INSERT INTO {table} (host, id, datetime, path, bytes) VALUES (?, ?, ?, ?, ?)"
    )

    # Iterate through all files in the input directory
    for file_name in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file_name)
        print(f"Processing: {file_path}")

        # Open each gzip-compressed log file
        with gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore') as logfile:
            batch = BatchStatement()
            count = 0

            for line in logfile:
                match = LOG_PATTERN.match(line)
                if not match:
                    continue  # skip lines that don't match the expected format

                host = match.group("host")
                dt = parse_datetime(match.group("datetime"))
                path = match.group("path")
                bytes_transferred = parse_bytes(match.group("bytes"))
                uid = uuid.uuid4()

                # Add the record to the batch
                batch.add(insert_stmt, (host, uid, dt, path, bytes_transferred))
                count += 1

                # Execute the batch every 100 rows for better performance
                if count % 100 == 0:
                    session.execute(batch)
                    batch = BatchStatement()

            # Execute any remaining inserts
            if len(batch) > 0:
                session.execute(batch)

        print(f"Inserted {count} rows from {file_name}")

    # Close the Cassandra cluster connection
    cluster.shutdown()

if __name__ == "__main__":
    main()
