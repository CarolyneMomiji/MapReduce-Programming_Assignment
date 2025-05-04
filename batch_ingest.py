import happybase
from hdfs import InsecureClient

# HBase Connection
try:
    connection = happybase.Connection('localhost', port=9090)
    connection.open()
    print("Successfully connected to HBase.")
except Exception as e:
    print(f"Error connecting to HBase: {e}")
    exit()

# Access HBase table
try:
    table_name = 'yellow_tripdata'
    table = connection.table(table_name)
    print(f"Successfully accessed the table: {table_name}")
except Exception as e:
    print(f"Error accessing the table '{table_name}': {e}")
    connection.close()
    exit()

# Connect to WebHDFS (Replace localhost with your HDFS Namenode hostname)
HDFS_NAMENODE = "ip-172-31-87-189.ec2.internal"  # Use your namenode address
HDFS_PORT = "9870"  # Default WebHDFS port

try:
    hdfs_client = InsecureClient(f'http://{HDFS_NAMENODE}:{HDFS_PORT}', user='hdfs')
    print(f"Successfully connected to HDFS at http://{HDFS_NAMENODE}:{HDFS_PORT}")
except Exception as e:
    print(f"Error connecting to HDFS: {e}")
    connection.close()
    exit()

# Function to ingest data from a single HDFS file
def batch_ingest_from_hdfs(hdfs_path):
    try:
        with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
            for line in reader:
                fields = line.strip().split(',')
                
                # Validate number of fields
                if len(fields) < 17:
                    print(f"Skipping line due to insufficient fields: {line}")
                    continue

                # Construct the row key
                row_key = f"{fields[0]}_{fields[1]}"  # VendorID + tpep_pickup_datetime
                
                # Prepare data to insert
                data = {
                    b'trip:passenger_count': fields[3].encode(),
                    b'trip:trip_distance': fields[4].encode(),
                    b'trip:RatecodeID': fields[5].encode(),
                    b'trip:store_and_fwd_flag': fields[6].encode(),
                    b'location:PULocationID': fields[7].encode(),
                    b'location:DOLocationID': fields[8].encode(),
                    b'payment:payment_type': fields[9].encode(),
                    b'payment:fare_amount': fields[10].encode(),
                    b'payment:extra': fields[11].encode(),
                    b'payment:mta_tax': fields[12].encode(),
                    b'payment:tip_amount': fields[13].encode(),
                    b'payment:tolls_amount': fields[14].encode(),
                    b'payment:improvement_surcharge': fields[15].encode(),
                    b'payment:total_amount': fields[16].encode(),
                }
                
                # Insert data into HBase
                try:
                    table.put(row_key, data)
                except Exception as e:
                    print(f"Error inserting row {row_key}: {e}")
            print(f"Successfully ingested data from {hdfs_path}")
    except Exception as e:
        print(f"Error processing file {hdfs_path}: {e}")

# Loop through all part files in the HDFS directory
hdfs_directory = '/user/hadoop/yellow_tripdata'
try:
    files = hdfs_client.list(hdfs_directory)
    for file in files:
        if file.startswith('part-m-'):
            batch_ingest_from_hdfs(f"{hdfs_directory}/{file}")
except Exception as e:
    print(f"Error listing files in HDFS directory '{hdfs_directory}': {e}")

# Close connections
connection.close()
print("HBase connection closed.")




