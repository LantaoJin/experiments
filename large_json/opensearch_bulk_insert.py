import json
import random
import string
import time
from datetime import datetime, timedelta
from opensearchpy import OpenSearch, helpers

# OpenSearch Configuration
OPENSEARCH_HOST =  "localhost"
OPENSEARCH_PORT = 9200
OPENSEARCH_INDEX = "mock-logs"
OPENSEARCH_USER = "admin"
OPENSEARCH_PASSWORD = ""

# Change below for diff exp
NUM_RECORDS = 10 # how many documents in one bulk insert
NUM_FIELDS = 100000 # how many sub-fields under `log` object field
INDEX_COUNT = 10 # how many indices will be created
READ_ROUND = 50 # how many times a simple search execute

# Mapping Mode Configuration
# 0: log field is an object, all sub-fields created by dynamic mapping
# 1: log field with enabled:false (no indexing and no dynamic mapping of log contents)
# 2: log field with dynamic_templates (just skip indexing for log.* fields)
MAPPING_MODE = 1

# Mapping definitions
MAPPINGS = {
    1: {
        "mappings": {
            "properties": {
                "log": {
                    "type": "object",
                    "enabled": False
                }
            }
        }
    },
    2: {
        "mappings": {
            "properties": {
                "log": {
                    "type": "object"
                }
            },
            "dynamic_templates": [
                {
                    "skip_text": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "keyword",
                            "index": False,
                            "doc_values": False
                        },
                        "path_match": "log.*"
                    }
                },
                {
                    "skip_indexing": {
                        "mapping": {
                            "index": False,
                            "doc_values": False
                        },
                        "path_match": "log.*"
                    }
                }
            ]
        }
    },
    3: {
        "mappings": {
            "properties": {
                "log": {
                    "type": "object"
                }
            }
        }
    }
}


def get_opensearch_client():
    """Create and return OpenSearch client"""
    client = OpenSearch(
        hosts=[{'host': "localhost", 'port': 9200}],
        use_ssl=False,
        verify_certs=False,
        ssl_show_warn=False,
    )
    return client

def get_opensearch_security_client():
    """Create and return OpenSearch client"""
    client = OpenSearch(
        hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
        http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False,
    )
    return client

def generate_random_string(length=10):
    """Generate random string"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


def generate_random_value():
    """Generate a random value of various types"""
    value_type = random.choice([
        'string', 'string', 'string',  # More strings
        'int', 'int',
        'float', 'float',
        'boolean',
        'timestamp',
        'nested_object'
    ])
    
    if value_type == 'string':
        return generate_random_string(random.randint(5, 20))
    elif value_type == 'int':
        return random.randint(-10000, 10000)
    elif value_type == 'float':
        return round(random.uniform(-10000, 10000), 2)
    elif value_type == 'boolean':
        return random.choice([True, False])
    elif value_type == 'timestamp':
        return (datetime.utcnow() - timedelta(seconds=random.randint(0, 86400))).isoformat() + "Z"
    elif value_type == 'nested_object':
        # Create a small nested object (2-5 random fields)
        nested = {}
        for _ in range(random.randint(2, 5)):
            key = f"field_{random.randint(1000, 9999)}"
            nested[key] = generate_random_value()
        return nested


def generate_random_key():
    """Generate random key names"""
    patterns = [
        f"field_{random.randint(1000, 999999)}",
        f"{generate_random_string(8)}_{random.randint(100, 999)}",
        f"attr_{generate_random_string(6)}_{random.randint(10, 99)}",
        f"prop_{random.randint(1000, 9999)}",
        f"var_{generate_random_string(10)}",
    ]
    return random.choice(patterns)


def generate_mock_log_large(num_fields: int = 1000):
    """Generate a single mock log record with many sub-fields"""
    
    # Random timestamps
    base_time = datetime.utcnow()
    log_time = base_time - timedelta(seconds=random.randint(0, 3600))
    obs_time = log_time + timedelta(milliseconds=random.randint(1, 100))
    
    # Random values
    environments = ["prod", "staging", "dev"]
    regions = ["us-east-1", "us-west-2", "eu-west-1"]
    levels = ["error", "warn", "info", "debug"]
    
    # Generate large "log" object with many sub-fields
    log_object = {
        "msg": f"Error {generate_random_string(15)} for {generate_random_string(10)}",
        "caller": f"{generate_random_string(8)}/{generate_random_string(6)}.go:{random.randint(1, 2000)}",
        "level": random.choice(levels),
        "ts": log_time.isoformat() + "Z",
    }
    
    # Add num_fields random fields to log object
    for i in range(num_fields):
        key = generate_random_key()
        value = generate_random_value()
        log_object[key] = value
    
    # Create the full record
    mock_data = {
        "traceId": generate_random_string(36),
        "instrumentationScope": {
            "droppedAttributesCount": random.randint(0, 5)
        },
        "resource": {
            "droppedAttributesCount": random.randint(0, 3),
            "attributes": {
                "log_type": "EKS_node",
                "k8s_label.productid": f"pr{random.randint(100000, 999999)}",
                "k8s_label.sourcetype": generate_random_string(10),
                "productid": f"pr{random.randint(100000, 999999)}",
                "k8s.platform": "EKS",
                "k8s_label.criticality_code": str(random.randint(1, 10)),
                "k8s.cluster.business.unit": generate_random_string(8),
                "criticality_code": str(random.randint(1, 10)),
                "sourcetype": generate_random_string(10),
                "log_tier": random.choice(["standard", "premium", "basic"]),
                "applicationid": f"ap{random.randint(100000, 999999)}",
                "obs_namespace": generate_random_string(10)
            },
            "schemaUrl": ""
        },
        "flags": random.randint(0, 1),
        "severityNumber": random.randint(0, 24),
        "schemaUrl": "",
        "spanId": generate_random_string(16),
        "severityText": random.choice(levels),
        "attributes": {
            "cluster.name": f"{generate_random_string(8)}-cluster{random.randint(1, 5)}-ci-{random.choice(environments)}-{random.choice(regions)}",
            "cluster.region": random.choice(regions),
            "log.file.path": f"/var/log/{generate_random_string(8)}/{generate_random_string(6)}.log",
            "cluster.env": random.choice(environments),
            "obs_body_length": random.randint(50, 5000),
        },
        "time": log_time.isoformat() + "Z",
        "droppedAttributesCount": random.randint(0, 2),
        "observedTimestamp": obs_time.isoformat() + "Z",
        "@timestamp": (base_time + timedelta(seconds=3)).isoformat() + "Z",
        "log": log_object
    }
    
    return mock_data


def generate_documents_and_save(num_records: int, num_fields: int):
    """Generate multiple mock records to yield documents for bulk insert and store in file"""
    with open("mock_logs_large.jsonl", 'w') as f:
        for i in range(num_records):
            print(f"\n--- Record {i + 1}/{num_records} ---")
            doc = generate_mock_log_large(num_fields=num_fields)
            f.write(json.dumps(doc) + '\n')
            print(f"✓ Record {i + 1} saved")
            # Yield in opensearchpy bulk format
            yield {
                "_index": OPENSEARCH_INDEX,
                "_id": f"{i}",
                "_source": doc
            }

def generate_documents(num_records: int, num_fields: int, index_name: str):
    """Generate multiple mock records to yield documents for bulk insert and store in file"""
    for i in range(num_records):
        print(f"\n--- Record {i + 1}/{num_records} ---")
        doc = generate_mock_log_large(num_fields=num_fields)
        # Yield in opensearchpy bulk format
        yield {
            "_index": index_name,
            "_id": f"{i}",
            "_source": doc
        }

def check_connection():
    """Check connection to OpenSearch"""

    try:
        # Connect to OpenSearch
        print("Connecting to OpenSearch...")
        client = get_opensearch_security_client()
        # Test connection
        info = client.info()
        print(f"✓ Connected to OpenSearch: {info['version']['number']}")

    except Exception as e:
        print(f"✗ Error: {str(e)}")
        print("\nTroubleshooting tips:")
        print("  1. Check OpenSearch is running on {}:{}".format(OPENSEARCH_HOST, OPENSEARCH_PORT))
        print("  2. Verify credentials (username: {})".format(OPENSEARCH_USER))
        print("  3. Check SSL settings (currently SSL verification disabled)")

def bulk_insert_to_opensearch(num_records: int, num_fields: int, index_name: str, chunk_size: int = 100):
    """Bulk insert generated documents to OpenSearch"""

    try:
        # Connect to OpenSearch
        print("Connecting to OpenSearch...")
        client = get_opensearch_client()

        # Test connection
        info = client.info()
        print(f"✓ Connected to OpenSearch: {info['version']['number']}")

        # Create index if it doesn't exist
        if not client.indices.exists(index=index_name):
            print(f"Creating index: {index_name} (mapping_mode={MAPPING_MODE})")
            mapping = MAPPINGS.get(MAPPING_MODE)
            if mapping:
                client.indices.create(index=index_name, body=mapping)
            else:
                client.indices.create(index=index_name)
            print(f"✓ Index created: {index_name}")
        else:
            print(f"✓ Index already exists: {index_name}")

        # Update settings on existing index
        print("Updating index settings...")
        client.indices.put_settings(
            index=index_name,
            body={
                "settings": {
                    "index.mapping.total_fields.limit": 1000000
                }
            }
        )
        # Bulk insert
        print(f"\nBulk inserting {num_records} documents with {num_fields} fields each...")
        print(f"Chunk size: {chunk_size}")
        print()


        success_count = 0
        error_count = 0

        # Use helpers.bulk for efficient bulk operations
        for success, info in helpers.bulk(
            client,
            generate_documents(num_records, num_fields, index_name),
            chunk_size=chunk_size,
            raise_on_error=False
        ):
            if success:
                success_count += 1
                if success_count % 100 == 0:
                    print(f"✓ Inserted {success_count} documents...")
            else:
                error_count += 1
                print(f"✗ Error: {info}")
        
        print(f"\n{'='*50}")
        print(f"Bulk Insert Complete!")
        print(f"{'='*50}")
        print(f"✓ Successfully inserted: {success_count} documents")
        if error_count > 0:
            print(f"✗ Failed inserts: {error_count} documents")
        print(f"Total: {success_count + error_count} documents")
        
        # Get index stats
        stats = client.indices.stats(index=index_name)
        doc_count = stats['indices'][index_name]['primaries']['docs']['count']
        store_size = stats['indices'][index_name]['primaries']['store']['size_in_bytes']
        
        print(f"\nIndex Statistics:")
        print(f"  Total documents: {doc_count}")
        print(f"  Index size: {store_size / (1024*1024):.2f} MB")
        
        client.close()
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        print("\nTroubleshooting tips:")
        print("  1. Check OpenSearch is running on {}:{}".format(OPENSEARCH_HOST, OPENSEARCH_PORT))
        print("  2. Verify credentials (username: {})".format(OPENSEARCH_USER))


def run_search_benchmark(index_name: str, num_queries: int = 10):
    """Run search queries and measure end-to-end time"""
    print("\n" + "="*50)
    print(f"Running Search Benchmark on {index_name}")
    print("="*50)

    client = get_opensearch_client()
    search_times = []

    for i in range(num_queries):
        start_time = time.time()
        response = client.search(
            index=index_name,
            body={"size": 10}
        )
        end_time = time.time()

        elapsed_ms = (end_time - start_time) * 1000
        search_times.append(elapsed_ms)
        hits = response['hits']['total']['value']
        print(f"  Search {i + 1}: {elapsed_ms:.2f} ms (hits: {hits})")

    avg_time = sum(search_times) / len(search_times)
    min_time = min(search_times)
    max_time = max(search_times)

    print("\nSearch Results:")
    print(f"  Average: {avg_time:.2f} ms")
    print(f"  Min: {min_time:.2f} ms")
    print(f"  Max: {max_time:.2f} ms")
    print(f"  Total: {sum(search_times):.2f} ms")

    client.close()


if __name__ == "__main__":
    print("="*50)
    print("OpenSearch Bulk Insert - Mock Data Generator")
    print("="*50)
    mapping_descriptions = {
        1: "log field is an object (dynamic mappings)",
        2: "log.enabled=false",
        3: "dynamic_templates (skip indexing)"
    }
    print("\nConfiguration:")
    print(f"  OpenSearch Host: {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
    print(f"  Records to generate: {NUM_RECORDS}")
    print(f"  Fields per log object: {NUM_FIELDS}")
    print(f"  Index count: {INDEX_COUNT}")
    print(f"  Search round: {READ_ROUND}")
    print(f"  Mapping mode: {MAPPING_MODE} ({mapping_descriptions[MAPPING_MODE]})")
    print()

    # check_connection()

    # Start total timer
    script_start_time = time.time()

    # Loop through INDEX_COUNT indices
    for idx in range(1, INDEX_COUNT + 1):
        index_name = f"{OPENSEARCH_INDEX}-{idx}"
        print("\n" + "="*50)
        print(f"Processing Index {idx}/{INDEX_COUNT}: {index_name}")
        print("="*50)

        # Perform bulk insert
        bulk_insert_to_opensearch(
            num_records=NUM_RECORDS,
            num_fields=NUM_FIELDS,
            index_name=index_name,
            chunk_size=100
        )

        # Run search benchmark
        run_search_benchmark(index_name=index_name, num_queries=READ_ROUND)

    # Calculate total time
    script_end_time = time.time()
    total_time_sec = script_end_time - script_start_time

    print("\n" + "="*50)
    print("Script Complete!")
    print("="*50)
    print(f"Total time: {total_time_sec:.2f} seconds ({total_time_sec/60:.2f} minutes)")
