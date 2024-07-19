import json
import redis

def upload_json_to_redis(file_path, redis_host='localhost', redis_port=6379, redis_password=None):
    # Connect to Redis
    r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
    
    with open(file_path, 'r') as file:
        # Read JSON file in chunks
        while True:
            line = file.readline()
            if not line:
                break
            data = json.loads(line.strip())
            
            for key, value in data.items():
                # Convert key tuple to string format suitable for Redis
                # TODO: Get rid of "location"
                redis_key = f'location:{key}'
                if isinstance(value, dict):
                    # If the value is a dictionary, set each field individually
                    for field, field_value in value.items():
                        if isinstance(field_value, (dict, list)):
                            field_value = json.dumps(field_value)
                        r.hset(redis_key, field, field_value)
                else:
                    # If the value is not a dictionary, convert it to a JSON string if necessary
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value)
                    r.set(redis_key, value)
    
    print("Upload complete!")

if __name__ == "__main__":
    file_path = '/Users/wthompson/Documents/wjtho/ContractWork/well-drilling-site-visualization/extra_python_files_etc/cached_data/chunked_well_data.json'  # Update this path
    upload_json_to_redis(file_path)
