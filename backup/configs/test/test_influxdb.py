from influxdb_client import InfluxDBClient, Point

INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "Jgxk0btcmISScfFxfUm903U2Q8vSACUZSXBJBw_tHWSHnDUl6HmpmEeOtw0XUZyQ5lfaEcyOdjmXxHHCOoC0Xg=="
INFLUXDB_ORG = "stream-org"
INFLUXDB_BUCKET = "industry_data"

try:
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api()

    # Write a test point
    point = Point("test_measurement").field("test_field", 123.45)
    try:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        print(f"Written to InfluxDB: {point.to_line_protocol()}")
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")


except Exception as e:
    print(f"Error writing to InfluxDB: {e}")
finally:
    client.close()
