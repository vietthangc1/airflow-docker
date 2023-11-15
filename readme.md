### Minio Setup
run minio:
```
🦄  docker run -d -p 9000:9000 -p 9090:9090 --name minio1 -e "MINIO_ROOT_USER=ROOTUSER" -e "MINIO_ROOT_PASSWORD=CHANGEME123" quay.io/minio/minio server /data --console-address ":9090"
```

config s3:
```
{
    "service_config": {
      "s3": {
        "bucket_name": "airflow"
      }
    },
    "host": "http://host.docker.internal:9000"
  }
```