import time
import json
import psutil
import boto3
import os
from functools import wraps

# Inizializza client S3
s3 = boto3.client("s3")
BUCKET = os.getenv("S3_BUCKET")
METRICS_PREFIX = os.getenv("METRICS_PREFIX", "metrics")

def monitor_job(job_name):
    """
    Decoratore per misurare tempo, CPU e memoria di una funzione/job
    e salvare i dati su S3.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            cpu_before = psutil.cpu_percent(interval=1)
            mem_before = psutil.virtual_memory().percent
            
            result = func(*args, **kwargs)
            
            end_time = time.time()
            cpu_after = psutil.cpu_percent(interval=1)
            mem_after = psutil.virtual_memory().percent
            
            metrics = {
                "job": job_name,
                "start_time": start_time,
                "end_time": end_time,
                "elapsed_seconds": end_time - start_time,
                "cpu_before_percent": cpu_before,
                "cpu_after_percent": cpu_after,
                "memory_before_percent": mem_before,
                "memory_after_percent": mem_after
            }
            
            if BUCKET:
                timestamp = int(time.time())
                key = f"{METRICS_PREFIX}/{job_name}_{timestamp}.json"
                s3.put_object(
                    Bucket=BUCKET,
                    Key=key,
                    Body=json.dumps(metrics, indent=2)
                )
            
            print(f"Job '{job_name}' completato in {metrics['elapsed_seconds']:.2f}s")
            print(f"   CPU: {cpu_before}% → {cpu_after}% | Memoria: {mem_before}% → {mem_after}%")
            
            return result
        return wrapper
    return decorator
