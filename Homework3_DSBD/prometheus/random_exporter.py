import prometheus_client  # Prometheus Python client library
import time  # Used to control the update interval for metrics
import psutil  # Can be used for real system metrics if needed
import random  # Generates random values for simulated metrics
import socket  # Used to fetch the hostname

# Update interval in seconds
UPDATE_PERIOD = 30

# Fetch the hostname dynamically
HOSTNAME = socket.gethostname()

# Application name label
APP_NAME = "random_exporter"

# Prometheus Gauge metric for fake CPU usage, with server, hostname, and app as labels
CPU_RANDOM_USAGE = prometheus_client.Gauge(
    'cpu_random', 
    'Fake CPU usage', 
    ['server', 'hostname', 'app']
)

# Prometheus Gauge metric for fake memory usage, with server, hostname, and app as labels
MEM_RANDOM_USAGE = prometheus_client.Gauge(
    'mem_random', 
    'Fake memory usage', 
    ['server', 'hostname', 'app']
)

# Prometheus Gauge metric for fake response time, with server, hostname, and app as labels
RESPONSE_TIME_RANDOM = prometheus_client.Gauge(
    'rt_random', 
    'Fake response time', 
    ['server', 'hostname', 'app']
)

# Prometheus Counter metric for number of iterations, with server, hostname, and app as labels
NUM_ITERATION = prometheus_client.Counter(
    'system_usage', 
    'Real iterations value', 
    ['server', 'hostname', 'app']
)

if __name__ == '__main__':
    # Start the Prometheus HTTP server on port 9999
    prometheus_client.start_http_server(9999)
    print(f"Prometheus metrics are available at http://{HOSTNAME}:9999/metrics")

    # Infinite loop to update metrics at each interval
    while True:
        # Set a random value for CPU usage
        CPU_RANDOM_USAGE.labels(server='random-node', hostname=HOSTNAME, app=APP_NAME).set(random.random() * 100)

        # Set a random value for memory usage
        MEM_RANDOM_USAGE.labels(server='random-node', hostname=HOSTNAME, app=APP_NAME).set(random.random() * 100)

        # Set a random value for response time using a Gaussian distribution
        RESPONSE_TIME_RANDOM.labels(server='random-node', hostname=HOSTNAME, app=APP_NAME).set(random.gauss(mu=125.0, sigma=30.0))

        # Increment the iteration counter
        NUM_ITERATION.labels(server='random-node', hostname=HOSTNAME, app=APP_NAME).inc()

        # Wait for the next update
        time.sleep(UPDATE_PERIOD)
