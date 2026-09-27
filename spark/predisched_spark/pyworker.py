"""Spark's Python worker entry point with a flush at the end (Windows only).

On Windows Spark starts a fresh ``python -m pyspark.worker`` per task (no forking daemon). In
PySpark 3.5.x that module's ``__main__`` never flushes the socket after the task's last records
(end of data, accumulators, end of stream) and leaves it to interpreter shutdown; under Python
3.12 the socket can close first, the JVM reads EOF and reports "Python worker exited unexpectedly
(crashed)" although the worker exited 0. This is the same ``__main__`` plus the flush.
``common.configure`` selects it with ``spark.python.worker.module``.
"""

import os

from pyspark import worker
from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import write_int

if __name__ == "__main__":
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    try:
        worker.main(sock_file, sock_file)
    finally:
        sock_file.flush()
