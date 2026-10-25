FROM yugabytedb/yugabyte:2026.1.2.0-b137
USER 0:0
RUN mkdir -p /data && chown 10001:100 /data
USER 10001:100
