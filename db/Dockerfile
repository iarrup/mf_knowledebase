# Use the official Apache AGE image based on PostgreSQL 16 (v1.6.0)
FROM apache/age:release_PG16_1.6.0

# Install pgvector for the corresponding PostgreSQL version
USER root
RUN apt-get update && \
    apt-get install -y postgresql-16-pgvector && \
    apt-get clean

# Switch back to the default postgres user
USER postgres