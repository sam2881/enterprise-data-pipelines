# =============================================================================
# APEX Custom Airflow Image
# =============================================================================
# Extends the base Airflow image with:
#   1. dag_utilities package (pip-installed so DAGs can import it)
#   2. spark_jobs scripts (available at /opt/airflow/spark_jobs/)
#   3. Runtime Python dependencies for dag_utilities
# =============================================================================

FROM apache/airflow:2.9.3-python3.11

USER root

# Create spark_jobs directory
RUN mkdir -p /opt/airflow/spark_jobs && chown airflow:root /opt/airflow/spark_jobs

USER airflow

# Install runtime dependencies for dag_utilities
COPY infrastructure/airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# Copy and install dag_utilities as a Python package
COPY --chown=airflow:root agents/data_agent/src/dag_utilities/ /tmp/dag_utilities/
RUN pip install --no-cache-dir /tmp/dag_utilities/ && rm -rf /tmp/dag_utilities/

# Copy spark_jobs to a known location
COPY agents/data_agent/src/spark_jobs/ /opt/airflow/spark_jobs/
