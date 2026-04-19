FROM apache/spark:3.5.1

USER root
RUN pip3 install --no-cache-dir \
    pyarrow \
    pandas \
    numpy \
    clickhouse-connect \
    networkx \
    scikit-learn \
    python-louvain \
    xgboost

WORKDIR /workspace
