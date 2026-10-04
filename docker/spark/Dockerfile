FROM apache/spark:3.5.3-python3

USER root
COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
# Cache ivy cho --packages, ghi được bởi user spark
RUN mkdir -p /opt/aq-ivy && chown -R spark:spark /opt/aq-ivy
USER spark
WORKDIR /opt/aq
