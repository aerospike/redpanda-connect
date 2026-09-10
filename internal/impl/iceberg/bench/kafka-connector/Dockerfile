FROM confluentinc/cp-kafka-connect-base:7.7.8@sha256:ea010d115a6fa505992db38a39590448614adc715b1ccddfeaf19a147237caaa
RUN curl -fL \
      -o /tmp/iceberg-kafka-connect.zip \
      "https://github.com/databricks/iceberg-kafka-connect/releases/download/v0.6.19/iceberg-kafka-connect-runtime-0.6.19.zip" && \
    confluent-hub install --no-prompt \
      --component-dir /usr/share/confluent-hub-components/ \
      /tmp/iceberg-kafka-connect.zip && \
    rm /tmp/iceberg-kafka-connect.zip
