FROM confluentinc/cp-kafka-connect-base:7.7.8@sha256:ea010d115a6fa505992db38a39590448614adc715b1ccddfeaf19a147237caaa
# Install the Confluent JDBC sink connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.14
# Install the PostgreSQL JDBC driver (not bundled with the connector hub package)
RUN curl -fL \
      -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.3.jar \
      "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
