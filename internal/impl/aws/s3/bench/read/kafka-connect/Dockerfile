FROM confluentinc/cp-server-connect-base:7.7.8@sha256:cba58c584b7dbbd49aea39053962d3574f780b4d802d91c8045ed20431f14edf
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.23
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3-source:latest
