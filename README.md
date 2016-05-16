Analytics

This application is built using Kappa architecture for complex analytics business use case.

Entire life-cycle of the application is managed in the Spark cluster without any trips to intermediate storage. This eliminates potential data loss, delays and duplication.

The following steps summarize the application behavior.

1) Hits to search engine are streamlined to Kafka topic using Logstash pipeline
2) Application reads from Kafka and de-dupes hits
3) Application maintains a streaming window to build top hits
4) Application delegates top hits after the window elapses to search engine
5) Search engine is hit with the top queries and results extracted
6) Provider ranking and popularity matrix is built for the top hits
7) Provider ranking matrix is converted to JSON
8) JSON is published to Kafka topic for downstream applications consumption

