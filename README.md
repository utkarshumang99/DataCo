# DataCo
A real-time data pipeline that streams data from Kafka, processes the data in a datastore &amp; indexes the processed data in Elasticsearch. The data pipeline will be
used to ingest and analyze clickstream data from a web application.

#producer.py
The above file can act as an dummy clickstream data producer.

#data_store.py
Data_Store.py python file handles the following:
1. Read Kafka messages from kafka topic.
2. Connect to MySql Database.
3. Create Table to store incoming clickstream data(raw).
4. Process & Aggregate the raw data.
5. Store the aggregated data in MySql Database.
6. Checkpoint mechanism to track the last processed timestamp.
7. Index the aggregated data in Elasticsearch.


Assumptions made:-
1. We assume that the incoming clickstream data is in JSON format & contain all the required fields,
2. The data pipeline will be deployed on a cloud-based infrastructure with sufficient resources to handle the data processing & requirements.
3. Data store used - MySQL
4. Users country & city will be deteremined using an IP geolocation service.
5. Users browser, operating system & device information will be extracted from user agent string.
6. Checkpoint table in database (MySQL).
7. Cron jobs or Task Scheduler can be used to periodically process the data.
