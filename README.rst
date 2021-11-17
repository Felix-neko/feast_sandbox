A  repo for FEAST debugging scripts for a test task
(some feast tutorials are ported to Apache Hive offline store)

=================================
Starting local debug servers
=================================
Our sandbox has batteries included: we provide local Docker servers of Redis and Apache Hive.
To start them, type

``bash start_docker.sh``

To stop, type

``bash stop_docker.sh``

=======================
Driver rating tutorial
=======================

``feast_sandbox/driver_tutorial/download_data.py``: downloads data table with driver data from BigQuery
and stores it to ``repos/driver_parquet_repo`` folder as ``parquet`` file .

``feast_sandbox/driver_tutorial/populate_hive_base.py``: uploads previously downloaded data table to Apache Hive
(`driver_tuitorial` database is created)

``feast_sandbox/driver_tutorial/predict_driver.py``: main tutorial script
(predicts best driver from 5 given)

=========================
Fraud detection tutorial
=========================
A simplified version of normal fraud detection tutorial from ``Feast``
(we infer the AI model locally instead of sending model and features to Google AI platform)

``feast_sandbox/fraud_tutorial/download_data.py``: downloads data table with fraud data from BigQuery
and stores it to ``repos/fraud_parquet_repo`` folder as ``parquet`` files.

``feast_sandbox/driver_tutorial/populate_hive_base.py``: uploads previously downloaded data table to Apache Hive
(`fraud_tuitorial` database is created)

``feast_sandbox/driver_tutorial/detect_fraud.py``: main tutorial script
(trains a classification model and predicts if a transaction is fraudulent)

TODO: port backfill_features function!


======================
Credit rank tutorial
======================

Not implemented yet. Maybe later : 3