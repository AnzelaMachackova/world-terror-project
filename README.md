# World Terror Project: Airflow DAG
This project is focused on Airflow, GCP and public API. In this project you will find a simple DAG that:
1) downloads the public dataset via API from the kaggle website
2) saves the dataset to the GCP bucket
3) create a table <i>world_terror_table</i> in BigQuery
   
The DAG contains only three tasks, and their execution is conditional upon the termination of the previous one. DAG only performs basic functions and does not modify the table.
<img width="800" alt="world-terror-dag" src="https://github.com/AnzelaMachackova/world-terror-project/assets/92174501/3faf9d31-bc7d-4b21-befe-3acac4fdfda8">

In case of an error during execution, DAG will display an error in the log and stop the run. However, special behavior for different types of errors is not implemented.

DAG works with kaggle token for API calls, uses Google Cloud Credentials to execute tasks associated with GCS and BigQuery. When downloading data from Kaggle, DAG unzips the file. During loading the table into BigQuery, it defines the schema and column names in a suitable format.

The table contains data on terrorist acts around the world in the years 1960-2021. In the case of uploading another dataset, it is necessary to modify the hardcoded schema, which is not dynamic. Thanks to the configuration file, it is possible to edit the variables defining the GCP project, the name of the file, the link to the dataset, etc.

Dataset: <a href="https://www.kaggle.com/datasets/willianoliveiragibin/terrorism-in-world/data">Terrorism_in_world</a> 
