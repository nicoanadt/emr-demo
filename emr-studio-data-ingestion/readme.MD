# EMR Studio demo

### Ingest data from Postgres to EMR based on a bookmark value

![Architecture](https://raw.githubusercontent.com/nicoanadt/emr-demo/main/emr-studio-data-ingestion/resources/EMR%20Studio%20data%20ingestion%20job.png))


Content of this folder
1. `PostgresIngestionJob-Single.ipynb` : All scripts are running in one notebook
2. `PostgresIngestionJob-Function.ipynb` : Require import of ETLUtility to get functions
3. `ETLUtility.ipynb` : Additional transformation reusable functions
4. `ETLUtility.py` : `ETLUtility.ipynb` converted to py for import from the main notebook

This job can be orchestrated by a step function such as:

![Architecture](https://raw.githubusercontent.com/nicoanadt/emr-demo/main/emr-studio-data-ingestion/resources/StepFunctions.png))
