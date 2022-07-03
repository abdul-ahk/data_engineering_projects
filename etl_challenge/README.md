# README #

### What is this project about?

*ETL Challenge: To extract data from https-endpoints and transforming it to be GDPR compliant by removing PII's, then further designing a database based on the analysis requirements and finally transform then load data into database for further analysis.

### How do I run it?

* Oracle 18c Express Edition used as destination storage
* requirement.txt file is provided to install all the dependencies required to run the application
* Command to execute the ETL-Job: 
	python etl\main.py --url_list https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users \
	https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages \
	--user USERNAME --password PASSWORD --dsn <Data Source Name e.g: localhost/:1521/xe/SPARK-DB>
