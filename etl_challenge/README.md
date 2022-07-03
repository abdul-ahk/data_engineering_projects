# README #

Steps necessary to get application up and running.

### What is this repository for? ###

*Test ETL project, to extract data from https endpoints and load it into database after transformation to perform desired analysis

### How do I get set up? ###

* Oracle 18c Express Edition used as destination storage
* requirement.txt file is provided to install all the dependencies required to run the application
* Command to execute the ETL-Job: 
	python etl\main.py --url_list https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users \
	https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages \
	--user USERNAME --password PASSWORD --dsn <Data Source Name e.g: localhost/:1521/xe/SPARK-DB>