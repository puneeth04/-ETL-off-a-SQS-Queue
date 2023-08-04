# ETL-off-a-SQS-Queue

This application will read messages from SQS queue, transform the data 
and load to postgres DB.

## Steps to Run:
1. Clone the repository:
```
git clone https://github.com/puneeth04/ETL-off-a-SQS-Queue.git
``` 
2. Open a Terminal:
    i) cd into directory
```
cd ETL-off-a-SQS-Queue
```

3. Run Command:
```
make setup 
```
2. Open a new Terminal:
    i) cd into directory
```
cd ETL-off-a-SQS-Queue
```

3. Run Command:
```
make run 
```


## Questions
### How would you deploy this application in production?
    1) We need to have CICD setup to deploy across environments.
    2) We need to have SQS & RDS instance setup in AWS
### What other components would you want to add to make this production ready?
    1) Health checks on database
    2) store  secrets in AWS Secret Manager
    3) Add alerting mechanisim on failures
    4) Improve performance loading large volumes of data to Postgres (bulk insert using postgres copy)


### How can this application scale with a growing dataset?
    We can leverage event driven processesing using AWS Lambda and attaching SQS as a trigger or
    We can use AWS Kinesis which collects the messages from sqs and trigger the lambda.
    Lambda can run asynchronously based on the events by setting the concurrency limit.
### How can PII be recovered later on?
    We can use the 'retrieve_messages' method in the script to retrieve the data from Postgres and in the method we decoded the values of the 'PII' columns.

### What are the assumptions you made?
    Assumptions:
                1) Not to change the ddl script - App version is integer but we received as a string, so made below changes
                to match the datatypes.
                App version is in below format (Major.Minor.Patch) -> will be converted to (MajorMinorPatch)