setup:
	docker-compose --verbose -f docker-compose-test.yml down
	docker-compose -f docker-compose-test.yml up

run-docker:
	docker build . -t sqs_etl_pipeline
	docker run -it  sqs_etl_pipeline

run:
	pip3 install -r requirements.txt
	python3 src/etl_process.py