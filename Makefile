init_airflow:
	docker run -it \
 	--rm -p 8080:8080 \
 	-v ${PWD}/dags:/opt/airflow/dags \
 	--entrypoint bash dataops:dev

build_docker_image:
	docker build . -t dataops:dev
