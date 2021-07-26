# Datathon 2021 - Dataops

## Run repo
First run ```make build_docker_image``` in order to build the docker image and install dependences.
Once image is built run ```make init_airflow``` in order to run image.
Inside docker image:

```airflow initdb```

```airflow scheduler &```

```airflow webserver```

Navigate to http://0.0.0.0:8080/admin/ and trigger dag.

Output is stored at /home/airflow/resultados.csv inside container.

## Linter
Black is installed when image is built. In order to run black to reformat files:

1- Navigate to /opt/airflow/dags

2- run ```black .```

It will reformat files automatically# datathon-dataops
