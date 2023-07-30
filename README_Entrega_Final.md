┌───────────────────────────────── •✧✧• ─────────────────────────────────┐
     Entrega Final del curso de Data Engineering Flex - Comisión 51935
└───────────────────────────────── •✧✧• ─────────────────────────────────┘
                         Alumno > Santiago Vorsic 

# Entrega Final
El presente trabajo se basa en ek ETL y procesamiento de datos de una base de registros de contagios de Covid dentro del municipio de Medellin en Colombia durante el final del período de la pandemia de diche enfermedad.
Este conjunto de documentos desarrollado en base a los entregables de ejemplo, por medio de Spark como framework de ETL, y su proceso hace uso de Pandas en la transformación de los datos.

# Distribución de los archivos
Los archivos a tener en cuenta son:
* `docker_images/`: Contiene los Dockerfiles para crear las imagenes utilizadas de Airflow y Spark.
* `docker-compose.yml`: Archivo de configuración de Docker Compose. Contiene la configuración de los servicios de Airflow y Spark.
* `.env`: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres. NO SE INCLUYE DENTRO DE LA ENTREGA A FIN DE EVITAR RIESGOS DE SEGURIDAD.
* `dags/`: Carpeta con los archivos de los DAGs.
    * `etl_covid.py`: DAG principal que ejecuta el pipeline de extracción, transformación y carga de datos de usuarios. Se incluyen un sensor que envía alertas en caso de demorarse demasiado el proceso. La demora es identificada como un factor de riesgo, ya que el proceso de debería demorar menos de 5 minutos, una demora excesiva puede ser indicador de que algo ha fallado o está por fallar.
* `logs/`: Carpeta con los archivos de logs de Airflow. NO SE INCLUYE DENTRO DE LA ENTREGA POR NO SER DE RELEVANCIA PARA LA MISMA.
* `plugins/`: Carpeta con los plugins de Airflow. NO SE INCLUYE DENTRO DE LA ENTREGA POR NO SER DE RELEVANCIA PARA LA MISMA.
* `postgres_data/`: Carpeta con los datos de Postgres.
* `scripts/`: Carpeta con los scripts de Spark.
    * `postgresql-42.5.2.jar`: Driver de Postgres para Spark.
    * `common.py`: Script de Spark con funciones comunes.
    * `ETL_Covid.py`: Script de Spark que ejecuta el ETL.

# Pasos para ejecutar el trabajo
1. Posicionarse en la carpeta `EntregaFinal_SantiagoVorsic_DATENG_51935/`. A esta altura debería ver el archivo `docker-compose.yml`.
2. Crear las siguientes carpetas a la misma altura del `docker-compose.yml`. Las mismas permitiran que los archivos temporarles y cache se registren en caso de necesitar hacer un relevamiento de lo sucedido.
```bash
mkdir -p dags,logs,plugins,postgres_data,scripts
```
3. Contar con un archivo con variables de entorno llamado `.env` ubicado a la misma altura que el `docker-compose.yml`. Cuyo contenido sea:
```bash
REDSHIFT_HOST=...
REDSHIFT_PORT=5439
REDSHIFT_DB=...
REDSHIFT_USER=...
REDSHIFT_SCHEMA=...
REDSHIFT_PASSWORD=...
REDSHIFT_URL="jdbc:postgresql://${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASSWORD}"
DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar
```
4. Descargar las imagenes de Airflow y Spark.
```bash
docker-compose pull lucastrubiano/airflow:airflow_2_6_2
docker-compose pull lucastrubiano/spark:spark_3_4_1
```
5. Ejecutar el siguiente comando para levantar los servicios de Airflow y Spark.
```bash
docker-compose up --build
```
6. Una vez que los servicios estén levantados, ingresar a Airflow por medio del navegador en `http://localhost:8080/`.
8. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Redshift:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
9. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Spark:
    * Conn Id: `spark_default`
    * Conn Type: `Spark`
    * Host: `spark://spark`
    * Port: `7077`
    * Extra: `{"queue": "default"}`
10. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `driver_class_path`
    * Value: `/tmp/drivers/postgresql-42.5.2.jar`
11. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `spark_scripts_dir`
    * Value: `/opt/airflow/scripts`
12. Ejecutar el DAG `etl_users`.