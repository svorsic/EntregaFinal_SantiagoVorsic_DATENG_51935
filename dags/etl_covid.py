# Este es el DAG que orquesta el ETL de la tabla covid_colombia

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.exceptions import AirflowSensorTimeout, AirflowSkipException

import smtplib
import pytz
import importlib_metadata
from datetime import datetime, timedelta
from email import message

QUERY_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS covid_colombia (
    fecha_reporte_web DATETIME,
    id_de_caso INT,
    fecha_de_notificaci_n DATETIME,
    departamento INT,
    ciudad_municipio INT,
    edad INT,
    unidad_medida VARCHAR(50),
    sexo VARCHAR(50),
    fuente_tipo_contagio VARCHAR(50),
    ubicacion VARCHAR(50),
    estado VARCHAR(50),
    recuperado VARCHAR(50),
    fecha_inicio_sintomas DATETIME,
    fecha_diagnostico DATETIME,
    fecha_recuperado VARCHAR(50),
    tipo_recuperacion VARCHAR(50),
    per_etn_ INT,
    fecha_muerte VARCHAR(50),
    process_date VARCHAR(10) distkey
) SORTKEY(process_date, id_de_caso);
"""

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM covid_colombia WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""


# Se crea la función process_date and push it to xcom
def get_process_date(**kwargs):
    # Si la decha de proceso, process_date, se encuentra provista, es tomada. En caso contrario se asigna el día de la fecha.
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)

# Sensor de aposible alerta dentro de los procesos de ETL

class SensorDemora(BaseSensorOperator):

    #Sensor que chequea si la acción realizada lleva más del tiempo esperado, en este caso se fija un límite de 15 minutos.


    def __init__(self, *args, **kwargs):
        super(SensorDemora, self).__init__(*args, **kwargs)

    def poke(self, context):
        # Obtener el tiempo de inicio de la tarea actual
        execution_date = context['execution_date']
        start_time = context['ti'].start_date

        # Obtener el tiempo actual ajustando zona horaria
        current_time = datetime.now(tz=pytz.utc)

        # Calcular la diferencia de tiempo entre el inicio y el tiempo actual
        time_difference = current_time - start_time

        # Verificar si han pasado más de 15 minutos (900 segundos)
        if time_difference.total_seconds() > 900:
            # Si han pasado más de 15 minutos, generar una alerta
            enviar_alerta(self)
            raise AirflowSensorTimeout("La tarea ha tardado más de 15 minutos en ejecutarse")
        # Si no ha pasado suficiente tiempo, el sensor continuará monitoreando la tarea
        else:
            raise AirflowSkipException("La operación se completó con éxito. Todo parece en orden por ahora.")


# Email de alerta de fallo.

def enviar_alerta():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_PASSWORD'))
        subject='Alerta: El proceso de ETL esta demorando mas de lo deseado'
        body_text = "El ETL se encuentra demorado, el proceso esta en riesgo. Se recomienda monitorear la situacion."
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_EMAIL_TO'),message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')
        raise exception

# Email de comprobación de proceso exitoso

def enviar_exito():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_PASSWORD'))
        subject2='ETL Exitoso'
        body_text2='La tarea de extraccion de datos de Covid en Medellin fue exitosa. Puede tener paz en el alma hasta que surja el proximo error.'
        message2='Subject: {}\n\n{}'.format(subject2,body_text2)
        x.sendmail(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_EMAIL_TO'),message2)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')
        raise exception

default_args = {
    "owner": "Santiago Vorsic",
    'start_date': datetime(2023, 7, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    'depends_on_past': False,
}

with DAG(
    dag_id="etl_covid",
    default_args=default_args,
    description="ETL de la tabla covid Colombia",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
        dag=dag,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    clean_process_date = SQLExecuteQueryOperator(
        task_id="clean_process_date",
        conn_id="redshift_default",
        sql=QUERY_CLEAN_PROCESS_DATE,
        dag=dag,
    )

    spark_etl_covid = SparkSubmitOperator(
        task_id="spark_etl_covid",
        application=f'{Variable.get("spark_scripts_dir")}/ETL_Covid.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    sensor_fallo = SensorDemora(
        task_id='sensor_fallo',
        dag=dag,
        poke_interval=60
    )

    email_exito = PythonOperator(
        task_id="email_exito",
        python_callable=enviar_exito,
        dag=dag,
    )

    get_process_date_task >> create_table >> clean_process_date >> spark_etl_covid >> email_exito
    spark_etl_covid >> sensor_fallo
