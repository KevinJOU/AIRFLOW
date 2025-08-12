from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -------- Tâche READ --------
def read_logs(**kwargs):
    spark = SparkSession.builder.appName("ReadLogsExample").getOrCreate()
    logs_path = "/opt/airflow/logs/data/logs_serveur.txt" 
    df_raw = spark.read.text(logs_path)
    df_split = df_raw.withColumn("timestamps", F.split(F.col("value"), "@").getItem(0)) \
                     .withColumn("log", F.split(F.col("value"), "@").getItem(1)) \
                     .select("timestamps", "log")
    df_split.show(truncate=False)  
    output_path = "/tmp/read_logs_df.parquet"
    df_split.write.mode("overwrite").parquet(output_path)
    kwargs['ti'].xcom_push(key='logs_parquet_path', value=output_path)
    spark.stop()

    
# -------- Tâche COMPUTE --------
def compute_task(**kwargs):
    spark = SparkSession.builder.appName("ComputeBreakdown").getOrCreate()
    logs_parquet_path = kwargs['ti'].xcom_pull(key='logs_parquet_path', task_ids='read_logs_task')
    df = spark.read.parquet(logs_parquet_path)
    df2 = df.withColumn("Breakdown_Type", F.regexp_extract("log", r"slab/([^/]+)/", 1)) \
            .withColumn("Breakdown_Level", F.regexp_extract("log", r"256/([0-9]+)", 1))
    # Détecter changement de Breakdown_Type
    window_all = Window.orderBy("timestamps")
    df2 = df2.withColumn("Group_Flag",
                         F.when(F.lag("Breakdown_Type", 1).over(window_all) != F.col("Breakdown_Type"), 1).otherwise(0))
    df2 = df2.withColumn("Group", F.sum("Group_Flag").over(window_all))
    # Compte successif
    df2 = df2.withColumn("Count_Successive", F.count("Breakdown_Type").over(Window.partitionBy("Group")))
    # Utiliser le groupe comme identifiant de séquence
    grouped_df = df2.groupBy("Group", "Breakdown_Type", "Count_Successive") \
            .agg(F.collect_list("Breakdown_Level").alias("Breakdowns_Level")) \
            .withColumn("Breakdowns_Level", F.concat_ws(",", F.col("Breakdowns_Level"))) \
            .select("Breakdown_Type", "Count_Successive", "Breakdowns_Level")   # pour matcher la sortie attendue

    grouped_df.show(truncate=False)
    output_path = "/tmp/compute_df.parquet"
    grouped_df.write.mode("overwrite").parquet(output_path)
    kwargs['ti'].xcom_push(key='compute_parquet_path', value=output_path)
    spark.stop()
    
# -------- Tâche SAVE RESULT --------
def save_result(**kwargs):
    spark = SparkSession.builder.appName("SaveResult").getOrCreate()
    compute_parquet_path = kwargs['ti'].xcom_pull(key='compute_parquet_path', task_ids='compute_task')
    df = spark.read.parquet(compute_parquet_path)
    output_csv_dir = "/tmp/final_results_csv"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_csv_dir)
    kwargs['ti'].xcom_push(key='final_csv_path', value=output_csv_dir)
    spark.stop()

# -------- Définition du DAG --------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2000, 8, 11),
    "retries": 1
}

with DAG(
    dag_id="Projet_SDA_KJ",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    read_logs_task = PythonOperator(
        task_id="read_logs_task",
        python_callable=read_logs
    )

    compute_task_op = PythonOperator(
        task_id="compute_task",
        python_callable=compute_task
    )

    save_result_task = PythonOperator(
        task_id="save_result",
        python_callable=save_result
    )

    read_logs_task >> compute_task_op >> save_result_task