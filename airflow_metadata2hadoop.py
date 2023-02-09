"""
dag is used to upload airflow metadata into Hadoop
"""

#import Python dependencies needed
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

#create python function
def airflow_dags_extract():
    from psycopg2 import connect as psyconn
    from impala.dbapi import connect as impalaconn
    import logging
    connection = None
    impyla_conn = None
    cursor = None
    impyla_cursor = None
    #create a logger with 'airflow-data-load'
    logger = logging.getLogger('airflow-data-load')
    #set logger level explicitly
    logger.setLevel(logging.INFO)

    try:
        airflow_pgsql_conf = Variable.get('airflow_pgsql_conf', deserialize_json=True)
        env = Variable.get('environment', deserialize_json=True)
        zone = env['zone']
        logger.info(f'read Variable:environment value in airflow: {zone}')
        
        # Connect to postgresql database, which is the airflow metadata database
        connection = psyconn(user=airflow_pgsql_conf['user'],
                             password=airflow_pgsql_conf['password'],
                             host=airflow_pgsql_conf['host'],
                             port=airflow_pgsql_conf['port'],
                             database=airflow_pgsql_conf['database'])
        logger.info('Connect to postgresql database using psycopg2')
        
        # Create a cursor to perform database operations
        cursor = connection.cursor()
        logger.info('Create a cursor to perform database operations')
        # Read impala variable details
        impala_conf = Variable.get('impala_conf', deserialize_json=True)
        logger.info(f'Read impala variable details: {impala_conf}')
        
        impyla_conn = impalaconn(
            impala_conf['host'],
            impala_conf['port'],
            database=f"example_database",
            timeout = 120,
            auth_mechanism='GSSAPI')
        logger.info('Connect to Impala using impyla client with Kerberos auth')

        impyla_cursor = impyla_conn.cursor()
        logger.info('Create a cursor to perform impyla operations')

        rowsinbatch = 10000
        resultcnt = 1
        index = 0
        while resultcnt > 0:
            postgreSQL_select_Query = f"""select id, dag_id, cast((execution_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), state, run_id, 
                                          external_trigger, cast(conf as varchar(30)), cast((end_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), 
                                          cast((start_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), run_type, 
                                          cast((last_scheduling_decision::timestamptz AT TIME ZONE 'UTC') as varchar(30)), dag_hash, creating_job_id,
                                          cast((queued_at::timestamptz AT TIME ZONE 'UTC') as varchar(30))
                                          from dag_run 
                                          order by id
                                          limit {rowsinbatch} offset ({index} * {rowsinbatch})"""
            record = cursor.execute(postgreSQL_select_Query)
            # Fetch result
            record = cursor.fetchall()
            resultcnt = len(record)
            values = ",".join([f"({r[0]}, '{r[1]}', '{r[2]}', '{r[3]}', '{r[4]}', {r[5]}, '{r[6]}', '{r[7]}', '{r[8]}', '{r[9]}', '{r[10]}', '{r[11]}', '{r[12]}', '{r[13]}', now())".replace("'None'", 'NULL').replace("None", 'Null') for r in record])       
            if(index == 0):
                impyla_cursor.execute(f"drop table if exists example_database.ops_airflow_dag_run")
                impyla_cursor.execute(f"""create table if not exists example_database.ops_airflow_dag_run(
                                          id int comment 'unique key', 
                                          dag_id string comment 'the dag_id to find dag runs for', 
                                          execution_date timestamp comment 'the execution date', 
                                          state string comment 'the state of the dag run', 
                                          run_id string comment 'defines the run id for this dag run', 
                                          external_trigger boolean comment 'whether this dag run is externally triggered', 
                                          conf string comment 'pickled configuration object to trigger the dag run', 
                                          end_date timestamp comment 'end time of this dag run', 
                                          start_date timestamp comment 'start time of this dag run', 
                                          run_type string comment 'type of dag run',
                                          last_scheduling_decision timestamp comment 'When a scheduler last attempted to schedule TIs for this DagRun',
                                          dag_hash string comment 'Hash of Serialized DAG',
                                          creating_job_id string comment 'id of the job creating this DagRun',
                                          queued_at timestamp comment 'timestamp of dag status changed to queued',
                                          processed_ts timestamp comment 'timestamp of table data loading')
                                          comment 'dag_run table describes an instance of a Dag'
                                          STORED AS PARQUET
                                        """)
            if(resultcnt > 0):
                query = f"""INSERT INTO example_database.ops_airflow_dag_run(id, dag_id, execution_date, state, run_id, external_trigger, conf, end_date, start_date, 
                            run_type, last_scheduling_decision, dag_hash, creating_job_id, queued_at, processed_ts) VALUES """ + values
                impyla_cursor.execute(query)
            index += 1
        logger.info("dag_run table all loaded")            

        rowsinbatch = 10000
        resultcnt = 1
        index = 0
        while resultcnt > 0:
            postgreSQL_select_Query = f"""select dag_id, is_paused, is_subdag, is_active, cast((last_parsed_time::timestamptz AT TIME ZONE 'UTC') as varchar(30)), 
                                          cast((last_pickled::timestamptz AT TIME ZONE 'UTC') as varchar(30)), cast((last_expired::timestamptz AT TIME ZONE 'UTC') as varchar(30)), 
                                          scheduler_lock, pickle_id, fileloc, owners, description, default_view, schedule_interval, root_dag_id, 
                                          cast((next_dagrun::timestamptz AT TIME ZONE 'UTC') as varchar(30)),
                                          cast((next_dagrun_create_after::timestamptz AT TIME ZONE 'UTC') as varchar(30)),
                                          concurrency, has_task_concurrency_limits, max_active_runs
                                          from dag 
                                          order by dag_id
                                          limit {rowsinbatch} offset ({index} * {rowsinbatch})"""
            record = cursor.execute(postgreSQL_select_Query)
            # Fetch result
            record = cursor.fetchall()
            resultcnt = len(record)
            values = ",".join([f"('{r[0]}', {r[1]}, {r[2]}, {r[3]}, '{r[4]}', '{r[5]}', '{r[6]}', {r[7]}, {r[8]}, '{r[9]}', '{r[10]}', '{r[11]}', '{r[12]}', '{r[13]}', '{r[14]}', '{r[15]}', '{r[16]}', '{r[17]}', '{r[18]}', '{r[19]}', now())".replace("'None'", 'NULL').replace("None", 'Null') for r in record])        
            if(index == 0):
                impyla_cursor.execute(f"drop table if exists example_database.ops_airflow_dag")
                impyla_cursor.execute(f"""create table if not exists example_database.ops_airflow_dag(
                                          dag_id string comment 'the dag_id to find dag runs for', 
                                          is_paused boolean comment 'returns a boolean indicating whether this DAG is pause', 
                                          is_subdag boolean comment 'whether the DAG is a subdag', 
                                          is_active boolean comment 'whether that DAG was seen on the last DagBag load', 
                                          last_parsed_time timestamp comment 'the time last touched by the scheduler', 
                                          last_pickled timestamp comment 'last time this DAG was pickled', 
                                          last_expired timestamp comment 'time when the DAG last received a refresh signal', 
                                          scheduler_lock boolean comment 'whether (one of) the scheduler is scheduling this DAG at the moment', 
                                          pickle_id int comment 'id associated with the pickled version of this DAG', 
                                          fileloc string comment 'the location of the file containing the DAG object', 
                                          owners string comment 'represent the owners', 
                                          description string comment 'description of the dag', 
                                          default_view string comment 'default view of the dag inside the webserver', 
                                          schedule_interval string comment 'a parameter that can be set at the DAG level to define when that DAG will be run', 
                                          root_dag_id string comment 'root dag id of the dag', 
                                          next_dagrun timestamp comment 'The logical date of the next dag run',
                                          next_dagrun_create_after timestamp comment 'Earliest time at which this next_dagrun can be created',
                                          concurrency string comment 'deprecated, will be removed in airflow 3.0, pls use max_active_tasks then',
                                          has_task_concurrency_limits string comment 'whether has task concurrency limit',
                                          max_active_runs string comment 'defines how many running concurrent instances of a DAG there are allowed to be',
                                          processed_ts timestamp comment 'timestamp of table data loading')
                                          comment 'dag table to describe all dags'
                                          STORED AS PARQUET
                                       """)
            if(resultcnt > 0):
                query = f"""INSERT INTO example_database.ops_airflow_dag(dag_id, is_paused, is_subdag, is_active, last_parsed_time, last_pickled, last_expired, scheduler_lock, 
                            pickle_id, fileloc, owners, description, default_view, schedule_interval, root_dag_id, next_dagrun, next_dagrun_create_after,
                            concurrency, has_task_concurrency_limits, max_active_runs, processed_ts) VALUES """ + values
                impyla_cursor.execute(query)
            index += 1
        logger.info("dag table all loaded")     

        rowsinbatch = 10000
        resultcnt = 1
        index = 0
        while resultcnt > 0:
            postgreSQL_select_Query = f"""select id, task_id, dag_id, cast((execution_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), cast((start_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), 
                                          cast((end_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), duration 
                                          from task_fail 
                                          order by id
                                          limit {rowsinbatch} offset ({index} * {rowsinbatch})"""
            record = cursor.execute(postgreSQL_select_Query)
            # Fetch result
            record = cursor.fetchall()
            resultcnt = len(record)
            values = ",".join([f"({r[0]}, '{r[1]}', '{r[2]}', '{r[3]}', '{r[4]}', '{r[5]}', {r[6]}, now())".replace("'None'", 'NULL').replace("None", 'Null') for r in record])        
            if(index == 0):
                impyla_cursor.execute(f"drop table if exists example_database.ops_airflow_task_fail")
                impyla_cursor.execute(f"""create table if not exists example_database.ops_airflow_task_fail(
                                          id int comment 'unique key', 
                                          task_id string comment 'identifier for the task', 
                                          dag_id string comment 'the dag_id to find dag runs for', 
                                          execution_date timestamp comment 'the execution date', 
                                          start_date timestamp comment 'start time of this task run', 
                                          end_date timestamp comment 'end time of this task run', 
                                          duration int comment 'duration of the task running', 
                                          processed_ts timestamp comment 'timestamp of table data loading')
                                          comment 'task fail table to describe all task failure'
                                          STORED AS PARQUET
                                       """)
            if(resultcnt > 0):
                query = f"INSERT INTO example_database.ops_airflow_task_fail(id, task_id, dag_id, execution_date, start_date, end_date, duration, processed_ts) VALUES " + values
                impyla_cursor.execute(query)
            index += 1
        logger.info("task_fail table all loaded")        

        rowsinbatch = 10000
        resultcnt = 1
        index = 0
        while resultcnt > 0:
            postgreSQL_select_Query = f"""select id, dag_id, state, job_type, cast((start_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), cast((end_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), 
                                          cast((latest_heartbeat::timestamptz AT TIME ZONE 'UTC') as varchar(30)), executor_class, hostname, unixname 
                                          from job 
                                          order by id
                                          limit {rowsinbatch} offset ({index} * {rowsinbatch})"""
            record = cursor.execute(postgreSQL_select_Query)
            # Fetch result
            record = cursor.fetchall()
            resultcnt = len(record)
            values = ",".join([f"({r[0]}, '{r[1]}', '{r[2]}', '{r[3]}', '{r[4]}', '{r[5]}', '{r[6]}', '{r[7]}', '{r[8]}', '{r[9]}', now())".replace("'None'", 'NULL').replace("None", 'Null') for r in record])        
            if(index == 0):
                impyla_cursor.execute(f"drop table if exists example_database.ops_airflow_job")
                impyla_cursor.execute(f"""create table if not exists example_database.ops_airflow_job(
                                          id int comment 'unique key', 
                                          dag_id string comment 'the dag_id to find dag runs for', 
                                          state string comment 'the state of the dag run', 
                                          job_type string comment 'the type of job', 
                                          start_date timestamp comment 'start date of the job', 
                                          end_date timestamp comment 'end date of the job', 
                                          latest_heartbeat timestamp comment 'heartbeats update the job entry in the the database with a timestamp', 
                                          executor_class string comment 'executor type', 
                                          hostname string comment 'airflow worker host name', 
                                          unixname string comment 'unix user name', 
                                          processed_ts timestamp comment 'timestamp of table data loading')
                                          comment 'job table to describe all jobs'
                                          STORED AS PARQUET
                                      """)
            if(resultcnt > 0):
                query = f"INSERT INTO example_database.ops_airflow_job(id, dag_id, state, job_type, start_date, end_date, latest_heartbeat, executor_class, hostname, unixname, processed_ts) VALUES " + values
                impyla_cursor.execute(query)
            index += 1
        logger.info("job table all loaded")

        rowsinbatch = 10000
        resultcnt = 1
        index = 0
        while resultcnt > 0:
            postgreSQL_select_Query = f"""select task_id, dag_id, cast((execution_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), 
                                        cast((start_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), cast((end_date::timestamptz AT TIME ZONE 'UTC') as varchar(30)), 
                                        cast(duration as float), state, try_number, hostname, unixname, job_id, pool, queue, priority_weight, operator, 
                                        cast((queued_dttm::timestamptz AT TIME ZONE 'UTC') as varchar(30)), pid, max_tries, cast(executor_config as varchar(30)),
                                        pool_slots, queued_by_job_id, external_executor_id 
                                        from task_instance 
                                        order by dag_id, task_id, execution_date
                                        limit {rowsinbatch} offset ({index} * {rowsinbatch})"""
            record = cursor.execute(postgreSQL_select_Query)
            # Fetch result
            record = cursor.fetchall()
            resultcnt = len(record)
            values = ",".join([f"('{r[0]}', '{r[1]}', '{r[2]}', '{r[3]}', '{r[4]}', '{r[5]}', '{r[6]}', {r[7]}, '{r[8]}', '{r[9]}', {r[10]}, '{r[11]}', '{r[12]}', {r[13]}, '{r[14]}', '{r[15]}', {r[16]}, {r[17]}, '{r[18]}', {r[19]}, {r[20]}, '{r[21]}', now())".replace("'None'", 'NULL').replace("None", 'Null') for r in record])        
            if(index == 0):
                impyla_cursor.execute(f"drop table if exists example_database.ops_airflow_task_instance")
                impyla_cursor.execute(f"""create table if not exists example_database.ops_airflow_task_instance(
                                        task_id string comment 'identifier for the task', 
                                        dag_id string comment 'the dag_id to find dag runs for',
                                        execution_date timestamp comment 'the execution date', 
                                        start_date timestamp comment 'the start date of dag run', 
                                        end_date timestamp comment 'the end date of dag run', 
                                        duration string comment 'duration of dag running', 
                                        state string comment 'the state of the dag run', 
                                        try_number int comment 'the number of trying', 
                                        hostname string comment 'airflow worker host name', 
                                        unixname string comment 'unix user name', 
                                        job_id int comment 'the id of job', 
                                        pool string comment 'pool that task is associated with', 
                                        queue string comment 'queue that task gets assigned to', 
                                        priority_weight int comment 'task execution priority', 
                                        operator string comment 'operator class name', 
                                        queued_dttm timestamp comment 'timestamp when the task put in queue', 
                                        pid int comment 'process id', 
                                        max_tries int comment 'the maximum number of trying', 
                                        executor_config string comment 'executor configuration', 
                                        pool_slots int comment 'the number of slots occupied by a task',
                                        queued_by_job_id int comment 'the queued id of this job',
                                        external_executor_id string comment 'the identifier of the celery executor',
                                        processed_ts timestamp comment 'timestamp of table data loading')
                                        comment 'This table is the single source of truth around what tasks have run and the state they are in.'
                                        STORED AS PARQUET
                                      """)
            if(resultcnt > 0):
                query = f"""INSERT INTO example_database.ops_airflow_task_instance(task_id, dag_id, execution_date, start_date, end_date, duration, state, 
                            try_number, hostname, unixname, job_id, pool, queue, priority_weight, operator, queued_dttm, pid, 
                            max_tries, executor_config, pool_slots, queued_by_job_id, external_executor_id, processed_ts) VALUES """ + values
                impyla_cursor.execute(query)
            index += 1
        logger.info("task_instance table all loaded")
        

    except (Exception) as error:
        logger.error('something is wrong. error: %s', error)
    finally:
        #closing database connection
        if connection is not None:
            if cursor is not None:
                cursor.close()
            connection.close()
        if impyla_conn is not None:
            if impyla_cursor is not None:
                impyla_cursor.close()
            impyla_conn.close()

#define default and dag-specific arguments
default_args = {
   'owner': 'airflow',   
   'email': ['airflow@example.com']
}
            
#configure the arguments in dag
dag_pthon= DAG(
    dag_id="airflow_metadata_loading",
    default_args=args,
    schedule="00 2 * * *",
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
)

#set the tasks
hadoop_load_airflow_monitoring_task = PythonOperator(
    task_id="hadoop_load_airflow_monitoring",
    python_callable=airflow_dags_extract,
    dag=dag
)

hadoop_load_airflow_monitoring_task

