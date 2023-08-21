# developer-- Yasin Mamat
# Email Id- yasin.mamat@workday.com

from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators import SimpleHttpOperator, HttpSensor, BashOperator, EmailOperator, S3KeySensor, PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.s3_delete_objects_operator import S3DeleteObjectsOperator
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow import configuration
from airflow.models import Variable
from airflow.hooks import PostgresHook
from datetime import datetime, date, timedelta, timezone
from dateutil.relativedelta import relativedelta

from requests.auth import HTTPBasicAuth
import requests
import pandas as pd
from pandas.io.json import json_normalize
from io import StringIO
import boto3
import json
import sys

current_date = datetime.now().strftime('%Y-%m-%d')
ENV = Variable.get('ENV')
source = 'WOW'
job_name ='WD_'+ source + '_Sales_AE_RSD_Data_Load'
Schedule = Variable.get(source + '_Schedule')
#Schedule = None
ops_bucket_nm = Variable.get('operations_bucket_name')
ssh_conn_id = 'SSH_EMR_' + ENV
Sender = configuration.conf.get('smtp', 'smtp_mail_from')
Recipients_Fl =['IS-BI-Support@workday.com']
Recipients_SC =['IS-BI-Support@workday.com'] 
ds_job_dir = '/home/hadoop/ds_jobs'
log_dir = '/home/hadoop/ds_logs'
log_date = 'log_' + current_date
sn_pwd = Variable.get('snow_secret_key')
sn_api_url = Variable.get('snow_api_url')
wow_ae_rsd_username = Variable.get('wow_ae_rsd_username')
wow_password_ae_rsd = Variable.get('wow_password_ae_rsd')
redshift_conn_id = 'redshift_ccr'
region_name = 'us-west-2'
pipeline_schema = 'ctrl'
pipeline_tb = 'ds_pipeline_list'
pipeline_table = pipeline_schema + '.' + pipeline_tb
ds_metadata_table=pipeline_schema + '.' + 'ds_metadata'
ds_src_obj_list_table = pipeline_schema + '.' + 'ds_src_obj_list'
start_status = 'STARTS'
completed_status = 'COMPLETED'
spark_job_configs = Variable.get('spark_job_configs')

current_date=datetime.now()
bucket_key=current_date.strftime('%m%d%Y')

############ S3 bucket credentials ###########################
ops_bucket_name = Variable.get('operations_bucket_name')
ops_access_key = Variable.get('ops_access_key')
ops_secret_key = Variable.get('ops_secret_key')
cur_bucket_name = Variable.get('cur_bucket_name')
cur_access_key = Variable.get('cur_access_key')
cur_secret_key = Variable.get('cur_secret_key')
raw_bucket_name = Variable.get('raw_bucket_name')
raw_access_key = Variable.get('raw_access_key')
raw_secret_key = Variable.get('raw_secret_key')

# SMTP Configurations
EMAIL_HOST = configuration.conf.get('smtp', 'smtp_host')
EMAIL_HOST_USER = configuration.conf.get('smtp', 'smtp_user')  # Replace with your SMTP username
EMAIL_HOST_PASSWORD = configuration.conf.get('smtp', 'smtp_password')  # Replace with your SMTP password
EMAIL_PORT = configuration.conf.get('smtp', 'smtp_port')



# set up basic logging
logger = logging.getLogger()


def _write_log(msg, error=False):
    """
    Logging Enabled
    """
    if msg is not None:
        if not error:
            logging.info("{} Job ===> {} \n".format(job_name, msg))
        else:
            logging.error("{} Job ===> {} \n".format(job_name, msg))


def execute_redshift_query(sql_query):
    try:
        pg_hook = PostgresHook(redshift_conn_id)
        results = pg_hook.run(sql_query)
        return results
    except Exception as e:
        _write_log(msg=str(e), error=True)
        _write_log("Failed to Connect & Execute Query on Redshift")
        sys.exit(1)


def get_redshift_query_records(sql_query):
    try:
        pg_hook = PostgresHook(redshift_conn_id)
        results = pg_hook.get_records(sql_query)
        return results
    except Exception as e:
        _write_log(msg=str(e), error=True)
        _write_log("Failed to Connect & Get Records from query on Redshift")
        sys.exit(1)

extract_raw_object_query = '''
select object_name from {ds_src_obj_list_table} where active_flag = 'true' and object_name in '('ae_rsd')'  
order by object_name asc
'''.format(ds_src_obj_list_table=ds_src_obj_list_table)
WOW_Raw_List = get_redshift_query_records(extract_raw_object_query)
_write_log("Raw Object List: {}".format(WOW_Raw_List))

extract_ent_object_query = '''
select object_name from {ds_metadata_table} where active_flag = 'true' and source_system='{source_system}' 
and object_name like 'ent_%' order by object_name asc
'''.format(ds_metadata_table=ds_metadata_table, source_system=source)
WOW_Sales_AE_Ents_List = get_redshift_query_records(extract_ent_object_query)
_write_log("Entity List: {}".format(WOW_Sales_AE_Ents_List))


WOW_api_url = 'https://wd5-services1.myworkday.com/ccx/service/customreport2/workday/Sales_AE_Prism_ISU/'


# list of dictionary of task details
api_details = [
    {
        "task_name": "ae_rsd",
        "task_details": {
            "api": WOW_api_url + "Sales_AE_RSD_Competency_Form__Full_Extract_?Career___Progress_Check-In_Templates!WID=941a824270db019285b6cfd35615a6e9!7ad208cd53530179d1063137671e66fc!3018556062f001d519184c36190e9baf!94b93a3bb8810158984f19a9591545dc&Organizations!WID=84a5a2213bc01056864b52938b7e0cd9&Include_Subordinate_Organizations=1&format=csv",
            "config": {"output_file": "ae_rsd.csv"}
        }
    }
]


def extract_load_datetime(obj_nm):
    try:
        load_datetime_query = '''
        SELECT load_date FROM {pipeline_table} where source_system in ('WOW') and object_name = '{obj_name}' and pipeline_name='source_to_raw'
        '''.format(obj_name=obj_nm, pipeline_table=pipeline_table)
        results = get_redshift_query_records(load_datetime_query)
        load_datetime = results[0]
        return load_datetime
    except Exception as e:
        _write_log(msg=str(e), error=True)
        _write_log("Failed to Extract Load datetime from pipeline table")
        sys.exit(1)



def update_pipeline_table(load_status, object_name):
    """
    Update Pipeline Status
    """
    try:
        if load_status == 'COMPLETED':
            pipeline_query = """
            update {pipeline_table}
            set
            load_status = '{load_status}',
            etl_end_date = sysdate,
            load_date = sysdate
            from
            {pipeline_table}
            where source_system = '{source}' and object_name = '{object_name}' and pipeline_name='source_to_raw'
            """.format(pipeline_table=pipeline_table,
                       load_status=load_status,
                       object_name=object_name,
                       source=source)
        elif load_status == 'IN PROCESS':
            pipeline_query = """
            update {pipeline_table}
            set
            load_status = '{load_status}',
            etl_start_date = sysdate,
            etl_end_date = sysdate
            from
            {pipeline_table}
            where source_system = '{source}' and object_name = '{object_name}' and pipeline_name='source_to_raw'
            """.format(pipeline_table=pipeline_table,
                       load_status=load_status,
                       object_name=object_name,
                       source=source)
        else:
            pipeline_query = """
            update {pipeline_table}
            set
            load_status = '{load_status}',
            etl_end_date = sysdate
            from
            {pipeline_table}
            where source_system = '{source}' and object_name = '{object_name}' and pipeline_name='source_to_raw'
            """.format(pipeline_table=pipeline_table,
                       load_status=load_status,
                       object_name=object_name,
                       source=source)
        results = execute_redshift_query(pipeline_query)
        _write_log("Updated Ctrl Pipeline Table!!")
        return results
    except Exception as e:
        _write_log(msg=str(e), error=True)
        _write_log("Failed to Update pipeline table")
        sys.exit(1)




default_args = {
    'owner': 'Data Services',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_success': False,
    'email_on_retry': False
}


def ods_cleanup():
    try:
        _write_log('>>>>>>> ODS Cleanup initiated <<<<<<<<<')
        s3 = boto3.resource('s3', region_name=region_name,
                            aws_access_key_id=cur_access_key,
                            aws_secret_access_key=cur_secret_key)
        bucket = s3.Bucket(cur_bucket_name)
        bucket.objects.filter(Prefix=source + '/ODS/').delete()

    except Exception as e:
        _write_log(msg=str(e), error=True)
        _write_log('Error while cleaning {src}/ODS: ' + str(e).format(src=source))
        sys.exit(1)


def load_type_update(ip_load_stts):
    try:
        raw_src_sys = source + '_RAW'

        if ip_load_stts == 'STARTS':

            ods_cleanup()

            _write_log('>>>>>>> Updating ds_load_type as STARTS <<<<<<<<<')
            sql_query = '''
            update {tbl_name} set load_status = '{load_status}' , etl_updated_date = sysdate where source_system = '{source_system}' and active_flag = '{flag}'
            '''.format(tbl_name='ctrl.ds_load_type', load_status=ip_load_stts, source_system=raw_src_sys, flag='true')

        elif ip_load_stts == 'COMPLETED':

            max_load_dt_qry = '''
            select max(load_date) as max_load_dt from {tblname} where source_system = '{src_sys}' and pipeline_name= 's3_raw_to_curated'
            '''.format(tblname='ctrl.ds_pipeline_list', src_sys=source)
            max_date = get_redshift_query_records(max_load_dt_qry)

            _write_log('>>>>>>> max load date is {} <<<<<<<<<'.format(max_date[0][0]))

            if max_date:
                _write_log('>>>>>>> Updating ds_load_type as COMPLETED <<<<<<<<<')
                sql_query = '''
                update {tbl_name} set load_status = '{load_status}', last_rundatetime = '{rundatetime}',etl_updated_date = sysdate
                where source_system = '{source_system}' and active_flag = '{flag}'
                '''.format(tbl_name='ctrl.ds_load_type', load_status=ip_load_stts, source_system=raw_src_sys,
                           flag='true', rundatetime=max_date[0][0])
            else:
                _write_log('load date is empty, check the pipeline table', error=True)
                sys.exit(1)

        execute_redshift_query(sql_query)
        _write_log("{status} - Load status updated".format(status=ip_load_stts))

    except Exception as e:
        _write_log(msg=str(e), error=True)
        _write_log("Failed while updating load status on ds_load_type table")
        sys.exit(1)


def send_curated_failure_email(task_id):
    msg = MIMEMultipart('alternative')
    msg['From'] = Sender
    msg['To'] = ', '.join(Recipients_Fl)

    file_html = """<html><head>
                     <style>table,th,td{border: 1px solid black;border-collapse: collapse;} th,td{padding: 5px;text-align: left;}</style>
                     </head><body>
                     <p>Hi Team,</p>
                     <p>Please check the log file for more details:</p>
                     <table style="width:100%">
                       <tr style="background-color:#ffff99">
                         <th colspan="2">Execution Details</th>
                       </tr>
                       <tr>
                         <td style="width:20%"><i>Pipeline Name</i></td>
                         <td>""" + job_name + """</td>
                       </tr>
                         <tr>
                         <td><i>File_Name</i></td>
                         <td>""" + task_id + """</td>
                       </tr>
                       <tr>
                         <td><i>Log_Path</i></td>
                         <td>""" + log_dir + "/" + "curated_" + task_id + "." + log_date + """</td>
                       </tr>
                     </table>
                     <br/><br/>
                     <p>Regards,</p>
                     Support Team
                     </body></html>"""

    msg['Subject'] = "Workday-{1}-{0}: Data file Error for {2}".format(ENV, source, task_id)
    mime_text = MIMEText(file_html, 'html')
    msg.attach(mime_text)
    s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
    s.starttls()
    s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    s.sendmail(msg['From'], Recipients_Fl, msg.as_string())
    s.quit()


        
def success_email_notification(context):
    """
    Send success email alerts.
    """
    task_instance = context['task_instance']
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Workday-{1}-{0}: Data Load Completed.".format(ENV, source)
    msg['From'] = Sender
    msg['To'] = ', '.join(Recipients_SC)

    html = """<html>
        <head></head>
        <body>
          <h1>WOW Data load Completed Successfully.</h1>
        </body>
        </html>
            """
    mime_text = MIMEText(html, 'html')
    msg.attach(mime_text)
    s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
    s.starttls()
    s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    s.sendmail(msg['From'], Recipients_SC, msg.as_string())
    s.quit()


def create_sn_ticket(task):
    """
    Generate a service now ticket
    """

    # Eg. User name="admin", Password="admin" for this code sample.
    user = 'airflow.integration'

    # Set proper headers
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    description = "{job_name} Airflow Job Failed for {task}".format(job_name=job_name, task=task)
    short_description = "{job_name} Airflow Job Failed for {task}. Please check the log for more details.".format(
        job_name=job_name, task=task)

    # Do the HTTP request
    response = requests.post(sn_api_url, auth=(user, sn_pwd), headers=headers,
                             json={"assignment_group": "Enterprise BT Analytics Engineering",
                                   "caller_id": "",
                                   "description": description,
                                   "category": "",
                                   "short_description": short_description,
                                   "contact_type": "Integration",
                                   "business_service": "8a6c83f4db179014b0d5abc5ca96197c"})

    # Check for HTTP codes other than 200
    if response.status_code != 201:
        _write_log('Status: {}'.format(response.status_code))
        _write_log('Headers: {}'.format(response.headers))
        _write_log('Error Response: {}'.format(response.json()))
        sys.exit()

    # Decode the JSON response into a dictionary and use the data
    data = response.json()
    _write_log(data)


def send_failure_email(type, obj_nm):
    msg = MIMEMultipart('alternative')
    msg['From'] = Sender
    msg['To'] = ', '.join(Recipients_Fl)

    df_html = """<html><head>
                     <style>table,th,td{border: 1px solid black;border-collapse: collapse;} th,td{padding: 5px;text-align: left;}</style>
                     </head><body>
                     <p>Hi Team,</p>
                     <p>Please check the log file for more details:</p>
                     <table style="width:100%">
                       <tr style="background-color:#ffff99">
                         <th colspan="2">Execution Details</th>
                       </tr>
                       <tr>
                         <td style="width:20%"><i>Pipeline Name</i></td>
                         <td>""" + job_name + """</td>
                       </tr>
                         <tr>
                         <td><i>Table_Name</i></td>
                         <td>""" + type + "_" + obj_nm + """</td>
                       </tr>
                       <tr>
                         <td><i>Log_Path</i></td>
                         <td>""" + "s3://" + ops_bucket_name + "/logs/" + type + "_logs/" + type + "_" + obj_nm + ".log" + """</td>
                       </tr>
                     </table>
                     <br/><br/>
                     <p>Regards,</p>
                     Support Team
                     </body></html>"""

    aggr_html = """<html><head>
                     <style>table,th,td{border: 1px solid black;border-collapse: collapse;} th,td{padding: 5px;text-align: left;}</style>
                     </head><body>
                     <p>Hi Team,</p>
                     <p>Please check the log file for more details:</p>
                     <table style="width:100%">
                       <tr style="background-color:#ffff99">
                         <th colspan="2">Execution Details</th>
                       </tr>
                       <tr>
                         <td style="width:20%"><i>Pipeline Name</i></td>
                         <td>""" + job_name + """</td>
                       </tr>
                         <tr>
                         <td><i>Table_Name</i></td>
                         <td>""" + obj_nm + """</td>
                       </tr>
                       <tr>
                         <td><i>Log_Path</i></td>
                         <td>""" + "s3://" + ops_bucket_name + "/logs/" + type + "_logs/" + obj_nm + ".log" + """</td>
                       </tr>
                     </table>
                     <br/><br/>
                     <p>Regards,</p>
                     Support Team
                     </body></html>"""

    if type == 'aggregate':
        msg['Subject'] = "WorkdayBI{0}-{2}: Data Load Failed for {1}".format(ENV, obj_nm, source)
        mime_text = MIMEText(aggr_html, 'html')
    else:
        msg['Subject'] = "WorkdayBI{0}-{2}: Data Load Failed for {1}".format(ENV, obj_nm, source)
        mime_text = MIMEText(df_html, 'html')
    msg.attach(mime_text)
    s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
    s.starttls()
    s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    s.sendmail(msg['From'], Recipients_Fl, msg.as_string())
    s.quit()




def failure_email_notification(type, object_nm):
    """
    Send failure email alerts.
    """
    send_failure_email(type, object_nm)
    raise ValueError('{} Task Failed, Please check the main load logs'.format(object_nm))



def failure_email_and_create_sn_tk(type, object_nm):
    """
    Send failure email and create service now ticket.
    """
    send_failure_email(type, object_nm)
    create_sn_ticket(type, object_nm)
    raise ValueError('{} Task Failed, Please check the main load logs'.format(object_nm))


def failure_curated_email_notification(context):
    """
    Send failure email alerts.
    """
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    send_curated_failure_email(task_id)



def pipeline_result(type, obj_name):
    try:
        if type == 'aggregate':
            pipeline_query = '''
            SELECT load_status FROM {pipeline_table}
            where object_name = '{obj_name}' and source_system = '{source}'
            '''.format(obj_name=obj_name, ctrl_schema=pipeline_schema, source=source, pipeline_table=pipeline_table)
        else:
            pipeline_query = '''
            SELECT load_status FROM {pipeline_table}
            where object_name = '{type}_{obj_name}' and source_system = '{source}'
            '''.format(obj_name=obj_name, type=type, ctrl_schema=pipeline_schema, source=source,
                       pipeline_table=pipeline_table)
        results = get_redshift_query_records(pipeline_query)
        status_result = results[0][0]
        _write_log("Extracted Status from Pipeline table. Load Status: {}".format(status_result))
        return status_result
    except Exception as e:
        _write_log(msg=str(e), error=True)
        _write_log("Failed to Extract Status from ds Pipeline Table")
        sys.exit(1)


def check_load_status(type, obj_name):
    if pipeline_result(type, obj_name) == 'COMPLETED' \
            or pipeline_result(type, obj_name) == 'COMPLETED—No File To Load' \
            or pipeline_result(type, obj_name) == 'IN PROCESS':
        _write_log('{}: {} and Source: {}'.format(type, obj_name, source))
        return 'DATA_LOAD_COMPLETED_{}_{}'.format(type, obj_name)
    elif pipeline_result(type, obj_name) == 'FAILED' \
            or pipeline_result(type, obj_name) == 'Failed':
        _write_log('{}: {} and Source: {}'.format(type, obj_name, source))
        return 'Failed_notification_{}_{}'.format(type, obj_name)

def update_audit_log(file_name,load_date,rec_count):

    s3_path="s3:///"+raw_bucket_name+"/"+source+"/"+bucket_key+"/"
    try:
        insert_audit_log = '''
        INSERT INTO ctrl.ds_audit_log (load_type,load_stage,pipeline_name,source_system,object_name,s3_bucket,load_date,load_hour,row_count) 
        VALUES
        ('{load_type}','source_to_raw','{job_name}','{source}','{object_name}','{s3_path}','{load_date}','{load_hour}',{record_count}) ; commit;
        '''.format(load_type="INCR",job_name=job_name, source=source,object_name=file_name.replace(".csv",""),s3_path=s3_path,load_date=load_date,load_hour="0",record_count=rec_count)
        execute_redshift_query(insert_audit_log)

    except Exception as e:
        _write_log(msg=str(e), error=True)
        _write_log("Failed to insert record in audit log table")
        sys.exit(1)
        
        

def process_row(bucket_nm, task_details, **kwargs):
    """
    process_row function performs the belows tasks
    1. Construct the API by the iterating over the dictionary
    2. Perform the GET requests to pull data from API
    3. Upload the data to S3 location

    :param task_details: dictionary of parameters for the API
    """

    _write_log('Task Details: {}'.format(task_details))
   

    #Airflow execution date
    Last_Functionally_Updated_end_Date=current_date.strftime('%Y-%m-%d') #str(kwargs["execution_date"]).split("T")[0]
    _write_log('Executed date: {}'.format(Last_Functionally_Updated_end_Date))

    Last_Functionally_Updated_Start_Date=extract_load_datetime(task_details["task_name"])[0].strftime('%Y-%m-%d')
 
    _write_log('Load Executed date from pipleine table: {}'.format(Last_Functionally_Updated_end_Date))

    # Construct the API by processing dictionary
    api_header = task_details["task_details"]["api"]
    _write_log('API Header: {}'.format(api_header))

    if task_details["task_name"] == "Payment_details":
        api_endpoint = api_header + "&" + "Invoice_Date_From={0}&Invoice_Date_To={1}" \
                                          "&format=csv".format(Last_Functionally_Updated_Start_Date, Last_Functionally_Updated_end_Date)

    elif task_details["task_name"] == "Forecast_details":
        api_endpoint = api_header + "&" + "Last_Functionally_Updated_Start_Date={0}" \
                                          "&Last_Functionally_Updated_End_Date={1}&format=csv".format(
            Last_Functionally_Updated_Start_Date, Last_Functionally_Updated_end_Date)
    elif task_details["task_name"] == "Timesheet_details":
        api_endpoint = api_header + "&Last_Functionally_Updated_Start_Date={0}" \
                                          "&Last_Functionally_Updated_End_Date={1}&format=csv".format(
            Last_Functionally_Updated_Start_Date, Last_Functionally_Updated_end_Date)#,start_date,end_date)
    else:
        api_endpoint = api_header

    _write_log('API EndPoint: {}'.format(api_endpoint))

    file_name = task_details["task_details"]["config"]["output_file"]
    s3_key = source+"/"+bucket_key+"/" + file_name

    s3 = boto3.client("s3", region_name=region_name, aws_access_key_id=raw_access_key,
                      aws_secret_access_key=raw_secret_key)

    # Pulls the data from API by a GET requests
    response = requests.get(api_endpoint, auth=HTTPBasicAuth(wow_ae_rsd_username, wow_password_ae_rsd))
    data = response.text
    if response.status_code!=200:
        print("response status code: "+str(response.status_code))
        raise Exception("Failed to get data from WOW PSA API Call")
    if data is not None and response.status_code==200:
        # Creates a boto3 s3 client request & Posts the data to the s3 location
        s3.put_object(Body=data, Bucket=bucket_nm, Key=s3_key)
        rec_count=data.count('\n')-1
        load_date=current_date.strftime('%Y-%m-%d')
        update_audit_log(file_name,load_date,rec_count)
        _write_log("Data Ingested in Raw s3 for {0}".format(task_details["task_name"]))
    else:
        _write_log("No Data To be Ingested for {0}".format(task_details["task_name"]))


with DAG(dag_id=job_name,
         default_args=default_args,
         schedule_interval=Schedule,
         max_active_runs=1
         ) as dag:
    Start_Data_Processing = DummyOperator(task_id='Start_Data_Processing')
    # Start_Load_DIMS = DummyOperator(task_id='Start_Load_DIMS')
    # Start_Load_FACTS = DummyOperator(task_id='Start_Load_FACTS')
    # Start_Load_REFS = DummyOperator(task_id='Start_Load_REFS')
    Start_Load_ENTS = DummyOperator(task_id='Start_Load_ENTS')


    def DynamicUpdateInProcessPipeline(obj_nm):
        task = PythonOperator(
            task_id='ProcessRawPipeline_{0}'.format(obj_nm),
            python_callable=update_pipeline_table,
            op_kwargs={"load_status": "IN PROCESS", "object_name": obj_nm},
            on_failure_callback=failure_email_notification
        )
        return task

    def DynamicWOWDataLoad(obj_nm):
        task = PythonOperator(
            task_id=obj_nm,
            provide_context=True,
            python_callable=process_row,
            op_kwargs={'bucket_nm': raw_bucket_name, 'task_details': api},
            on_failure_callback=failure_email_notification
        )
        return task


    def DynamicUpdateCompletedPipeline(obj_nm):
        task = PythonOperator(
            task_id='CompleteRawPipeline_{0}'.format(obj_nm),
            python_callable=update_pipeline_table,
            op_kwargs={"load_status": "COMPLETED", "object_name": obj_nm},
            on_failure_callback=failure_email_notification
        )
        return task

    WOW_Sales_AE_Raw_Load_Type_Update_Start = PythonOperator(
        task_id="WOW_Sales_AE_Raw_Load_Type_Update_Start",
        python_callable=load_type_update,
        op_kwargs={"ip_load_stts": 'STARTS'},
        on_failure_callback=failure_email_notification,
        dag=dag
    )
    Raw_2_Curated = [SSHOperator(
        task_id="Curated_{raw}".format(raw=raw[0].strip()),
        ssh_conn_id='{0}'.format(ssh_conn_id),
        command='/usr/lib/spark/bin/spark-submit \
            --name {raw}\
            {spark_job_configs} {ds_job_dir}/raw_2_curated.py {ops_bucket_nm} {source} {raw} > {log_dir}/curated_{raw}.{log_date} 2>&1'.format(
            raw=raw[0].strip(),
            log_date=log_date,
            ds_job_dir=ds_job_dir,
            log_dir=log_dir,
            source=source,
            ops_bucket_nm=ops_bucket_nm,
            spark_job_configs=spark_job_configs),
        on_failure_callback=failure_curated_email_notification
    ) for raw in WOW_Raw_List]

    WOW_Sales_AE_Raw_Load_Type_Update_Completed = PythonOperator(
        task_id="WOW_Sales_AE_Raw_Load_Type_Update_Completed",
        python_callable=load_type_update,
        op_kwargs={"ip_load_stts": 'COMPLETED'},
        on_failure_callback=failure_email_notification,
        dag=dag
    )

    def DynamicEntLoad(ent):
        task = SSHOperator(
            task_id="{ent}".format(ent=ent[0].strip()),
            ssh_conn_id='{0}'.format(ssh_conn_id),
            command='sh {ds_job_dir}/run_dim_fact.sh \
             {ds_job_dir} {log_dir} {ops_bucket_nm} {source} ent {ent} > {log_dir}/ent_logs/{ent}.logs 2>&1'.format(
                ent=ent[0].strip().split('ent_')[1],
                ds_job_dir=ds_job_dir,
                log_dir=log_dir,
                source=source,
                ops_bucket_nm=ops_bucket_nm)
        )
        return task

    def DynamicCheckStatus(type, object_nm):
        task = BranchPythonOperator(
            task_id='Check_Status_{}_{}'.format(type, object_nm),
            python_callable=check_load_status,
            op_kwargs={"type": type, "obj_name": object_nm}
        )
        return task

    def DynamicFailedTask(type, object_nm):
        task = PythonOperator(
            task_id='Failed_notification_{type}_{object_nm}'.format(type=type, object_nm=object_nm),
            provide_context=False,
            python_callable=failure_email_notification,
            retries=0,
            op_kwargs={'type': type, 'object_nm': object_nm}
        )
        return task


    def DynamicCompletedTask(type, object_nm):
        task = DummyOperator(task_id='DATA_LOAD_COMPLETED_{}_{}'.format(type, object_nm))
        return task

    Data_Load_Completed = DummyOperator(task_id='Data_Load_Completed',
                                        on_success_callback=success_email_notification)

    for api in api_details:
        UpdateRawInProcessPipeline = DynamicUpdateInProcessPipeline(api["task_name"])
        WowRawLoad = DynamicWOWDataLoad(api["task_name"])
        UpdateRawCompletedPipeline = DynamicUpdateCompletedPipeline(api["task_name"])
        Start_Data_Processing >> UpdateRawInProcessPipeline
        UpdateRawInProcessPipeline >> WowRawLoad >> UpdateRawCompletedPipeline
        UpdateRawCompletedPipeline >> WOW_Sales_AE_Raw_Load_Type_Update_Start >> Raw_2_Curated >> WOW_Sales_AE_Raw_Load_Type_Update_Completed 

    for ent in WOW_Sales_AE_Ents_List:
        EntLoad = DynamicEntLoad(ent)
        Ent_check_status = DynamicCheckStatus('ent', ent[0].strip().split('ent_')[1])
        FailedEntLoad = DynamicFailedTask('ent', ent[0].strip().split('ent_')[1])
        CompletedEntLoad = DynamicCompletedTask('ent', ent[0].strip().split('ent_')[1])
        WOW_Sales_AE_Raw_Load_Type_Update_Completed >> Start_Load_ENTS >> EntLoad >> Ent_check_status >> [FailedEntLoad, CompletedEntLoad]
        CompletedEntLoad >> Data_Load_Completed