from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.bash import BashOperator


DATE_TODAY                  = date.today()
YESTERDAY_DATE              = DATE_TODAY - timedelta(days = 1)
CHANNEL_FOR_REPORT          = 't.me/timeline_prediction' # Link to Telegram chat for send reports
IMAGE_PATH                  = './Images/'
REAL_TIMELINE_CHART_FILE    = IMAGE_PATH + f'real_{YESTERDAY_DATE}.png'
PREDICTION_TL_CHART_FILE    = IMAGE_PATH + f'predict_{DATE_TODAY}.png'
CLEAR_ML_PROJECT_NAME       = 'MLOPS_task_4'
CLEAR_ML_TASK_LINK          = 'https://app.clear.ml/projects/d62afdcace204b0e9b9715b7c62fc82d/experiments'


args = {
    'owner': 'YARO',
    'start_date': datetime(2018, 11, 1),
    'provide_context': True
}


with DAG(CLEAR_ML_PROJECT_NAME + '_reports',
         description='Send the reports about temeline ML model works',
         schedule='0 9 * * *',
         catchup=False,
         default_args=args) as dag:

    task_1 = BashOperator(
        task_id="DVC_pull",
        bash_command='cd ~/parsr_scrypts && dvc pull --force')
    
    task_2 = BashOperator(
        task_id="Send_Hello",
        bash_command=f'cd ~/parsr_scrypts && python3 sender.py text Report_for_{DATE_TODAY} {CHANNEL_FOR_REPORT}')
    
    task_3 = BashOperator(
        task_id="Send_real_timeline_for_yesterday",
        bash_command=f'cd ~/parsr_scrypts && python3 sender.py file {REAL_TIMELINE_CHART_FILE} {CHANNEL_FOR_REPORT}')

    task_4 = BashOperator(
        task_id="Send_link_of_ClearML_task",
        bash_command=f'cd ~/parsr_scrypts && python3 sender.py text {CLEAR_ML_TASK_LINK} {CHANNEL_FOR_REPORT}')
 
    task_5 = BashOperator(
        task_id="Send_new_predict_timeline",
        bash_command=f'cd ~/parsr_scrypts && python3 sender.py file {PREDICTION_TL_CHART_FILE} {CHANNEL_FOR_REPORT}')


    task_1 >> task_2 >> task_3 >> task_4 >> task_5