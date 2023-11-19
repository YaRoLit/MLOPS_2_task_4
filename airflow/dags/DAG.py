from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


MSG_LIMIT               = 1000                                  # Number messages for reading
LINK                    = 'https://t.me/+KxlX36pb-3hjMjRi'      # Link to Telegram chat for parsing
TMP_FILE                = './Data/Messages/tmp_mess.csv'
ALL_MESSAGES_FILE       = './Data/Messages/all_messages.csv'
ALL_MESSAGES_CNT_FILE   = './Data/Messages/all_messages_cnt.csv'
MODEL_FILE              = './Data/Models/model.pkl'
PREDICT_FILE            = './Data/Predicts/predict.mtr'
CLEAR_ML_PROJECT_NAME   = 'MLOPS_task_4'


args = {
    'owner': 'YARO',
    'start_date': datetime(2018, 11, 1),
    'provide_context': True
}


with DAG('MLOPS_task_4',
         description='Model for analyzing Telegram chat activity',
         schedule='1 0 * * *',
         catchup=False,
         default_args=args) as dag:

    task_1 = BashOperator(
        task_id="DVC_pull",
        bash_command='cd ~/parsr_scrypts && dvc pull --force')
    
    task_2 = BashOperator(
        task_id="parsr_chat",
        bash_command=f'cd ~/parsr_scrypts && python3 T_parsr.py {LINK} {MSG_LIMIT} {TMP_FILE}')
    
    task_3 = BashOperator(
        task_id="data_preprocessing",
        bash_command=f'cd ~/parsr_scrypts && python3 preproc.py {ALL_MESSAGES_FILE} {TMP_FILE}')

    task_4 = BashOperator(
        task_id="model_ops",
        bash_command=f'cd ~/parsr_scrypts && python3 model.py {ALL_MESSAGES_CNT_FILE} {MODEL_FILE} {PREDICT_FILE} {CLEAR_ML_PROJECT_NAME}')
 
    task_5 = BashOperator(
        task_id="DVC_push",
        bash_command='cd ~/parsr_scrypts && dvc commit --force && dvc push')


    task_1 >> task_2 >> task_3 >> task_4 >> task_5