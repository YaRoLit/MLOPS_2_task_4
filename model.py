import sys
from datetime import timedelta, date

import pandas as pd
import numpy as np
import pickle
import matplotlib.pyplot as plt

from sklearn.neighbors import KNeighborsRegressor
from sktime.forecasting.compose import make_reduction
from sktime.forecasting.base import ForecastingHorizon
from sktime.performance_metrics.forecasting import MeanAbsolutePercentageError

from clearml import Task


DATE_TODAY      = date.today()
YESTERDAY_DATE  = DATE_TODAY - timedelta(days = 1)
IMAGE_PATH      = './Images/'
HOURS           = np.arange(0, 24)

smape = MeanAbsolutePercentageError(symmetric = True) 


def get_yesterday_messages_cnt(df: pd.DataFrame) -> list:
    '''Get yesterday messages'''

    df_yesterday_mess = df[df.datetime.dt.date == YESTERDAY_DATE]

    messages_cnt = df_yesterday_mess.txt_cnt.to_numpy()

    messages_cnt = np.concatenate(
        (messages_cnt, np.zeros(24 - len(messages_cnt))),
        axis=None)

    return messages_cnt


def check_metrics(y_val: np.array, predict_file: str) -> tuple:
    '''Check the model metric'''

    y_pred = pickle.load(open(predict_file, 'rb'))

    return smape(y_pred, y_val)


def remake_model(df: pd.DataFrame, model_file: str) -> make_reduction:
    '''Remake the model for timeline prediction'''

    y = df.txt_cnt

    regressor  = KNeighborsRegressor(n_neighbors=1)

    forecaster = make_reduction(regressor, **params)
    
    forecaster.fit(y)

    pickle.dump(forecaster, open(model_file, 'wb'))

    return forecaster


def new_predict(model_file: str, predict_file: str) -> pd.Series:
    '''Make the prediction for new day'''

    fh = ForecastingHorizon(np.arange(1, 25))

    model = pickle.load(open(model_file, 'rb'))

    y_pred = np.array(model.predict(fh))

    pickle.dump(y_pred, open(predict_file, 'wb'))

    return y_pred


if __name__ == "__main__":
    try:
        all_messages_file = sys.argv[1]
        model_file = sys.argv[2]
        predict_file = sys.argv[3]
        project_name = sys.argv[4]

    except:
        raise ValueError ('''Please use correct parameters:
            [1]-all mesages file name
            [2]-model file name
            [3]-predict file name''')

    task = Task.init(
        project_name=project_name,
        task_name=str(DATE_TODAY)
        )

    params = {}
    params['window_length']     = 24 * 7
    params['strategy']          = 'recursive'
    task.connect(params)

    df = pd.read_csv(
        all_messages_file,
        sep='\t',
        encoding='utf-8',
        parse_dates=['datetime'])
    
    df = df[df.datetime.dt.date != date.today()]
    
    yesterday_messages_timeline = get_yesterday_messages_cnt(df)

    score = check_metrics(
        yesterday_messages_timeline,
        predict_file)
    task.get_logger().report_scalar(
        title='sMAPE', 
        series = 'train', 
        value=score, 
        iteration=0)

    plt.bar(HOURS, yesterday_messages_timeline, color='g') 
    plt.title(f'Real timeline for {YESTERDAY_DATE}') 
    plt.ylabel('Count of messages') 
    plt.xlabel('Hours')
    img_file = IMAGE_PATH + f'real_{YESTERDAY_DATE}.png'  
    plt.savefig(img_file)
    plt.close()

    model = remake_model(df, model_file)
    task.upload_artifact(
        name='timeline_model',
        artifact_object=model)

    y_pred = new_predict(model_file, predict_file)
    task.upload_artifact(
        name=f'y_pred_{DATE_TODAY}',
        artifact_object=y_pred)
    
    plt.bar(HOURS, y_pred, color='b') 
    plt.title(f'Timeline predict for {DATE_TODAY}') 
    plt.ylabel('Count of messages') 
    plt.xlabel('Hours')  
    img_file = IMAGE_PATH + f'predict_{DATE_TODAY}.png'
    plt.savefig(img_file)
    plt.close()


    task.close()