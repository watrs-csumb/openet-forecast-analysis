from sklearn.metrics import mean_absolute_error, mean_squared_error

import numpy as np
import pandas as pd
import scipy

def calculate_metrics(data: pd.DataFrame) -> pd.Series:
    if ~pd.Series(['et_actual', 'et_forecast']).isin(data.columns).all():
        raise ValueError("DataFrame not eligible for metrics. Missing columns 'et_actual' or 'et_forecast'")
    
    mae = mean_absolute_error(data['et_actual'], data['et_forecast'])
    mse = mean_squared_error(data['et_actual'], data['et_forecast'])
    rmse = np.sqrt(mse)
    cor = np.corrcoef(data['et_actual'], data['et_forecast'])[0, 1]
    bias = np.mean(data['et_actual'] - data['et_forecast'])
    skill_score = 1 - (mse / mean_squared_error(data['et_actual'], [np.mean(data['et_forecast'])] * len(data)))
    
    return pd.Series({
		'mae': mae,
		'rmse': rmse,
		'corr': cor,
		'bias': bias,
		'skill_score': skill_score
})

def to_weekly(data: pd.DataFrame, *, index:str|list, on:str) -> any:
    resampler = data.groupby(index).resample('w', on=on)
    
    return resampler