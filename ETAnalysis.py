from sklearn.metrics import mean_absolute_error, mean_squared_error

import numpy as np
import pandas as pd
import scipy

def calculate_metrics(data: any, *, historical_ref: pd.DataFrame, actual: str, expected: str) -> pd.Series:
	if ~pd.Series(['time']).isin(data.reset_index().columns).all():
		raise ValueError("'time' column not found. calculate_metrics uses this column for climatology.\n" + str(data.info()))
	
	mae = mean_absolute_error(data[actual], data[expected])
	mse = mean_squared_error(data[actual], data[expected])
	
	rmse = np.sqrt(mse)
	cor = np.corrcoef(data[actual], data[expected])[0, 1]
	bias = np.mean(data[actual] - data[expected])
	
	# Climatology uses the mean of actual_et for that time of year using historical data.
	mod_frame = historical_ref.reset_index().set_index('time')
	weekly_mean_et = mod_frame.groupby(['field_id', 'crop', mod_frame.index.month, mod_frame.index.day])['actual_et'].mean()
	
	# Pandas behavior does its calculations row-by-row. Matching weekly_mean_et requires some work.
	# Create a mask that matches month and day on data['time'] to corresponding ones on weekly_mean_et
	month_day_mask = ((weekly_mean_et.index.get_level_values(2) == data['time'].dt.month) & 
						(weekly_mean_et.index.get_level_values(3) == data['time'].dt.day))
	# Create a simpler mask that matches field_id and crop.
	# These columns are indexes in weekly_mean_et, which are ['field_id', 'crop', 'time', 'time']
	field_crop_mask = (data.index.get_level_values('field_id') == weekly_mean_et.index.get_level_values('field_id') &
					   data.index.get_level_values('crop') == weekly_mean_et.index.get_level_values('crop'))
	climatology = historical_ref[(month_day_mask) & (field_crop_mask)]

	skill_score = 1 - (mse / mean_squared_error(data[actual], climatology * len(data)))
	
	return pd.Series({
		'mae': mae.round(2),
		'rmse': rmse.round(2),
		'corr': cor.round(2),
		'bias': bias.round(2),
		'skill_score': skill_score.round(2)
	})

def to_weekly(data: pd.DataFrame, *, index:str|list, on:str) -> any:
	resampler = data.groupby(index).resample('w', on=on)
	
	return resampler