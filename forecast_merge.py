from pathlib import Path
import pandas as pd

def merge_forecasts():
    forecasts_table = pd.DataFrame()
    files = Path(f'data/forecasts/').glob('*.csv')
    
    for file in files:
        # splits into [$date, 'forecast.csv']
        parts = str(file.name).split('_')
        data = pd.read_csv(file, low_memory=False)
        data['forecasting_date'] = parts[0]
        forecasts_table = pd.concat([data, forecasts_table], ignore_index=True)
    
    forecasts_table.set_index('forecasting_date', inplace=True)
    forecasts_table.to_csv('forecasts_table.csv')
    
if __name__ == '__main__':
    merge_forecasts()