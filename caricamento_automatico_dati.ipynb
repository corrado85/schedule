import warnings
from datetime import date, timedelta
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import yfinance as yf
from tqdm import tqdm
import pandas as pd
from datetime import datetime
import holidays
import random
from statistics import mode
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import r2_score
import concurrent.futures
import gdown
from scipy.stats import zscore
import yahoo_fin.stock_info as si

# Non mostro la numerazione scientifica
pd.set_option('display.float_format', lambda x: '%.2f' % x)

# Mostro separatori delle migliai con la virgola
pd.options.display.float_format = '{:,.3f}'.format

# Percorso del file CSV
cartella_csv = 'path/to/your/csv/folder/'

# Debugging cell (facoltativo)
import sys
print("Python version:", sys.version)
print("Pandas version:", pd.__version__)

# Solo i tickers che ho in portafoglio
prtf = ['TRMD', 'MSFT', 'STLA', 'NVDA', 'MC.PA', 'LRCX', 'META', 'AMAT', 'CFRUY', 'URI', 'AAPL', 'DHI', 'CHRD', 'LPG', 'SMCI', 'MOD']

symbol_list = prtf

all_tickers_filtered = prtf

all_tickers = prtf

print(len(prtf))

def fetch_fundamental_data(symbol):
    try:
        stock = yf.Ticker(symbol).info
        return stock
    except Exception as e:
        print(f"Ignorato errore per il simbolo {symbol}: {e}")
        return None

# Inizializza una lista per conservare i dati fondamentali
fundamental_data = []

# Utilizza tqdm per aggiungere una progress bar al ciclo
max_threads = 10
with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
    # Use executor.map to parallelize the fetching of data
    fundamental_data = list(tqdm(executor.map(fetch_fundamental_data, all_tickers), total=len(all_tickers), desc="Fetching data"))

# Crea il DataFrame finale dai dati fondamentali raccolti
df_fundamental = pd.DataFrame(fundamental_data)

# Definisci la funzione che esegue la moltiplicazione
def multiply_columns(df):
    df['mostRecentQuarter'] = pd.to_datetime(df['mostRecentQuarter'], unit='s').dt.date
    return df.drop(columns=['currentPrice', 'fiftyTwoWeekHigh'])

# Filtra e seleziona il DataFrame, applica la funzione e ordina
df_filtered = (
    df_fundamental
    [['shortName', 'industry', 'dividendYield', 'ebitdaMargins', 'recommendationKey', 'totalCashPerShare',
      'returnOnEquity', 'currentPrice', 'fiftyTwoWeekHigh', 'earningsQuarterlyGrowth', 'mostRecentQuarter']]
    .pipe(multiply_columns)
    .sort_values(by='dividendYield', ascending=False)
)

df_filtered
nuovo = df_filtered

file = "/content/drive/MyDrive/Investimenti/finanza quant/simple_kpis.csv"

esistente = pd.read_csv(file, usecols=range(1, len(pd.read_csv(file, nrows=1).columns)))

esistente['mostRecentQuarter'] = pd.to_datetime(esistente['mostRecentQuarter'])
nuovo['mostRecentQuarter'] = pd.to_datetime(nuovo['mostRecentQuarter'])

esistente['shortName'] = esistente['shortName'].astype(str)
nuovo['shortName'] = nuovo['shortName'].astype(str)

esistente['industry'] = esistente['industry'].astype(str)
nuovo['industry'] = nuovo['industry'].astype(str)

appeso = pd.concat([esistente, nuovo]).drop_duplicates(subset=["shortName", "mostRecentQuarter"]).sort_values(by=['shortName', 'mostRecentQuarter'])

appeso.to_csv(file, index=False)
