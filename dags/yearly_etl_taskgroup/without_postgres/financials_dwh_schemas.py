from google.cloud.bigquery import SchemaField

tables = ['F_STOCKS', 'F_NEWS', 
        'D_COMMODITIES', 'D_EX_RATE', 'D_FINANCIALS', 'D_INFLATION', 'D_INT_RATE']

F_STOCKS = [SchemaField('Date', 'TIMESTAMP', 'NULLABLE', None, ()),
SchemaField('Open', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('High', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('Low', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('Close', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('Volume', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('Dividends', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('Stock_Splits', 'INTEGER', 'NULLABLE', None, ()),
SchemaField('Stock', 'STRING', 'NULLABLE', None, ()),
SchemaField('SMA_50', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('SMA_200', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('GC', 'BOOLEAN', 'NULLABLE', None, ()),
SchemaField('DC', 'BOOLEAN', 'NULLABLE', None, ()),
SchemaField('Price_Category', 'STRING', 'NULLABLE', None, ()),
SchemaField('usd_sgd', 'STRING', 'NULLABLE', None, ()),
SchemaField('sora', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('networth', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('EXR_ID', 'STRING', 'NULLABLE', None, ()),
SchemaField('INR_ID', 'STRING', 'NULLABLE', None, ()),
SchemaField('FIN_ID', 'STRING', 'NULLABLE', None, ()),
SchemaField('INFL_ID', 'STRING', 'NULLABLE', None, ())
]

F_NEWS = [SchemaField('Ticker', 'STRING', 'NULLABLE', None, ()),
SchemaField('Title', 'STRING', 'NULLABLE', None, ()),
SchemaField('Date', 'DATE', 'NULLABLE', None, ()),
SchemaField('Link', 'STRING', 'NULLABLE', None, ()),
SchemaField('Source', 'STRING', 'NULLABLE', None, ()),
SchemaField('Comments', 'STRING', 'NULLABLE', None, ())
]

D_COMMODITIES = [SchemaField('Date', 'TIMESTAMP', 'NULLABLE', None, ()),
SchemaField('Open', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('High', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('Low', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('Close', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('Adj_Close', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('Volume', 'INTEGER', 'NULLABLE', None, ()),
SchemaField('Price_Category', 'STRING', 'NULLABLE', None, ())
]

D_EX_RATE = [SchemaField('Date', 'TIMESTAMP', 'NULLABLE', None, ()),
SchemaField('EXR_ID', 'STRING', 'NULLABLE', None, ()),
SchemaField('eur_sgd', 'STRING', 'NULLABLE', None, ()),
SchemaField('gbp_sgd', 'STRING', 'NULLABLE', None, ()),
SchemaField('usd_sgd', 'STRING', 'NULLABLE', None, ()),
SchemaField('aud_sgd', 'STRING', 'NULLABLE', None, ()),
SchemaField('cad_sgd', 'STRING', 'NULLABLE', None, ()),
SchemaField('cny_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('hkd_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('inr_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('idr_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('jpy_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('krw_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('myr_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('twd_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('nzd_sgd', 'STRING', 'NULLABLE', None, ()),
SchemaField('php_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('qar_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('sar_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('chf_sgd', 'STRING', 'NULLABLE', None, ()),
SchemaField('thb_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('aed_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('vnd_sgd_100', 'STRING', 'NULLABLE', None, ()),
SchemaField('ex_rate_preliminary', 'STRING', 'NULLABLE', None, ()),
SchemaField('ex_rate_timestamp', 'STRING', 'NULLABLE', None, ()),
]

D_FINANCIALS = [SchemaField('id', 'STRING', 'NULLABLE', None, ()),
SchemaField('ticker', 'STRING', 'NULLABLE', None, ()),
SchemaField('year', 'TIMESTAMP', 'NULLABLE', None, ()),
SchemaField('type', 'STRING', 'NULLABLE', None, ()),
SchemaField('value', 'FLOAT', 'NULLABLE', None, ())
]

D_INFLATION = [SchemaField('id', 'STRING', 'NULLABLE', None, ()),
SchemaField('year', 'TIMESTAMP', 'NULLABLE', None, ()),
SchemaField('inflation', 'FLOAT', 'NULLABLE', None, ())
]

D_INT_RATE = [SchemaField('Date', 'TIMESTAMP', 'NULLABLE', None, ()),
SchemaField('Actual_Date', 'TIMESTAMP', 'NULLABLE', None, ()),
SchemaField('INR_ID', 'STRING', 'NULLABLE', None, ()),
SchemaField('aggregate_volume', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('calculation_method', 'STRING', 'NULLABLE', None, ()),
SchemaField('comp_sora_1m', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('comp_sora_3m', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('comp_sora_6m', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('highest_transaction', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('lowest_transaction', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('on_rmb_facility_rate', 'STRING', 'NULLABLE', None, ()),
SchemaField('published_date', 'STRING', 'NULLABLE', None, ()),
SchemaField('sor_average', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('sora', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('sora_index', 'FLOAT', 'NULLABLE', None, ()),
SchemaField('standing_facility_borrow', 'STRING', 'NULLABLE', None, ()),
SchemaField('standing_facility_deposit', 'STRING', 'NULLABLE', None, ()),
SchemaField('int_rate_preliminary', 'INTEGER', 'NULLABLE', None, ()),
SchemaField('int_rate_timestamp', 'STRING', 'NULLABLE', None, ())
]