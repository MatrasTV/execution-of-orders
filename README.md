# execution-of-orders

Utility to load daily Cella statistics into PostgreSQL.

Install dependencies first:

```
pip install -r requirements.txt
```

The script reads configuration from environment variables and requires no
command-line options. Set the variables and run the script:

```
export DATA_DIR="//192.168.3.7/ul/Закупки/DATA"
export PARTIAL_XLS="Частично.xls"
export FULL_XLS="Целиком.xls"
export FORECAST_CSV="Почасовой прогноз прихода заказов на склад.csv"
export PGHOST=192.168.3.19
export PGPORT=5432
export PGDATABASE=postgres
export PGUSER=Admin
export PGPASSWORD=0782
export SCHEMA=REPORT
export TABLE="execution-of-orders"

python load_cella_stats_daily.py
```

By default, statistics are loaded for all Cellas found in the reports. To limit
processing to a single Cella, set the ``CELLA`` environment variable before
running the script.

If ``DATA_DIR`` is omitted, the script looks for files in the current
directory. Individual file variables (``PARTIAL_XLS`` etc.) may also be
overridden with absolute paths.
