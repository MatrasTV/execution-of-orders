# execution-of-orders

Utility to load daily Cella statistics into PostgreSQL.

The script reads configuration from environment variables and requires no
command-line options. Set the variables and run the script:

```
export CELLA=Cella613
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

File paths default to the names shown above if the corresponding environment
variables are not set.
