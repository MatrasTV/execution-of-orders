# execution-of-orders

Utility to load daily Cella statistics into PostgreSQL.

```
python load_cella_stats_daily.py \
    --cella Cella613 \
    --partial "Частично.xls" \
    --full "Целиком.xls" \
    --forecast "Почасовой прогноз прихода заказов на склад.csv" \
    --host 192.168.3.19 --port 5432 --dbname postgres \
    --user Admin --password 0782 \
    --schema REPORT --table "execution-of-orders"
```
