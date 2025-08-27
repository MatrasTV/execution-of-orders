# execution-of-orders

Utility to load daily Cella statistics into PostgreSQL.

Install dependencies first:

```
pip install -r requirements.txt
```

All configuration values such as file locations and database credentials are
hard coded in ``load_cella_stats_daily.py``. The script scans all three reports,
combines the union of Cellas found and stores row counts from the two XLS files
along with the "Ожидается" value from the forecast CSV (looked up by the
``cella`` column). Missing pieces for a Cella are written as SQL ``NULL``.

To run the loader simply execute:

```
python load_cella_stats_daily.py
```

To change paths or connection settings, edit the constants at the top of the
script.
