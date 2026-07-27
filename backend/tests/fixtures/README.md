# Test fixtures

`sample_5k.parquet` -- a 5,000-row sample of `data_source/oracle_data.parquet`,
used by `tests/integration/test_determinism.py` to prove pipeline
reproducibility without paying the cost of running against the full
1.5M-row dataset on every test run.

Regenerate with:

```bash
python -c "
import duckdb
duckdb.connect().execute(\"\"\"
    COPY (SELECT * FROM read_parquet('data_source/oracle_data.parquet') USING SAMPLE 5000 ROWS)
    TO 'tests/fixtures/sample_5k.parquet' (FORMAT PARQUET)
\"\"\")
"
```
