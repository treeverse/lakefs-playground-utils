# lakeFS Playground Utilities

## Using in a notebook:

```python
>>> !pip install lakefs-playground-utils # Syntax used in Google Colab, might be different for Jupyter, Databricks, etc.
>>> 
>>> import playground
>>> conn = playground.get_or_create('myemail@example.com') # Creates a lakeFS playground environment, or returns an existing one
>>> playground.mount(conn) # Sets up a `lakefs://` protocol handler for pandas, pre-configured to read+write from our playground environment
```

Once set up, we can use pandas to read and write from a lakeFS repository on our playground installation:

```python
>>> import pandas as pd
>>> pd.read_parquet('lakefs://repo/branch/path/to/part-00000.snappy.parquet')
```

## License

Apache 2.0

See [LICENSE](./LICENSE)
