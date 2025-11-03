**DiskCache** is a small and *non-invasive* DuckDB extension that adds disk (SSD) caching to its ExternalFileCache (enable_external_file_cache = true by default). That ExternalFileCache (CachingFileSystem) is specifically geared towards accessing Parquet files in Data Lakes (ducklake, iceberg, delta), and caches data in RAM via the DuckDB *Buffer Manager*.

This **DiskCache** extension significantly enlarges the data volumes that can be quickly cached by also storing data on *local disk*.

It works by intercepting reads and writes that go through httpfs (including http://, https://, s3://, hf://, r2://, and gcp:// access) as well as Azure and will write what you just accessed to a local cache file as well. Subsequent reads will be served from that. By default, it will only cache files read via a *Data Lake reader*. For those Parquet reads, it is known that the underlying Parquet files do not change in the cloud, and therefore can be safely cached.

There is the possibility (off by default) to use **DiskCache** more generally, not only for Parquet files, by specifying *regular expressions* on the URIs passed to any external file reader (not only Parquet, but also JSON, CSV, etc.). Note that this is *unsafe*. Because **DiskCache** operates after the ExternalFileCache from DuckDB, it does not know whether DuckDB asks to re-read a Parquet file because it got swapped out of RAM, or because DuckDB detected a new modification time (or etag). In the latter case, **DiskCache** hits would deliver *stale data*. Note that this cannot happen in *Data Lake reads*. Therefore, when using free-form regexps for **DiskCache** it is recommended to set:

```sql
PRAGMA external_file_cache = false;
```

You can configure **DiskCache** with:

```sql
FROM disk_cache_config(directory, max_size_mb, nr_io_threads, regexps="");
```

You can inspect the configuration by invoking it without parameters. You can reconfigure an existing cache by changing all parameters except the directory. If you change the directory (where the cached file ranges are stored), then the cache gets cleared.

The `regexps` parameter contains semicolon-separated regexps that allow more aggressive caching: they will cache any URL that matches one of the regexps. Note that in this case, such caching of files purely based on their name may result in stale data being returned/

The current contents of the cache can be queried with:

```sql
FROM disk_cache_stats();
```

It lists the cache contents in reverse LRU order (hottest ranges first). One possible usage of this table function could be to store the (leading) part of these ranges in a DuckDB table.

**DiskCache** provides a `disk_cache_hydrate(URL, start, size)` scalar function that uses *massively parallel I/O* to read and cache these ranges. Please order the URL,start such that adjacent requests can be combined.

When shutting down DuckDB, you could save the list of contents of **DiskCache** as follows:

```sql
CREATE OR REPLACE TABLE hydrate AS
SELECT uri range_start_uri, range_size
FROM disk_cache_stats()
ORDER BY ALL;
```

When you restart DuckDB later, potentially on another machine, you can quickly hydrate **DiskCache**:

```sql
SELECT disk_cache_hydrate(uri, range_start_uri, range_size) FROM hydrate;
```

The `disk_cache_hydrate()` function generates I/O threads (see: `disk_cache_config`) for parallel I/O requests. Doing so is necessary in *cloud instances* to get near the network bandwidth, and allows for quick hydration of the smart cache from a previous state.

**DiskCache** supports a "fake-S3" `fake_s3://X` filesystem  which acts like S3 but directs to the local filesystem, while adding *fake network latencies* similar to S3 latencies (best-case "inside the same AZ"). This is a handy tool for local performance debugging without having to spin up an EC2 instance.


