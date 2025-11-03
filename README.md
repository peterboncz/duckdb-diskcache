**DiskCache** is a small and *non-invasive* DuckDB extension that adds disk (SSD) caching to its ExternalFileCache. That ExternalFileCache (CachingFileSystem) is specifically geared towards accessing Parquet files in Data Lakes (ducklake, iceberg, delta) where it knows that their Parquet files will never be changed and thus can safely be cached. DuckDB's ExternalFileCache caches data in RAM via the *Buffer Manager* that is also used for database data. This **DiskCache** extension significantly enlarges the data volumes that can be quickly cached by also storing data on *local disk*.

It works by intercepting reads and writes that go through httpfs (including http://, https://, s3://, hf://, r2://, and gcp:// access) as well as Azure and will write what you just accessed to a local cache file as well. Subsequent reads will be served from that. This also holds for data that you just wrote: it will also be cached in **DiskCache**.  By default, it will only cache files read via a *Data Lake reader*. 

**DiskCache** will use by default a maximum size of 4GB * cores and as number of IO threads 12 * cores (with max 192).
It works fine by default in conjunction with the ExternalFileCache which is on by default in DuckDB. **DiskCache** intercepts network reads and writes, but these will only occur if the ExternalFileCache misses. Therefore, very hot ranges that are firmly cached in RAM by ExternalDiskCache will never need a network read and therefore appear to be cold data to the **DiskCache**. We therefore also implemented RAM caching inside the **DiskCache** which gets used only if the ExternalFileCache is disabled by:
```sql
SET enable_external_file_cache = false;
```
The benefit then is that **DiskCache** sees all accesses and hot data  that resides in RAM, managed by itself, will not get swapped out of ths disk cache.

There is the possibility (off by default) to use **DiskCache** more agressively, and not only for Parquet files, by specifying *regular expressions* on the URIs passed to any external file reader (not only Parquet, but also JSON, CSV, etc.). Note that this is *unsafe*. Because **DiskCache** operates by default after the ExternalFileCache from DuckDB, it does not know whether DuckDB asks to re-read a Parquet file because it got swapped out of RAM, or because DuckDB detected a new modification time (or etag). In the latter case, **DiskCache** hits would deliver *stale data*. Note that this cannot happen in *Data Lake reads*. 

You can configure **DiskCache** with:

```sql
FROM disk_cache_config(directory, max_size_mb, nr_io_threads, regexps="");
```

You can inspect the configuration by invoking it without parameters. You can reconfigure an existing cache by changing all parameters except the directory. If you change the directory (where the cached file ranges are stored), then the cache gets cleared. The `regexps` parameter contains semicolon-separated regexps that allow more aggressive caching: they will cache any URL that matches one of the regexps. Note that in this case, such caching of files purely based on their name may result in stale data being returned/

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

The `disk_cache_hydrate()` function uses many I/O threads (see: `disk_cache_config`) for doing parallel I/O requests. Doing so is necessary in *cloud instances* to get near the network bandwidth, and allows for quick hydration of the smart cache from a previous state.

**DiskCache** supports a "fake-S3" `fake_s3://X` filesystem  which acts like S3 but directs to the local filesystem, while adding *fake network latencies* similar to S3 latencies (best-case "inside the same AZ"). This is a handy tool for local performance debugging without having to spin up an EC2 instance. One could e.g. create a SF100 tpch database and generate parquet files e.g., using:
```sql
load tpch;
call dbgen(sf=100);
copy (from lineitem) to 'sf100' (FORMAT parquet, file_size_bytes '20MB', filename_pattern 'limeitem_{i}', overwrite_or_ignore 1);
```
so lineitem becomes 100s of parquet files in  local subdirectory sf100. Then:
```sql
create view lineitem as (from read_parquet('fake_s3://sf100/lineitem*parquet'))  
```

(and similar for the other tables, order, partsupp, part, supplier, customer, nation, region). You now have a full SF100 TPCH database in many parquet files in Fake-S3 and can run e.g. `pragma tpch(1);` to test query 1 (and similar for 2-22).

