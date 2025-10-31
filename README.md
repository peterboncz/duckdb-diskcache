DiskCache is a small and non-invasive DuckDB extension that adds disk (SSD) caching to its ExternalFileCache (enable_external_file_cache = true by default).  That ExternalFileCache (CachingFileSystem) is specifically geared towards accessing Parquet files in Data Lakes (ducklake, iceberg, delta), and caches data in RAM via the DuckDB Buffer Manager. 

This DiskCache extension significantly enlarges the data volumes that can be quickly cached by also storing data on local disk.

It works by intercepting reads and writes that go through httpfs (incl hhtp://, https://, s3://, hf:// r2:// and gcp:// access) as well as azure and will write what you just accessed to a local cache file as well. Subsequent reads will be served from that.  By default, it will only cache files read via a Data Lake reader. For those parquet reads, it is known that the underlying parquet files do not change in the cloud, and therefore can be safely cached.

There is the possibility (off by default) to DiskCache much generally, not only parquet files, by specifying regeular expressions on the URIs passed to any external file reader (not only parquet, but also json, csv, ..). Note that this is unsafe. Because DiskCache operates after the ExternalFileCache from DuckDB, it does not know whether DuckDB asks to re-read a parquet file because it got swapped out of RAM, or because DuckDB detected a new modification time (or etag). In the latter case, DiskCache hits would deliver stale data. Note that this cannot happen in Data Lake reads. Therefore, when using free-form regexps for Disk it is recommended to set external_file_cache = false;

DiskCache supports a "fake-S3" fake_s3://X filesystem which acts like S3 but directs to local filesystem, while adding fake network latencies similar to S3 latencies (note: these would be best-case "inside the same AZ" S3 latency). This is a handy tool for local performance debugging without having to spin up an EC2 instance.

You can configure DiskCache with: FROM disk_cache_config(directory, max_size_mb, nr_io_threads, regexps="");

You can inspect the configuration by invoking that without parameters.
You can reconfigure an existing cache by changing all parameters except the directory. If you change the directory (where the cached file ranges are stored), then the cache gets cleared.

The regexps parameter contains semicolon-separated regexps that allow more aggressive caching: they will cache any URL that matches one of the regexps.

The current contents of the cache can be queried with FROM disk_cache_stats(); it lists the cache contents in reverse LRU order (hottest ranges first). One possible usage of this TableFunction could be to store the (leading) part of these ranges in a DuckDB table. 

Because, smartcache provides a disk_cache_hydrate(URL, start, size) scalar function that uses massively parallel IO to read and cache these ranges. Please order the the URL,start such that it can combine adjacent requests:

You could when shutting down your duckdb, save the list of contents of the DiskCache as follows:
 
create or replace table hydrate as 
(select concat(protocol, '://', uri) as uri, range_start_uri, range_size from disk_cache_stats() order by all);

When you restart your duckdb later, potentially on another machine, you can quickly hydrate the DiskCache:

select disk_cache_hydrate(uri, range_start_uri, range_size) from hydrate; 
 
The disk_cache_hydrate() generates io_treads (see: disk_cache_config) parallel IO requests. Doing so is necessary in cloud instances to get near the network bandwidth, and allows for quick hydration of the smartcache from a previous state.
