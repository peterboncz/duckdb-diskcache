#pragma once

// Undefine Windows macros BEFORE any includes
#ifdef WIN32
#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory
#endif

#include "duckdb.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include <regex>
#include <iomanip>
#include <thread>
#include <atomic>

// inspired on AnyBlob paper: lowest latency is 20ms, transfer 12MB/s for the first MB, increasing to 40MB/s until 8MB
#define EstimateS3(bytes)                                                                                              \
	(20 + ((((bytes < (1 << 20)) ? 80 : (bytes < (1 << 23)) ? (960 / ((bytes >> 18) + 8)) : 24) * bytes) >> 20))

namespace duckdb {

// Forward declarations
struct DiskCache;

// Canceled marker for nr_bytes field and error return values
constexpr idx_t CANCELED = static_cast<idx_t>(-1);

// WriteBuffer - shared buffer for async writes
struct WriteBuffer {
	std::shared_ptr<char> buf; // Shared pointer to buffer data (with array deleter)
	size_t nr_bytes;           // Size to write, or WRITE_CANCELED if canceled
	string file_path;          // Cache file path

	WriteBuffer() : buf(nullptr), nr_bytes(0) {
	}
};

//===----------------------------------------------------------------------===//
// DiskCacheFileRange - represents a cached range with its own disk file
// Each range gets its own file, eliminating the need for DiskCacheFile
//===----------------------------------------------------------------------===//
struct DiskCacheFileRange {
	idx_t uri_range_start, uri_range_end;                            // Range in remote blob file (uri)
	shared_ptr<WriteBuffer> write_buf;                               // Shared write buffer, nullptr when write complete
	idx_t usage_count = 0, bytes_from_cache = 0, bytes_from_mem = 0; // stats
	DiskCacheFileRange *lru_prev = nullptr, *lru_next = nullptr;     // LRU doubly-linked list

	DiskCacheFileRange(idx_t start, idx_t end, shared_ptr<WriteBuffer> write_buffer)
	    : uri_range_start(start), uri_range_end(end), write_buf(std::move(write_buffer)) {
	}
};

struct DiskCacheEntry {
	string uri; // full URL of the blob
	// Map of start position to DiskCacheFileRanges (unique ownership)
	map<idx_t, unique_ptr<DiskCacheFileRange>> ranges;
};

// Statistics structure
struct DiskCacheRangeInfo {
	string uri;             // Full URI including protocol (e.g., s3://bucket/path)
	string file;            // Disk file where this range is stored in the cache
	idx_t range_start_uri;  // Start position in blob of this range
	idx_t range_size;       // Size of range (end - start in remote file)
	idx_t usage_count;      // how often it was read from the cache
	idx_t bytes_from_cache; // disk bytes read from CacheFile
	idx_t bytes_from_mem;   // memory bytes read from this cached range
};

// DiskCacheWriteJob - async write job for disk persistence
struct DiskCacheWriteJob {
	string uri;                        // For error handling and cache invalidation
	shared_ptr<WriteBuffer> write_buf; // Shared write buffer
	idx_t file_id;                     // File ID for directory creation
};

// DiskCacheReadJob - async read job for prefetching
struct DiskCacheReadJob {
	string uri;        // Cache uri of the blob that gets cached
	idx_t range_start; // Start position in file
	idx_t range_size;  // Bytes to read
};

//===----------------------------------------------------------------------===//
// DiskCache - Main cache implementation (merged DiskCacheState + DiskCacheMap + DiskCache)
//===----------------------------------------------------------------------===//

struct DiskCache {
	static constexpr idx_t MAX_IO_THREADS = 256;
	static constexpr idx_t URI_SUFFIX_LEN = 15;

	// Configuration and state
	shared_ptr<DatabaseInstance> db_instance;
	bool disk_cache_initialized = false, disk_cache_shutting_down = false;
	string path_sep;       // normally "/", but "\" on windows
	string disk_cache_dir; // where we store data temporarily
	idx_t total_cache_capacity = 0;

	// Memory cache for disk-cached files (our own ExternalFileCache instance)
	unique_ptr<ExternalFileCache> blobfile_memcache;

	// Dir mgmt: rather than checking the FileSystem for existence on each write, remember what we created already
	mutable std::mutex subdir_mutex;
	std::bitset<4096 + 4096 * 256> subdir_created; // 4096 XXX/ directories plus 4096*256 XXX/YY directories

	// Cache maps and LRU
	mutable std::mutex disk_cache_mutex;                                     // Protects cache, LRU lists, sizes
	unique_ptr<unordered_map<string, unique_ptr<DiskCacheEntry>>> key_cache; // 1 entry per file (+rangelist per entry)
	DiskCacheFileRange *lru_head = nullptr, *lru_tail = nullptr;             // LRU on the ranges (not on the files)
	idx_t current_cache_size = 0, nr_ranges = 0, current_file_id = 10000000;
	idx_t memcache_size = 0; // Track size of data in our memcache

	// Cached regex patterns for file filtering
	mutable std::mutex regex_mutex;
	vector<std::regex> cached_regexps;

	// Multi-threaded background I/O system
	std::array<std::thread, MAX_IO_THREADS> io_threads;
	std::array<std::queue<DiskCacheWriteJob>, MAX_IO_THREADS> write_queues;
	std::array<std::queue<DiskCacheReadJob>, MAX_IO_THREADS> read_queues;
	std::array<std::mutex, MAX_IO_THREADS> io_mutexes;
	std::array<std::condition_variable, MAX_IO_THREADS> io_cvs;
	std::atomic<bool> shutdown_io_threads;
	std::atomic<idx_t> read_job_counter;
	idx_t nr_io_threads;

	// Constructor/Destructor
	explicit DiskCache(DatabaseInstance *db_instance_p = nullptr)
	    : key_cache(make_uniq<unordered_map<string, unique_ptr<DiskCacheEntry>>>()), shutdown_io_threads(false),
	      read_job_counter(0), nr_io_threads(1) {
		if (db_instance_p) {
			db_instance = db_instance_p->shared_from_this();
		}
	}
	~DiskCache() {
		disk_cache_shutting_down = true;
		StopIOThreads();
	}

	// Logging methods
	void LogDebug(const string &message) const {
		if (db_instance && !disk_cache_shutting_down) {
			DUCKDB_LOG_DEBUG(*db_instance, "[DiskCache] %s", message.c_str());
		}
	}
	void LogError(const string &message) const {
		if (db_instance && !disk_cache_shutting_down) {
			DUCKDB_LOG_ERROR(*db_instance, "[DiskCache] %s", message.c_str());
		}
	}

	// File path generation
	string GenCacheFilePath(idx_t file_id, const string &uri, idx_t range_start, idx_t range_size) const {
		// Derive XXX/YY directory structure from file_id (1M combinations: 4096 * 256)
		idx_t xxx = (file_id / 256) % 4096;
		idx_t yy = file_id % 256;

		// Extract last 15 characters of URI filename (after last separator)
		idx_t last_sep = uri.find_last_of(path_sep);
		string filename_suffix =
		    (last_sep != string::npos && uri.length() > last_sep + 1)
		        ? uri.substr(std::max<idx_t>(last_sep + 1, uri.length() > 15 ? uri.length() - 15 : 0))
		        : (uri.length() > 15 ? uri.substr(uri.length() - 15) : uri);

		// Format: disk_cache_dir/XXX/YY/fileid_offset_size_last15chars
		std::ostringstream path;
		path << disk_cache_dir << std::setfill('0') << std::setw(3) << std::hex << xxx << path_sep << std::setfill('0')
		     << std::setw(2) << std::hex << yy << path_sep << std::dec << file_id << "_" << range_start << "_"
		     << range_size << "_" << filename_suffix;
		return path.str();
	}

	// Directory management
	void EnsureDirectoryExists(idx_t file_id);
	bool CleanCacheDir();
	bool InitCacheDir();

	// Memory cache helpers
	void InsertRangeIntoMemcache(const string &file_path, idx_t file_range_start, BufferHandle &handle, idx_t len);
	bool TryReadFromMemcache(const string &file_path, idx_t file_range_start, void *buffer, idx_t &len);
	bool AllocateInMemCache(BufferHandle &handle, idx_t length) {
		try {
			handle = blobfile_memcache->GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, length);
			return true;
		} catch (const std::exception &e) {
			LogError("AllocateInMemCache: failed for '" + to_string(length) + " bytes: " + string(e.what()));
			return false;
		}
	}

	// Cache map operations
	void Clear() {
		key_cache->clear();
		lru_head = lru_tail = nullptr;
		current_cache_size = nr_ranges = 0;
		if (blobfile_memcache) {
			blobfile_memcache = make_uniq<ExternalFileCache>(*db_instance, true);
		}
		memcache_size = 0;
	}

	DiskCacheEntry *FindEntry(const string &uri) {
		auto map_key = StringUtil::Lower(uri);
		auto it = key_cache->find(map_key);
		return (it != key_cache->end()) ? it->second.get() : nullptr;
	}

	DiskCacheEntry *UpsertEntry(const string &uri) {
		auto map_key = StringUtil::Lower(uri);
		auto it = key_cache->find(map_key);
		if (it == key_cache->end()) {
			auto new_entry = make_uniq<DiskCacheEntry>();
			new_entry->uri = uri;
			LogDebug("Insert URI '" + uri + "'");
			auto *cache_entry = new_entry.get();
			(*key_cache)[map_key] = std::move(new_entry);
			return cache_entry;
		}
		return it->second.get();
	}
	void EvictEntry(const string &uri);
	void EvictRange(DiskCacheFileRange *range) {
		auto buf = range->write_buf->buf;
		if (buf) {
			range->write_buf->nr_bytes = CANCELED; // Write is ongoing, cancel it
		} else {
			DeleteCacheFile(range->write_buf->file_path); // Write completed, delete the file
		}
		RemoveFromLRU(range); // Remove from LRU
		current_cache_size -= std::min<idx_t>(current_cache_size, range->uri_range_end - range->uri_range_start);
		nr_ranges--;
	}

	// LRU management
	void TouchLRU(DiskCacheFileRange *range) {
		if (range != lru_head) {
			RemoveFromLRU(range);
			AddToLRUFront(range);
		}
	}
	void RemoveFromLRU(DiskCacheFileRange *range) {
		if (range->lru_prev) {
			range->lru_prev->lru_next = range->lru_next;
		} else {
			lru_head = range->lru_next;
		}
		if (range->lru_next) {
			range->lru_next->lru_prev = range->lru_prev;
		} else {
			lru_tail = range->lru_prev;
		}
		range->lru_prev = range->lru_next = nullptr;
	}

	void AddToLRUFront(DiskCacheFileRange *range) {
		range->lru_next = lru_head;
		range->lru_prev = nullptr;
		if (lru_head) {
			lru_head->lru_prev = range;
		}
		lru_head = range;
		if (!lru_tail) {
			lru_tail = range;
		}
	}

	// File operations
	bool EvictToCapacity(idx_t required_space);
	unique_ptr<FileHandle> TryOpenCacheFile(const string &file_path);
	bool WriteToCacheFile(const string &file_path, const void *buf, idx_t len);
	idx_t ReadFromCacheFile(const string &file_path, void *buf, idx_t len, idx_t offset); // Returns bytes_from_mem
	bool DeleteCacheFile(const string &file_path);
	vector<DiskCacheRangeInfo> GetStatistics() const;

	// Thread management
	void MainIOThreadLoop(idx_t thread_id);
	void ProcessWriteJob(DiskCacheWriteJob &job);
	void ProcessReadJob(DiskCacheReadJob &job);
	void QueueIOWrite(DiskCacheWriteJob &job, idx_t partition);
	void QueueIORead(DiskCacheReadJob &job);
	void StartIOThreads(idx_t thread_count);
	void StopIOThreads();

	// Core cache operations
	void InsertCache(const string &uri, idx_t pos, idx_t len, void *buf);
	idx_t ReadFromCache(const string &uri, idx_t pos, idx_t &len, void *buf);

	// Configuration and caching policy
	void ConfigureCache(idx_t max_size_bytes, const string &directory, idx_t writer_threads);
	bool CacheUnsafely(const string &uri) const;
	void UpdateRegexPatterns(const string &regex_patterns_str);
};

} // namespace duckdb
