#include "include/diskcache.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

DiskcacheFileRange *AnalyzeRange(Diskcache &cache, const string &uri, idx_t pos, idx_t &len) {
	DiskcacheEntry *diskcache_entry = cache.FindEntry(uri);
	if (!diskcache_entry || diskcache_entry->ranges.empty()) {
		return nullptr;
	}
	auto it = diskcache_entry->ranges.upper_bound(pos);
	DiskcacheFileRange *hit_range = nullptr;
	if (it != diskcache_entry->ranges.begin()) {
		auto prev_it = std::prev(it);
		auto &prev_range = prev_it->second;
		if (prev_range && prev_range->range_end > pos) {
			hit_range = prev_range.get();
		}
	}
	// Check the next range to see if we need to reduce 'len' (to avoid reading data that we already cached)
	if (it != diskcache_entry->ranges.end() && it->second) {
		if (it->second->range_start < pos + len) {
			len = it->second->range_start - pos;
		}
	}
	return hit_range;
}

idx_t Diskcache::ReadFromCache(const string &uri, idx_t pos, idx_t &len, void *buf) {
	DiskcacheFileRange *hit_range = nullptr;
	idx_t orig_len = len, off = pos, hit_size = 0;

	std::unique_lock<std::mutex> lock(diskcache_mutex);
	hit_range = AnalyzeRange(*this, uri, off, len); // may adjust len downward to match a next cached range
	if (hit_range) {
		hit_size = std::min(orig_len, hit_range->range_end - pos);
	}
	if (hit_size == 0) {
		lock.unlock();
		return 0;
	}

	// the read we want to do at 'pos' finds a hit of its 'hit_size' first bytes in 'hit_range'
	// it may have reduced the following non-cached read until 'len' to a next cached range
	idx_t offset = pos - hit_range->range_start; // offset in hit_range that we will read
	hit_range->usage_count++;
	hit_range->bytes_from_cache += hit_size;
	TouchLRU(hit_range); // Update LRU position

	// Check if we can read from WriteBuffer (write in progress or completed
	std::shared_ptr<char> buffer_data = hit_range->write_buf->buf;
	if (buffer_data) { // we obtained the shared_ptr reference, can use it now safely
		memcpy(buf, buffer_data.get() + offset, hit_size);
		lock.unlock();
		return hit_size;
	}
	// save the critical values before unlock (after unlock, hit_range* might get deallocated in a concurrent evict)
	string file = hit_range->write_buf->file_path;
	idx_t range_start = hit_range->range_start;
	lock.unlock();

	// Read from on-disk cache file unlocked
	// Note: hit_size may be reduced by ReadFromCacheFile if memcache has partial data
	idx_t bytes_from_mem = ReadFromCacheFile(file, buf, hit_size, offset);
	if (bytes_from_mem == CANCELED) {
		return 0; // Read failed -- but the wrapped FileSystem will read it again
	}
	if (bytes_from_mem > 0) { // Update bytes_from_mem counter if we had a memory hit
		lock.lock();
		auto diskcache_entry = FindEntry(uri);
		if (diskcache_entry) {
			auto range_it = diskcache_entry->ranges.find(range_start);
			if (range_it != diskcache_entry->ranges.end()) {
				range_it->second->bytes_from_mem += bytes_from_mem;
			}
		}
		lock.unlock();
	}
	return hit_size; // hit_size may have been reduced by ReadFromCacheFile
}

// we had to read from the original source (e.g. S3). Now try to cache this buffer in the disk-based diskcache
void Diskcache::InsertCache(const string &uri, idx_t pos, idx_t len, void *buf) {
	if (!diskcache_initialized || len == 0 || len > total_cache_capacity) {
		return; // bail out if non initialized or impossible length
	}
	std::lock_guard<std::mutex> lock(regex_mutex);
	auto cache_entry = UpsertEntry(uri);

	// Check (under lock) if range already cached (in the meantime, due to concurrent reads)
	auto hit_range = AnalyzeRange(*this, uri, pos, len);
	idx_t offset = 0, final_size = 0, range_start = pos, range_end = range_start + len;
	if (hit_range) { // another thread cached the same range in the meantime
		offset = hit_range->range_end - range_start;
		range_start = hit_range->range_end; // cache only from the end
	}
	if (range_end > range_start) {
		final_size = range_end - range_start;
	}
	if (final_size == 0) {
		return; // nothing to cache
	}
	if (!EvictToCapacity(final_size)) { // make sure we have room for 'final_size' extra bytes
		LogError("InsertCache: EvictToCapacity failed for " + to_string(final_size) +
		         " bytes (current_cache_size=" + to_string(current_cache_size) + ")");
		return; // failed to make room
	}
	// Create a shared WriteBuffer with the data
	auto write_buffer = make_shared_ptr<WriteBuffer>();
	write_buffer->buf = std::shared_ptr<char>(new char[final_size], [](char *p) { delete[] p; });
	write_buffer->nr_bytes = final_size;
	std::memcpy(write_buffer->buf.get(), static_cast<const char *>(buf) + offset, final_size);

	// Generate file path and store it in the write buffer
	idx_t file_id = ++current_file_id;
	write_buffer->file_path = GenCacheFilePath(file_id, uri, range_start, final_size);

	// Create a new DiskcacheFileRange with unique ownership
	auto new_range = make_uniq<DiskcacheFileRange>(range_start, range_end, write_buffer);
	auto *range_ptr = new_range.get();
	cache_entry->ranges[range_start] = std::move(new_range);

	// Add to LRU and update stats
	AddToLRUFront(range_ptr);
	current_cache_size += final_size;
	nr_ranges++;

	// Schedule the disk write
	DiskcacheWriteJob job;
	job.write_buf = write_buffer;
	job.uri = uri;
	job.file_id = file_id;
	QueueIOWrite(job, file_id % nr_io_threads); // Partition based on file_id
}

//===----------------------------------------------------------------------===//
// Memory cache helpers
//===----------------------------------------------------------------------===//

void Diskcache::InsertRangeIntoMemcache(const string &file, idx_t range_start, BufferHandle &handle, idx_t len) {
	auto &memcache_file = blobfile_memcache->GetOrCreateCachedFile(file);
	auto memcache_range =
	    make_shared_ptr<ExternalFileCache::CachedFileRange>(handle.GetBlockHandle(), len, range_start, "");
	auto lock_guard = memcache_file.lock.GetExclusiveLock();
	memcache_file.Ranges(lock_guard)[range_start] = memcache_range;
	memcache_size += len; // Track memcache usage
	LogDebug("InsertRangeIntoMemcache: inserted into memcache: '" + file + "' at offset " +
	         std::to_string(range_start) + " length " + std::to_string(len));
}

bool Diskcache::TryReadFromMemcache(const string &file, idx_t range_start, void *buf, idx_t &len) {
	if (!blobfile_memcache) {
		return false;
	}
	// Check if the range is already cached in memory
	auto &memcache_file = blobfile_memcache->GetOrCreateCachedFile(file);
	auto memcache_ranges_guard = memcache_file.lock.GetSharedLock();
	auto &memcache_ranges = memcache_file.Ranges(memcache_ranges_guard);
	auto it = memcache_ranges.find(range_start);
	if (it == memcache_ranges.end()) {
		return false; // Range not found in memory cache
	}
	auto &memcache_range = *it->second;
	if (memcache_range.nr_bytes == 0) {
		return false; // Empty range
	}
	LogDebug("TryReadFromMemcache: memcache hit for " + to_string(len) + " bytes in '" + file + "',  offset " +
	         to_string(range_start) + " length " + to_string(memcache_range.nr_bytes));
	auto &buffer_manager = blobfile_memcache->GetBufferManager();
	auto pin = buffer_manager.Pin(memcache_range.block_handle);
	if (!pin.IsValid()) {
		LogDebug("TryReadFromMemcache: pinning cache hit failed -- apparently there is high memory pressure");
		return false;
	}
	if (memcache_range.nr_bytes < len) {
		len = memcache_range.nr_bytes;
	}
	std::memcpy(buf, pin.Ptr(), len); // Memory hit - read from BufferHandle
	return true;
}

//===----------------------------------------------------------------------===//
// Multi-threaded background cache writer implementation
//===----------------------------------------------------------------------===//

void Diskcache::QueueIOWrite(DiskcacheWriteJob &job, idx_t partition) {
	{
		std::lock_guard<std::mutex> lock(io_mutexes[partition]);
		write_queues[partition].emplace(std::move(job));
	}
	io_cvs[partition].notify_one();
}

void Diskcache::QueueIORead(DiskcacheReadJob &job) {
	// Hash-based partitioning by URI to ensure same file goes to same thread
	hash_t uri_hash = Hash(string_t(job.uri.c_str(), static_cast<uint32_t>(job.uri.length())));
	idx_t target_thread = uri_hash % nr_io_threads;
	{
		std::lock_guard<std::mutex> lock(io_mutexes[target_thread]);
		read_queues[target_thread].emplace(std::move(job));
	}
	io_cvs[target_thread].notify_one();
}

void Diskcache::StartIOThreads(idx_t thread_count) {
	if (thread_count > MAX_IO_THREADS) {
		thread_count = MAX_IO_THREADS;
		LogDebug("StartIOThreads: limiting IO threads to maximum allowed: " + std::to_string(MAX_IO_THREADS));
	}
	shutdown_io_threads = false;
	nr_io_threads = thread_count;

	LogDebug("StartIOThreads: starting " + std::to_string(nr_io_threads) + " diskcache IO threads");

	for (idx_t i = 0; i < nr_io_threads; i++) {
		io_threads[i] = std::thread([this, i] { MainIOThreadLoop(i); });
	}
}

void Diskcache::StopIOThreads() {
	if (nr_io_threads == 0) {
		return; // Skip if no threads are running
	}
	shutdown_io_threads = true; // Signal shutdown to all threads

	// Notify all threads to wake up and check shutdown flag
	for (idx_t i = 0; i < nr_io_threads; i++) {
		io_cvs[i].notify_all();
	}
	// Wait for all threads to finish gracefully
	for (idx_t i = 0; i < nr_io_threads; i++) {
		if (io_threads[i].joinable()) {
			try {
				io_threads[i].join();
			} catch (const std::exception &) {
				// Ignore join errors during shutdown - thread may have already terminated
			}
		}
	}
	// Only log if not shutting down
	if (!diskcache_shutting_down) {
		LogDebug("StopIOThreads: stopped " + std::to_string(nr_io_threads) + " cache writer threads");
	}
	nr_io_threads = 0; // Reset thread count
}

void Diskcache::ProcessWriteJob(DiskcacheWriteJob &job) {
	EnsureDirectoryExists(job.file_id);        // first ensure there is directory to write the file into
	if (job.write_buf->nr_bytes == CANCELED) { // Check if write was canceled before we started
		LogDebug("ProcessWriteJob: write was canceled before starting, skipping");
		return;
	}
	// Perform the write
	bool write_success = WriteToCacheFile(job.write_buf->file_path, job.write_buf->buf.get(), job.write_buf->nr_bytes);
	bool was_canceled = (job.write_buf->nr_bytes == CANCELED);
	job.write_buf->buf = nullptr; // release buffer in any case
	if (write_success && !was_canceled) {
		LogDebug("ProcessWriteJob: write completed successfully for '" + job.write_buf->file_path + "'");
		return; // the happy path, we expect to be here. And we keep the file.
	}
	DeleteCacheFile(job.write_buf->file_path); // Write failed or was canceled, clean up
	if (was_canceled) {
		LogDebug("ProcessWriteJob: write was canceled, deleting partial file");
	} else {
		LogError("ProcessWriteJob: write failed for '" + job.write_buf->file_path + "', evicting entire cache entry");
		EvictEntry(job.uri); // evict the entire cache entry (ok, a bit nuclear, but should not happen often)
	}
}

void Diskcache::ProcessReadJob(DiskcacheReadJob &job) {
	try {
		auto db = db_instance.lock();
		if (!db) {
			return; // Database is shutting down
		}
		// Open file and allocate buffer
		auto &fs = FileSystem::GetFileSystem(*db);
		auto handle = fs.OpenFile(job.uri, FileOpenFlags::FILE_FLAGS_READ);
		auto buffer = unique_ptr<char[]>(new char[job.range_size]);

		// Read data from file
		fs.Read(*handle, buffer.get(), job.range_size, job.range_start);

		// Insert into cache (this will queue a write job)
		InsertCache(job.uri, job.range_start, job.range_size, buffer.get());
	} catch (const std::exception &e) {
		LogError("ProcessReadJob: failed to read '" + job.uri + "' at " + to_string(job.range_start) + ": " +
		         string(e.what()));
	}
}

void Diskcache::MainIOThreadLoop(idx_t thread_id) {
	LogDebug("MainIOThreadLoop " + std::to_string(thread_id) + " started");
	while (!shutdown_io_threads) {
		std::unique_lock<std::mutex> lock(io_mutexes[thread_id]);
		io_cvs[thread_id].wait(lock, [this, thread_id] {
			return !write_queues[thread_id].empty() || !read_queues[thread_id].empty() || shutdown_io_threads;
		});
		if (shutdown_io_threads && write_queues[thread_id].empty() && read_queues[thread_id].empty()) {
			break;
		}
		// Process writes with priority
		if (!write_queues[thread_id].empty()) {
			auto write_job = std::move(write_queues[thread_id].front());
			write_queues[thread_id].pop();
			lock.unlock();
			ProcessWriteJob(write_job);
		} else if (!read_queues[thread_id].empty()) {
			auto read_job = std::move(read_queues[thread_id].front());
			read_queues[thread_id].pop();
			lock.unlock();
			ProcessReadJob(read_job);
		}
	}
	// Only log thread shutdown if not during database shutdown to avoid access to destroyed instance
	if (!diskcache_shutting_down) {
		LogDebug("MainIOThreadLoop " + std::to_string(thread_id) + " stopped");
	}
}

//===----------------------------------------------------------------------===//
// Diskcache - evict a complete file (i.e. it entry and all its ranges)
//===----------------------------------------------------------------------===//
void Diskcache::EvictEntry(const string &uri) {
	if (!diskcache_initialized) {
		return;
	}
	std::lock_guard<std::mutex> lock(diskcache_mutex);
	auto map_key = StringUtil::Lower(uri);
	auto it = key_cache->find(map_key);
	if (it == key_cache->end()) {
		return; // Not found
	}
	// Iterate through all ranges and evict them
	auto &cache_entry = it->second;
	for (auto &range_pair : cache_entry->ranges) {
		EvictRange(range_pair.second.get()); // Evict the range (cancel write or delete file)
	}
	key_cache->erase(it); // Remove the entire cache entry
}

// Evict ranges (not entries) until the target is met
bool Diskcache::EvictToCapacity(idx_t new_range_size) {
	// Note: This is called with diskcache_mutex already held
	if (current_cache_size + new_range_size <= total_cache_capacity) {
		return true; // No eviction needed
	}
	// Try to evict ranges to make space, returns true if successful
	idx_t required_space = current_cache_size + new_range_size - total_cache_capacity;
	idx_t freed_space = 0;
	idx_t ranges_checked = 0;
	idx_t max_ranges = nr_ranges + 1; // Safety limit to prevent infinite loops

	auto *current_range = lru_tail; // Start from least recently used
	while (required_space > freed_space && current_range && ranges_checked++ < max_ranges) {
		// Save next candidate before we evict current
		auto *next_range = current_range->lru_prev;
		idx_t range_size = current_range->range_end - current_range->range_start;
		idx_t range_start = current_range->range_start;

		// Find and remove this range from its cache entry
		bool removed = false;
		for (auto &cache_pair : *key_cache) {
			auto &cache_entry = cache_pair.second;
			auto range_it = cache_entry->ranges.find(range_start);
			if (range_it != cache_entry->ranges.end() && range_it->second.get() == current_range) {
				EvictRange(current_range);           // Evict the range (cancel write or delete file)
				cache_entry->ranges.erase(range_it); // Remove from ranges map
				removed = true;
				break;
			}
		}
		if (!removed) {
			LogError("EvictToCapacity: could not find range in cache entries");
		}
		freed_space += range_size;
		current_range = next_range; // Move to next range in LRU
	}
	if (ranges_checked >= max_ranges) {
		LogError("EvictToCapacity: hit safety limit after checking " + std::to_string(ranges_checked) + " ranges");
	}
	if (freed_space < required_space) {
		LogError("EvictToCapacity: needed " + std::to_string(required_space) + " bytes but only freed " +
		         std::to_string(freed_space) + " bytes (current_cache_size=" + std::to_string(current_cache_size) +
		         ", nr_ranges=" + std::to_string(nr_ranges) + ", ranges_checked=" + std::to_string(ranges_checked));
		return false;
	}
	return true;
}

vector<DiskcacheRangeInfo> Diskcache::GetStatistics() const { // produce list of cached ranges for diskcache_stats() TF
	std::lock_guard<std::mutex> lock(diskcache_mutex);
	vector<DiskcacheRangeInfo> result;
	result.reserve(nr_ranges);

	// Iterate through all CacheEntries - no stale checking needed since ranges are deleted immediately
	for (const auto &cache_pair : *key_cache) {
		const auto &cache_entry = cache_pair.second;
		DiskcacheRangeInfo info;
		info.uri = cache_entry->uri; // Keep full URI with protocol intact
		for (const auto &range_pair : cache_entry->ranges) {
			info.file = range_pair.second->write_buf->file_path;
			info.range_start = range_pair.second->range_start;
			info.range_size = range_pair.second->range_end - range_pair.second->range_start;
			info.usage_count = range_pair.second->usage_count;
			info.bytes_from_cache = range_pair.second->bytes_from_cache;
			info.bytes_from_mem = range_pair.second->bytes_from_mem;
			result.push_back(info);
		}
	}
	return result;
}

//===----------------------------------------------------------------------===//
// Diskcache file management
//===----------------------------------------------------------------------===//

unique_ptr<FileHandle> Diskcache::TryOpenCacheFile(const string &file) {
	auto db = db_instance.lock();
	if (!db) {
		return nullptr;
	}
	try {
		auto &fs = FileSystem::GetFileSystem(*db);
		return fs.OpenFile(file, FileOpenFlags::FILE_FLAGS_READ);
	} catch (const std::exception &e) {
		// File was evicted between metadata check and open - this can legally happen
		LogDebug("TryOpenCacheFile: file not found (likely evicted): '" + file + "'");
		return nullptr;
	}
}

idx_t Diskcache::ReadFromCacheFile(const string &file, void *buffer, idx_t &length, idx_t offset) {
	auto db = db_instance.lock();
	if (!db) {
		return CANCELED;
	}
	// Check if we should use our memcache (only if DuckDB's external cache is disabled)
	auto &config = DBConfig::GetConfig(*db);
	bool use_memcache = !config.options.enable_external_file_cache;

	// If external cache is enabled but we have data in memcache, clear it
	if (!use_memcache && memcache_size > 0) {
		blobfile_memcache = make_uniq<ExternalFileCache>(*db, true);
		memcache_size = 0;
		LogDebug("ReadFromCacheFile: cleared memcache (DuckDB's external cache is now enabled)");
	}
	// Try memcache first
	if (use_memcache && TryReadFromMemcache(file, offset, buffer, length)) {
		return length; // reading from memcache succeeded - all bytes from mem
	}
	// Not in memory cache - read from disk
	auto handle = TryOpenCacheFile(file);
	if (!handle) {
		return CANCELED; // File was evicted or doesn't exist - cancel this read
	}
	// Allocate memory using the DuckDB buffer manager
	BufferHandle buffer_handle;
	if (!AllocateInMemCache(buffer_handle, length)) {
		return CANCELED; // allocation failed, cancel this read
	}
	auto buffer_ptr = buffer_handle.Ptr();
	try {
		auto &fs = FileSystem::GetFileSystem(*db);
		fs.Read(*handle, buffer_ptr, length, offset); // Read from disk starting at offset 0
	} catch (const std::exception &e) {
		// File was evicted/deleted after opening but before reading - signal cache miss to fall back to original source
		buffer_handle.Destroy();
		LogDebug("ReadFromCacheFile: read failed (likely evicted during read): '" + file + "': " + string(e.what()));
		return CANCELED;
	}
	std::memcpy(buffer, buffer_ptr, length); // Copy to output buffer

	// Only insert into our memcache if DuckDB's external cache is disabled
	if (use_memcache) {
		InsertRangeIntoMemcache(file, offset, buffer_handle, length);
		return length; // All bytes from mem (just inserted)
	}
	return 0; // All bytes from disk
}

bool Diskcache::WriteToCacheFile(const string &file, const void *buffer, idx_t length) {
	auto db = db_instance.lock();
	if (!db) {
		return false;
	}
	try {
		auto &fs = FileSystem::GetFileSystem(*db);
		auto flags = // Open file for writing in append mode (create if not exists)
		    FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE | FileOpenFlags::FILE_FLAGS_APPEND;
		auto handle = fs.OpenFile(file, flags);
		if (!handle) {
			LogError("WriteToCacheFile: failed to open: '" + file + "'");
			return false;
		}
		// Get current file size to know where we're appending
		int64_t bytes_written = fs.Write(*handle, const_cast<void *>(buffer), length);
		handle->Close(); // Close handle explicitly
		if (bytes_written != static_cast<int64_t>(length)) {
			LogError("WriteToCacheFile: failed to write all bytes to '" + file + "' (wrote " +
			         std::to_string(bytes_written) + " of " + std::to_string(length) + ")");
			return false;
		}
	} catch (const std::exception &e) {
		LogError("WriteToCacheFile: failed to write to '" + file + "': " + string(e.what()));
		return false;
	}
	return true;
}

bool Diskcache::DeleteCacheFile(const string &file) {
	auto db = db_instance.lock();
	if (!db) {
		return false;
	}
	try {
		auto &fs = FileSystem::GetFileSystem(*db);
		fs.RemoveFile(file);
		LogDebug("DeleteCacheFile: deleted file '" + file + "'");
		return true;
	} catch (const std::exception &e) {
		LogError("DeleteCacheFile: failed to delete file '" + file + "': " + string(e.what()));
		return false;
	}
}

//===----------------------------------------------------------------------===//
// Directory management
//===----------------------------------------------------------------------===//
void Diskcache::EnsureDirectoryExists(idx_t file_id) {
	// Derive directory structure from file_id (1M combinations: 4096 * 256)
	idx_t xxx = (file_id / 256) % 4096;
	idx_t yy = file_id % 256;
	idx_t idx = 4096 + xxx * 256 + yy; // Always use XXX/YY structure

	if (subdir_created.test(idx)) { // quick test before lock
		return;
	}
	std::lock_guard<std::mutex> lock(subdir_mutex);
	if (subdir_created.test(idx)) { // avoid race: some thread may just have created it
		return;
	}

	auto db = db_instance.lock();
	if (!db) {
		return;
	}

	// Format directory names as 3-digit and 2-digit hex
	std::ostringstream xxx_stream, yy_stream;
	xxx_stream << std::setfill('0') << std::setw(3) << std::hex << xxx;
	yy_stream << std::setfill('0') << std::setw(2) << std::hex << yy;

	auto dir = diskcache_dir + xxx_stream.str();
	try {
		auto &fs = FileSystem::GetFileSystem(*db);
		if (!fs.DirectoryExists(dir)) {
			fs.CreateDirectory(dir);
		}
		dir += path_sep + yy_stream.str();
		if (!fs.DirectoryExists(dir)) {
			fs.CreateDirectory(dir);
		}
		subdir_created.set(idx);
	} catch (const std::exception &e) {
		LogError("EnsureDirectoryExists: failed to mkdir " + dir + " for file_id " + std::to_string(file_id) + ": " +
		         string(e.what()));
	}
}

//===----------------------------------------------------------------------===//
// Diskcache (re-) configuration
//===----------------------------------------------------------------------===//

void Diskcache::ConfigureCache(idx_t max_size_bytes, const string &base_dir, idx_t max_io_threads) {
	std::lock_guard<std::mutex> lock(diskcache_mutex);
	auto db = db_instance.lock();
	if (!db) {
		return;
	}
	if (!diskcache_initialized) {
		diskcache_dir = base_dir;
		total_cache_capacity = max_size_bytes;

		LogDebug("ConfigureCache: initializing cache: directory='" + diskcache_dir + "' max_size=" +
		         std::to_string(total_cache_capacity) + " bytes io_threads=" + std::to_string(max_io_threads));
		if (!InitCacheDir()) {
			LogError("ConfigureCache: initializing cache directory='" + diskcache_dir + "' failed");
		}
		Clear();
		diskcache_initialized = true;
		// Initialize our own ExternalFileCache instance (always, but only used when DuckDB's is disabled)
		blobfile_memcache = make_uniq<ExternalFileCache>(*db, true);
		LogDebug("ConfigureCache: initialized blobfile_memcache for memory caching of disk-cached files");
		StartIOThreads(max_io_threads);
		return;
	}
	// Cache already initialized, check what needs to be changed
	bool need_restart_threads = (nr_io_threads != max_io_threads);
	bool directory_changed = (diskcache_dir != base_dir);
	bool size_reduced = (max_size_bytes < total_cache_capacity);
	bool size_changed = (total_cache_capacity != max_size_bytes);
	if (!directory_changed && !need_restart_threads && !size_changed) {
		LogDebug("ConfigureCache: stateuration unchanged, no action needed");
		return;
	}

	// Stop existing threads if we need to change thread count or directory
	LogDebug("ConfigureCache: old_dir='" + diskcache_dir + "' new_dir='" + base_dir +
	         "' old_size=" + std::to_string(total_cache_capacity) + " new_size=" + std::to_string(max_size_bytes) +
	         " old_threads=" + std::to_string(nr_io_threads) + " new_threads=" + std::to_string(max_io_threads));
	if (nr_io_threads > 0 && (need_restart_threads || directory_changed)) {
		LogDebug("ConfigureCache: stopping existing cache IO threads for restateuration");
		StopIOThreads();
	}
	// Clear existing cache only if directory changed
	if (directory_changed) {
		LogDebug("ConfigureCache: directory changed, clearing cache");
		Clear();
		if (!CleanCacheDir()) { // Clean old directory before switching
			LogError("ConfigureCache: cleaning cache directory='" + diskcache_dir + "' failed");
		}
		diskcache_dir = base_dir;
		if (!InitCacheDir()) {
			LogError("ConfigureCache: initializing cache directory='" + diskcache_dir + "' failed");
		}
		// Reinitialize blobfile_memcache when directory changes
		blobfile_memcache = make_uniq<ExternalFileCache>(*db, true);
		LogDebug("ConfigureCache: reinitialized blobfile_memcache after directory change");
	}
	// Same directory, just update capacity and evict if needed
	total_cache_capacity = max_size_bytes;
	if (size_reduced && current_cache_size > total_cache_capacity) {
		// Pass 0 to force eviction to exactly the current capacity
		if (!EvictToCapacity(0)) {
			LogError("ConfigureCache: failed to reduce the directory sizes to the new lower capacity/");
		}
	}
	// Start threads if they were stopped or thread count changed
	if (need_restart_threads || directory_changed) {
		StartIOThreads(max_io_threads);
	}
	LogDebug("ConfigureCache complete: directory='" + diskcache_dir + "' max_size=" + to_string(total_cache_capacity) +
	         " bytes io_threads=" + to_string(nr_io_threads));
}

//===----------------------------------------------------------------------===//
// unsafe caching policy based on regexps (non-default)
//===----------------------------------------------------------------------===//
void Diskcache::UpdateRegexPatterns(const string &regex_patterns_str) {
	std::lock_guard<std::mutex> lock(regex_mutex);

	// Store the original regex patterns string for later retrieval
	this->regex_patterns_str = regex_patterns_str;

	cached_regexps.clear(); // Clear existing patterns
	if (regex_patterns_str.empty()) {
		// Conservative mode: empty regexps
		LogDebug("UpdateRegexPatterns: updated to conservative mode (empty regex patterns)");
		return;
	}
	// Aggressive mode: parse semicolon-separated patterns
	vector<string> pattern_strings = StringUtil::Split(regex_patterns_str, ';');
	for (const auto &pattern_str : pattern_strings) {
		if (!pattern_str.empty()) {
			try {
				cached_regexps.emplace_back(pattern_str, std::regex_constants::icase);
				LogDebug("UpdateRegexPatterns: compiled regex pattern: '" + pattern_str + "'");
			} catch (const std::regex_error &e) {
				LogError("UpdateRegexPatterns: wrong regex pattern '" + pattern_str + "': " + string(e.what()));
			}
		}
	}
	LogDebug("UpdateRegexPatterns: now using " + std::to_string(cached_regexps.size()) + " regex patterns");
}

bool Diskcache::CacheUnsafely(const string &uri) const {
	if (!StringUtil::StartsWith(uri, diskcache_dir)) { // never cache own files
		std::lock_guard<std::mutex> lock(regex_mutex);
		if (!cached_regexps.empty()) { // empty is default!
			// the regexps allow unsafe caching (without worrying about etags/modified times): blindly cache
			for (const auto &compiled_pattern : cached_regexps) {
				if (std::regex_search(uri, compiled_pattern)) {
					return true;
				}
			}
		}
	}
	return false;
}

//===----------------------------------------------------------------------===//
// Diskcache - configuration and utility methods
//===----------------------------------------------------------------------===//
bool Diskcache::CleanCacheDir() {
	auto db = db_instance.lock();
	if (!db) {
		return false;
	}
	auto &fs = FileSystem::GetFileSystem(*db);
	if (!fs.DirectoryExists(diskcache_dir)) {
		return true; // Directory doesn't exist, nothing to clean
	}
	auto success = true;

	// Recursive helper lambda to remove directory contents
	std::function<void(const string &)> remove_dir_contents = [&](const string &dir_path) {
		try {
			fs.ListFiles(dir_path, [&](const string &name, bool is_dir) {
				if (name == "." || name == "..") {
					return;
				}
				string item_path = dir_path + path_sep + name;
				if (is_dir) {
					// Recursively remove subdirectory contents first
					remove_dir_contents(item_path);
					// Then remove the subdirectory itself
					try {
						fs.RemoveDirectory(item_path);
					} catch (const std::exception &) {
						success = false;
					}
				} else {
					// Remove file
					try {
						fs.RemoveFile(item_path);
					} catch (const std::exception &) {
						success = false;
					}
				}
			});
		} catch (const std::exception &) {
			success = false;
		}
	};
	// Clean the diskcache directory recursively
	try {
		remove_dir_contents(diskcache_dir);
	} catch (const std::exception &) {
		success = false;
	}
	return success;
}

bool Diskcache::InitCacheDir() {
	auto db = db_instance.lock();
	if (!db) {
		return false;
	}
	auto &fs = FileSystem::GetFileSystem(*db);
	if (!fs.DirectoryExists(diskcache_dir)) {
		try {
			fs.CreateDirectory(diskcache_dir);
		} catch (const std::exception &e) {
			LogError("Failed to create cache directory: " + string(e.what()));
			return false;
		}
	} else {
		if (!CleanCacheDir()) {
			return false;
		}
	}
	// Clear the subdirectory bitset - directories will be created on demand
	std::lock_guard<std::mutex> lock(subdir_mutex);
	subdir_created.reset();
	LogDebug("InitCacheDir: cleared subdirectory creation tracking bitset");
	return true;
}

} // namespace duckdb
