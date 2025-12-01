#include "include/diskcache_fs_wrapper.hpp"
#include "duckdb/main/database_file_opener.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// DiskcacheFileSystemWrapper Implementation
//===----------------------------------------------------------------------===//

unique_ptr<FileHandle> DiskcacheFileSystemWrapper::OpenFileExtended(const OpenFileInfo &info, FileOpenFlags flags,
                                                                    optional_ptr<FileOpener> opener) {
	auto wrapped_handle = wrapped_fs->OpenFile(info.path, flags, opener);
	if (!wrapped_handle) {
		return nullptr;
	}
	auto cache_file = IsFakeS3(info.path) || cache->CacheUnsafely(info.path);
	if (!cache_file && info.extended_info && !wrapped_handle->OnDiskFile()) {
		// parquet_scan specialized for a Lake (duck,ice,delta) switch off validation, this allows for safe caching
		const auto &open_options = info.extended_info->options;
		const auto validate_entry = open_options.find("validate_external_file_cache");
		if (validate_entry != open_options.end()) {
			cache_file |= !validate_entry->second.GetValue<bool>(); // do not validate => free pass for caching
		}
	}
	// Don't wrap if we won't cache AND the handle can't seek (e.g., compressed files)
	// Returning the original handle preserves correct CanSeek() behavior for non-seekable streams
	if (!cache_file && !wrapped_handle->CanSeek()) {
		return wrapped_handle;
	}
	// Wrap the handle - cache member will be nullptr for non-cached files, disabling caching logic
	return make_uniq<DiskcacheFileHandle>(*this, info.path, std::move(wrapped_handle), cache_file ? cache : nullptr);
}

static idx_t ReadChunk(duckdb::FileSystem &wrapped_fs, DiskcacheFileHandle &handle, char *buf, idx_t location,
                       idx_t len) {
	// NOTE: ReadFromCache() can return cached_bytes == 0 but adjust max_nr_bytes downwards to align with a cached range
	handle.cache->LogDebug("ReadChunk(path=" + handle.uri + ", location=" + to_string(location) +
	                       ", max_nr_bytes=" + to_string(len) + ")");
	idx_t nr_cached = handle.cache->ReadFromCache(handle.uri, location, len, buf);
#if 0
    if (nr_cached > 0) { // debug
		char *tmp_buf = new char[nr_cached];
		wrapped_fs.Seek(*blob_handle.wrapped_handle, location);
		idx_t tst_bytes = wrapped_fs.Read(*blob_handle.wrapped_handle, tmp_buf, nr_cached);
		if (tst_bytes != nr_cached) {
			throw "unable to read";
		} else if (memcmp(tmp_buf, buf, nr_cached)) {
			throw "unequal contents";
		}
	}
#endif
	if (len > nr_cached) { // Read the non-cached range and cache it
		idx_t nr_read = len - nr_cached;

		// Use the seeking Read to avoid issues with filesystems that don't track position correctly
		wrapped_fs.Read(*handle.wrapped_handle, buf + nr_cached, nr_read, location + nr_cached);
		// nr_read stays as requested since seeking Read throws on short read

		handle.cache->InsertCache(handle.uri, location + nr_cached, nr_read, buf + nr_cached);

		if (nr_read && DiskcacheFileSystemWrapper::IsFakeS3(handle.uri)) {
			std::this_thread::sleep_for(std::chrono::milliseconds(EstimateS3(nr_read))); // simulate S3 latency
		}
	}
	handle.file_position = location + len; // move file position
	return len;
}

void DiskcacheFileSystemWrapper::Read(FileHandle &handle, void *buf, int64_t nr_bytes, idx_t location) {
	auto &cache_handle = handle.Cast<DiskcacheFileHandle>();
	if (cache_handle.cache && cache->diskcache_initialized) {
		// the ReadFromCache() can break down one large range into multiple around caching boundaries
		char *buf_ptr = static_cast<char *>(buf);
		idx_t chunk_bytes;
		do { // keep iterating over ranges
			chunk_bytes = ReadChunk(*wrapped_fs, cache_handle, buf_ptr, location, nr_bytes);
			nr_bytes -= chunk_bytes;
			location += chunk_bytes;
			buf_ptr += chunk_bytes;
		} while (nr_bytes > 0 && chunk_bytes > 0); //  not done reading and not EOF
		return;
	}
	wrapped_fs->Read(*cache_handle.wrapped_handle, buf, nr_bytes, location);
}

int64_t DiskcacheFileSystemWrapper::Read(FileHandle &handle, void *buf, int64_t nr_bytes) {
	auto &cache_handle = handle.Cast<DiskcacheFileHandle>();
	if (cache_handle.cache && cache->diskcache_initialized) {
		return ReadChunk(*wrapped_fs, cache_handle, static_cast<char *>(buf), cache_handle.file_position, nr_bytes);
	}
	return wrapped_fs->Read(*cache_handle.wrapped_handle, buf, nr_bytes);
}

void DiskcacheFileSystemWrapper::Write(FileHandle &handle, void *buf, int64_t nr_bytes, idx_t location) {
	auto &cache_handle = handle.Cast<DiskcacheFileHandle>();
	cache_handle.file_position = location + nr_bytes;
	if (cache_handle.cache && cache->diskcache_initialized) {
		cache_handle.cache->InsertCache(cache_handle.uri, location, nr_bytes, buf);
	}
	wrapped_fs->Write(*cache_handle.wrapped_handle, buf, nr_bytes, location);
}

int64_t DiskcacheFileSystemWrapper::Write(FileHandle &handle, void *buf, int64_t nr_bytes) {
	auto &cache_handle = handle.Cast<DiskcacheFileHandle>();
	idx_t write_location = cache_handle.file_position;
	nr_bytes = wrapped_fs->Write(*cache_handle.wrapped_handle, buf, nr_bytes);
	if (nr_bytes > 0) {
		cache_handle.file_position += nr_bytes;
		if (cache_handle.cache && cache->diskcache_initialized) {
			cache_handle.cache->InsertCache(cache_handle.uri, write_location, nr_bytes, buf);
		}
	}
	return nr_bytes;
}

//===----------------------------------------------------------------------===//
// Cache Management Functions
//===----------------------------------------------------------------------===//
shared_ptr<Diskcache> GetOrCreateDiskcache(DatabaseInstance &instance) {
	auto &object_cache = instance.GetObjectCache();
	auto cached_entry = object_cache.Get<DiskcacheObjectCacheEntry>("diskcache_instance");
	if (cached_entry) {
		DUCKDB_LOG_DEBUG(instance, "[Diskcache] Retrieved existing Diskcache from ObjectCache");
		return cached_entry->cache;
	}
	// Create new cache and store in ObjectCache
	auto new_cache = make_shared_ptr<Diskcache>(&instance);
	auto cache_entry = make_shared_ptr<DiskcacheObjectCacheEntry>(new_cache);
	object_cache.Put("diskcache_instance", cache_entry);
	DUCKDB_LOG_DEBUG(instance, "[Diskcache] Created and stored new Diskcache in ObjectCache");
	return new_cache;
}

void WrapExistingFileSystem(DatabaseInstance &instance, bool unsafe_caching_enabled) {
	auto &config = DBConfig::GetConfig(instance);
	DUCKDB_LOG_DEBUG(instance, "[Diskcache] Wrapping existing filesystem");
	auto shared_cache = GetOrCreateDiskcache(instance);
	if (!shared_cache->diskcache_initialized) {
		DUCKDB_LOG_DEBUG(instance, "[Diskcache] Cache not initialized yet, skipping filesystem wrapping");
		return;
	}

	// Check if the filesystem is a VirtualFileSystem
	auto *wfs = dynamic_cast<DiskcacheFileSystemWrapper *>(config.file_system.get());
	if (!wfs) {
		auto *vfs = dynamic_cast<VirtualFileSystem *>(config.file_system.get());
		if (vfs) {
			DUCKDB_LOG_DEBUG(instance, "[Diskcache] Registering fake_s3:// filesystem for testing");
			auto fake_s3_fs = make_uniq<FakeS3FileSystem>();
			vfs->RegisterSubSystem(std::move(fake_s3_fs));
		}
		// Not a VirtualFileSystem - just wrap the entire filesystem without fake_s3
		DUCKDB_LOG_DEBUG(instance, "[Diskcache] Filesystem is not a VirtualFileSystem, wrapping entire filesystem");
		auto wrapped_fs = make_uniq<DiskcacheFileSystemWrapper>(std::move(config.file_system), shared_cache);
		DUCKDB_LOG_DEBUG(instance, "[Diskcache] --%s", wrapped_fs->GetName().c_str());
		config.file_system = std::move(wrapped_fs);
		DUCKDB_LOG_DEBUG(instance, "[Diskcache] Successfully wrapped filesystem");
	}
}

} // namespace duckdb
