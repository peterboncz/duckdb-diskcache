#include "disk_cache_fs_wrapper.hpp"
#include "disk_cache.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// DiskCacheFileSystemWrapper Implementation
//===----------------------------------------------------------------------===//

unique_ptr<FileHandle> DiskCacheFileSystemWrapper::OpenFileExtended(const OpenFileInfo &info, FileOpenFlags flags,
                                                                    optional_ptr<FileOpener> opener) {
	auto wrapped_handle = wrapped_fs->OpenFile(info.path, flags, opener);
	if (!wrapped_handle || (!IsFakeS3(info.path) && wrapped_fs->OnDiskFile(*wrapped_handle))) {
		return wrapped_handle; // Never cache file:// URLs as they are already local
	}
	auto lakecache_file = IsFakeS3(info.path) || cache->CacheUnsafely(info.path);
	if (!lakecache_file && info.extended_info) {
		// parquet_scan specialized for a Lake (duck,ice,delta) switch off validation, this allows for safe caching
		const auto &open_options = info.extended_info->options;
		const auto validate_entry = open_options.find("validate_external_file_cache");
		if (validate_entry != open_options.end()) {
			lakecache_file |= !validate_entry->second.GetValue<bool>(); // do not validate => free pass for caching
		}
	}
	if (!lakecache_file) {
		return wrapped_handle; // don't cache, return a normal handle
	}
	return make_uniq<DiskCacheFileHandle>(*this, info.path, std::move(wrapped_handle), cache);
}

static idx_t ReadChunk(duckdb::FileSystem &wrapped_fs, DiskCacheFileHandle &handle, char *buf, idx_t location,
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

		wrapped_fs.Seek(*handle.wrapped_handle, location + nr_cached);
		nr_read = wrapped_fs.Read(*handle.wrapped_handle, buf + nr_cached, nr_read);

		handle.cache->InsertCache(handle.uri, location + nr_cached, nr_read, buf + nr_cached);

		if (nr_read && DiskCacheFileSystemWrapper::IsFakeS3(handle.uri)) {
			std::this_thread::sleep_for(std::chrono::milliseconds(EstimateS3(nr_read))); // simulate S3 latency
		}
	}
	handle.file_position = location + len; // move file position
	return len;
}

void DiskCacheFileSystemWrapper::Read(FileHandle &handle, void *buf, int64_t nr_bytes, idx_t location) {
	auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
	if (!blob_handle.cache || !cache->disk_cache_initialized) {
		wrapped_fs->Read(*blob_handle.wrapped_handle, buf, nr_bytes, location);
		return; // a read that cannot cache
	}
	// the ReadFromCache() can break down one large range into multiple around caching boundaries
	char *buf_ptr = static_cast<char *>(buf);
	idx_t chunk_bytes;
	do { // keep iterating over ranges
		chunk_bytes = ReadChunk(*wrapped_fs, blob_handle, buf_ptr, location, nr_bytes);
		nr_bytes -= chunk_bytes;
		location += chunk_bytes;
		buf_ptr += chunk_bytes;
	} while (nr_bytes > 0 && chunk_bytes > 0); //  not done reading and not EOF
}

int64_t DiskCacheFileSystemWrapper::Read(FileHandle &handle, void *buf, int64_t nr_bytes) {
	auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
	if (!blob_handle.cache || !cache->disk_cache_initialized) {
		return wrapped_fs->Read(*blob_handle.wrapped_handle, buf, nr_bytes);
	}
	return ReadChunk(*wrapped_fs, blob_handle, static_cast<char *>(buf), blob_handle.file_position, nr_bytes);
}

void DiskCacheFileSystemWrapper::Write(FileHandle &handle, void *buf, int64_t nr_bytes, idx_t location) {
	auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
	wrapped_fs->Write(*blob_handle.wrapped_handle, buf, nr_bytes, location);
	blob_handle.file_position = location + nr_bytes;
	if (blob_handle.cache && cache->disk_cache_initialized) { // Cache the written data
		blob_handle.cache->InsertCache(blob_handle.uri, location, nr_bytes, buf);
	}
}

int64_t DiskCacheFileSystemWrapper::Write(FileHandle &handle, void *buf, int64_t nr_bytes) {
	auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
	idx_t write_location = blob_handle.file_position;
	nr_bytes = wrapped_fs->Write(*blob_handle.wrapped_handle, buf, nr_bytes);
	if (nr_bytes > 0) {
		blob_handle.file_position += nr_bytes;
		if (blob_handle.cache && cache->disk_cache_initialized) { // Cache the written data
			blob_handle.cache->InsertCache(blob_handle.uri, write_location, nr_bytes, buf);
		}
	}
	return nr_bytes;
}

//===----------------------------------------------------------------------===//
// Cache Management Functions
//===----------------------------------------------------------------------===//
shared_ptr<DiskCache> GetOrCreateDiskCache(DatabaseInstance &instance) {
	auto &object_cache = instance.GetObjectCache();
	auto cached_entry = object_cache.Get<DiskCacheObjectCacheEntry>("disk_cache_instance");
	if (cached_entry) {
		DUCKDB_LOG_DEBUG(instance, "[DiskCache] Retrieved existing DiskCache from ObjectCache");
		return cached_entry->cache;
	}
	// Create new cache and store in ObjectCache
	auto new_cache = make_shared_ptr<DiskCache>(&instance);
	auto cache_entry = make_shared_ptr<DiskCacheObjectCacheEntry>(new_cache);
	object_cache.Put("disk_cache_instance", cache_entry);
	DUCKDB_LOG_DEBUG(instance, "[DiskCache] Created and stored new DiskCache in ObjectCache");
	return new_cache;
}

void WrapExistingFilesystems(DatabaseInstance &instance) {
	auto &db_fs = FileSystem::GetFileSystem(instance);
	bool fake_s3_seen = false;
	DUCKDB_LOG_DEBUG(instance, "[DiskCache] Filesystem type: %s", db_fs.GetName().c_str());
	auto vfs = dynamic_cast<VirtualFileSystem *>(&db_fs); // Get VirtualFileSystem
	if (!vfs) {
		auto *opener_fs = dynamic_cast<OpenerFileSystem *>(&db_fs);
		if (opener_fs) {
			DUCKDB_LOG_DEBUG(instance, "[DiskCache] Found OpenerFileSystem, getting wrapped VFS");
			vfs = dynamic_cast<VirtualFileSystem *>(&opener_fs->GetFileSystem());
		}
	}
	if (!vfs) {
		DUCKDB_LOG_DEBUG(instance, "[DiskCache] Cannot find VirtualFileSystem - skipping filesystem wrapping");
		return;
	}
	auto shared_cache = GetOrCreateDiskCache(instance);
	if (!shared_cache->disk_cache_initialized) {
		DUCKDB_LOG_DEBUG(instance, "[DiskCache] Cache not initialized yet, skipping filesystem wrapping");
		return;
	}
	// Target blob storage filesystems to wrap
	vector<string> target_filesystems = {"AzureBlobStorageFileSystem", "HuggingFaceFileSystem", "S3FileSystem",
	                                     "HTTPFileSystem"}; // this one is used for http[s], gcs and r2

	// Try to wrap each target blob storage filesystem
	auto subsystems = vfs->ListSubSystems();
	DUCKDB_LOG_DEBUG(instance, "[DiskCache] Found %zu registered subsystems", subsystems.size());

	for (const auto &name : subsystems) {
		DUCKDB_LOG_DEBUG(instance, "[DiskCache] Processing subsystem: '%s'", name.c_str());
		if (name.find("DiskCache:") == 0) { // Skip if already wrapped (starts with "DiskCache:")
			fake_s3_seen |= (name == "DiskCache:fake_s3");
			DUCKDB_LOG_DEBUG(instance, "[DiskCache] Skipping already wrapped subsystem: '%s'", name.c_str());
			continue;
		}
		// Only wrap target blob storage filesystems
		bool is_target = false;
		for (const auto &target : target_filesystems) {
			if (name == target) {
				is_target = true;
				break;
			}
		}
		if (!is_target) {
			DUCKDB_LOG_DEBUG(instance, "[DiskCache] Skipping non-target subsystem: '%s'", name.c_str());
			continue;
		}
		auto extracted_fs = vfs->ExtractSubSystem(name);
		if (extracted_fs) {
			DUCKDB_LOG_DEBUG(instance, "[DiskCache] Successfully extracted subsystem: '%s' (GetName returns: '%s')",
			                 name.c_str(), extracted_fs->GetName().c_str());
			auto wrapped_fs = make_uniq<DiskCacheFileSystemWrapper>(std::move(extracted_fs), shared_cache);
			DUCKDB_LOG_DEBUG(instance, "[DiskCache] Created wrapper with name: '%s'", wrapped_fs->GetName().c_str());
			vfs->RegisterSubSystem(std::move(wrapped_fs));
			DUCKDB_LOG_DEBUG(instance, "[DiskCache] Successfully registered wrapped subsystem for '%s'", name.c_str());
		} else {
			DUCKDB_LOG_ERROR(instance, "[DiskCache] Failed to extract '%s' - subsystem not wrapped", name.c_str());
		}
	}
	if (!fake_s3_seen) { // Register fakes3:// filesystem for testing purposes - wrapped with caching
		DUCKDB_LOG_DEBUG(instance, "[DiskCache] Registering fake_s3:// filesystem for testing");
		auto fake_s3_fs = make_uniq<FakeS3FileSystem>();
		auto wrapped_fake_s3_fs = make_uniq<DiskCacheFileSystemWrapper>(std::move(fake_s3_fs), shared_cache);
		vfs->RegisterSubSystem(std::move(wrapped_fake_s3_fs));
	}

	// Log final subsystem list
	auto final_subsystems = vfs->ListSubSystems();
	DUCKDB_LOG_DEBUG(instance, "[DiskCache] After wrapping, have %zu subsystems", final_subsystems.size());
	for (const auto &name : final_subsystems) {
		DUCKDB_LOG_DEBUG(instance, "[DiskCache] - %s", name.c_str());
	}
}

} // namespace duckdb
