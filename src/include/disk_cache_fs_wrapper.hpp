#pragma once

#include "duckdb.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "disk_cache.hpp"

namespace duckdb {

// Forward declarations
struct DiskCache;
class DiskCacheFileHandle;

//===----------------------------------------------------------------------===//
// DiskCacheObjectCacheEntry - ObjectCache wrapper for DiskCache
//===----------------------------------------------------------------------===//
class DiskCacheObjectCacheEntry : public ObjectCacheEntry {
public:
	shared_ptr<DiskCache> cache;
	explicit DiskCacheObjectCacheEntry(shared_ptr<DiskCache> cache_p) : cache(std::move(cache_p)) {
	}
	string GetObjectType() override {
		return "DiskCache";
	}
	static string ObjectType() {
		return "DiskCache";
	}
	~DiskCacheObjectCacheEntry() override = default;
};

//===----------------------------------------------------------------------===//
// DiskCacheFileHandle - wraps original file handles to intercept reads
//===----------------------------------------------------------------------===//
class DiskCacheFileHandle : public FileHandle {
public:
	DiskCacheFileHandle(FileSystem &fs, string original_path, unique_ptr<FileHandle> wrapped_handle,
	                    shared_ptr<DiskCache> cache)
	    : FileHandle(fs, wrapped_handle->GetPath(), wrapped_handle->GetFlags()),
	      wrapped_handle(std::move(wrapped_handle)), cache(cache), uri(std::move(original_path)), file_position(0) {
	}
	~DiskCacheFileHandle() override = default;
	void Close() override {
		if (wrapped_handle) {
			wrapped_handle->Close();
		}
	}

public:
	unique_ptr<FileHandle> wrapped_handle;
	shared_ptr<DiskCache> cache;
	string uri;          // original uri
	idx_t file_position; // Track our own file position
};

//===----------------------------------------------------------------------===//
// DiskCacheFileSystemWrapper - wraps the original blob filesystems with caching
//===----------------------------------------------------------------------===//
class DiskCacheFileSystemWrapper : public FileSystem {
public:
	explicit DiskCacheFileSystemWrapper(unique_ptr<FileSystem> wrapped_fs, shared_ptr<DiskCache> shared_cache)
	    : wrapped_fs(std::move(wrapped_fs)), cache(shared_cache) {
	}
	virtual ~DiskCacheFileSystemWrapper() = default;

	static bool IsFakeS3(const string &path) {
		return StringUtil::Lower(path.substr(0, 8)) == "fake_s3:";
	}

protected:
	bool SupportsOpenFileExtended() const override {
		return true;
	}
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;

public:
	// FileSystem interface implementation
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override {
		return OpenFileExtended(OpenFileInfo(path), flags, opener);
	}
	// read ops are our caching opportunity -- worked out in cpp file
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	// write ops just wrap but also invalidate the file from the cache -- worked out in cpp file
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	// write operations should invalidate the cache
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override {
		if (cache) {
			cache->EvictEntry(source);
			cache->EvictEntry(target);
		}
		wrapped_fs->MoveFile(source, target, opener);
	}
	void RemoveFile(const string &uri, optional_ptr<FileOpener> opener) override {
		if (cache) {
			cache->EvictEntry(uri);
		}
		wrapped_fs->RemoveFile(uri, opener);
	}
	bool TryRemoveFile(const string &uri, optional_ptr<FileOpener> opener = nullptr) override {
		if (cache) {
			cache->EvictEntry(uri);
		}
		return wrapped_fs->TryRemoveFile(uri, opener);
	}
	// these two are not expected to be implemented in blob filesystems, but for completeness/safety they evict as well
	void Truncate(FileHandle &handle, int64_t new_size) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		if (blob_handle.cache) {
			blob_handle.cache->EvictEntry(blob_handle.uri);
		}
		wrapped_fs->Truncate(*blob_handle.wrapped_handle, new_size);
	}
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		if (cache) {
			cache->EvictEntry(blob_handle.uri);
		}
		return wrapped_fs->Trim(*blob_handle.wrapped_handle, offset_bytes, length_bytes);
	}

	// we give the FS a wrapped name
	string GetName() const override {
		return "DiskCache:" + wrapped_fs->GetName();
	}

	// we keep track of the seek position
	void Seek(FileHandle &handle, idx_t location) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		blob_handle.file_position = location;
		if (blob_handle.wrapped_handle) {
			wrapped_fs->Seek(*blob_handle.wrapped_handle, location);
		}
	}
	void Reset(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		blob_handle.file_position = 0;
		if (blob_handle.wrapped_handle) {
			wrapped_fs->Reset(*blob_handle.wrapped_handle);
		}
	}
	idx_t SeekPosition(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		return blob_handle.file_position;
	}

	// simple pass-through wrappers around all other methods
	int64_t GetFileSize(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		return wrapped_fs->GetFileSize(*blob_handle.wrapped_handle);
	}
	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		return wrapped_fs->GetLastModifiedTime(*blob_handle.wrapped_handle);
	}
	string GetVersionTag(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		return wrapped_fs->GetVersionTag(*blob_handle.wrapped_handle);
	}
	FileType GetFileType(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		return wrapped_fs->GetFileType(*blob_handle.wrapped_handle);
	}
	void FileSync(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		wrapped_fs->FileSync(*blob_handle.wrapped_handle);
	}
	bool CanSeek() override {
		return wrapped_fs->CanSeek();
	}
	bool OnDiskFile(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<DiskCacheFileHandle>();
		return wrapped_fs->OnDiskFile(*blob_handle.wrapped_handle);
	}
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		return wrapped_fs->DirectoryExists(directory, opener);
	}
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		wrapped_fs->CreateDirectory(directory, opener);
	}
	void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr) override {
		wrapped_fs->CreateDirectoriesRecursive(path, opener);
	}
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		wrapped_fs->RemoveDirectory(directory, opener);
	}
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		return wrapped_fs->ListFiles(directory, callback, opener);
	}
	bool FileExists(const string &uri, optional_ptr<FileOpener> opener = nullptr) override {
		return wrapped_fs->FileExists(uri, opener);
	}
	bool IsPipe(const string &uri, optional_ptr<FileOpener> opener = nullptr) override {
		return wrapped_fs->IsPipe(uri, opener);
	}
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override {
		return wrapped_fs->Glob(path, opener);
	}
	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override {
		wrapped_fs->RegisterSubSystem(std::move(sub_fs));
	}
	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override {
		wrapped_fs->RegisterSubSystem(compression_type, std::move(fs));
	}
	void UnregisterSubSystem(const string &name) override {
		wrapped_fs->UnregisterSubSystem(name);
	}
	unique_ptr<FileSystem> ExtractSubSystem(const string &name) override {
		return wrapped_fs->ExtractSubSystem(name);
	}
	vector<string> ListSubSystems() override {
		return wrapped_fs->ListSubSystems();
	}
	bool CanHandleFile(const string &fpath) override {
		return wrapped_fs->CanHandleFile(fpath);
	}
	string PathSeparator(const string &path) override {
		return wrapped_fs->PathSeparator(path);
	}
	string GetHomeDirectory() override {
		return wrapped_fs->GetHomeDirectory();
	}
	string ExpandPath(const string &path) override {
		return wrapped_fs->ExpandPath(path);
	}
	bool IsManuallySet() override {
		return wrapped_fs->IsManuallySet();
	}
	void SetDisabledFileSystems(const vector<string> &names) override {
		wrapped_fs->SetDisabledFileSystems(names);
	}
	bool SubSystemIsDisabled(const string &name) override {
		return wrapped_fs->SubSystemIsDisabled(name);
	}
	unique_ptr<FileHandle> OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle,
	                                          bool write) override {
		return wrapped_fs->OpenCompressedFile(context, std::move(handle), write);
	}

private:
	unique_ptr<FileSystem> wrapped_fs;
	shared_ptr<DiskCache> cache;
};

class FakeS3FileSystem : public LocalFileSystem {
public:
	FakeS3FileSystem() : LocalFileSystem() {
	}
	~FakeS3FileSystem() override = default;

	bool OnDiskFile(FileHandle &handle) override {
		return false; // the point of this fake fs is to pretend it is S3, ie a non-disk fs
	}
	string GetName() const override {
		return "fake_s3"; // Override GetName to identify as fake_s3:// filesystem
	}
	// Override all methods manipulating paths to strip/add the fake_s3:// path prefix
	unique_ptr<FileHandle> OpenFile(const string &uri, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		auto file_path = uri;
		if (DiskCacheFileSystemWrapper::IsFakeS3(uri)) {
			file_path = StripFakeS3Prefix(uri);
			if (flags.CreateFileIfNotExists()) { // S3 can write without first creating 'subdirs' - so FakeS3 can also
				auto last_slash = file_path.rfind('/'); // what about windoze??
				if (last_slash != std::string::npos) {
					LocalFileSystem::CreateDirectoriesRecursive(file_path.substr(0, last_slash), opener);
				}
			}
		}
		return LocalFileSystem::OpenFile(file_path, flags, opener);
	}
	bool FileExists(const string &uri, optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::FileExists(StripFakeS3Prefix(uri), opener);
	}
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::DirectoryExists(StripFakeS3Prefix(directory), opener);
	}
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		// DuckDB's IsRemoteFile() is hardcoded and does not recognize fake_s3:// - shouldn't it call !OnDiskFile??
		// so DuckLakeInsert::GetCopyOptions starts to create directories  were it should not..
		// DuckLakeInsert::GetCopyOptions then invokes LocalFilesystem:CreateDirectoriesRecursive on the local FS
		// LocalFilesystem:CreateDirectoriesRecursive then does call us here, but with fake_s3: as toplevel dir
		if (!DiskCacheFileSystemWrapper::IsFakeS3(directory)) {
			LocalFileSystem::CreateDirectory(directory, opener);
		} else if (directory.length() > 10) { // hack to prevent DuckLakeInsert::GetCopyOptions from creating /fake_s3:
			LocalFileSystem::CreateDirectoriesRecursive(StripFakeS3Prefix(directory), opener);
		}
	}
	void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr) override {
		LocalFileSystem::CreateDirectoriesRecursive(StripFakeS3Prefix(path), opener);
	}
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		LocalFileSystem::RemoveDirectory(StripFakeS3Prefix(directory), opener);
	}
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		auto wrapped_callback = [&callback](const string &path, bool is_directory) {
			callback("fake_s3://" + path, is_directory); // Pretend the paths start with fake_s3://
		};
		return LocalFileSystem::ListFiles(StripFakeS3Prefix(directory), wrapped_callback, opener);
	}
	bool TryRemoveFile(const string &uri, optional_ptr<FileOpener> opener = nullptr) override {
		return LocalFileSystem::TryRemoveFile(StripFakeS3Prefix(uri), opener);
	}
	bool CanHandleFile(const string &fpath) override {
		return DiskCacheFileSystemWrapper::IsFakeS3(fpath);
	}
	string ExpandPath(const string &path) override {
		return "fake_s3://" + LocalFileSystem::ExpandPath(StripFakeS3Prefix(path));
	}
	vector<OpenFileInfo> Glob(const string &uri, FileOpener *opener = nullptr) override {
		auto results = LocalFileSystem::Glob(StripFakeS3Prefix(uri), opener);
		for (auto &info : results) {
			info.path = "fake_s3://" + info.path; // Pretend the paths start with fake_s3://
		}
		return results;
	}

private:
	string StripFakeS3Prefix(const string &uri) {
		return DiskCacheFileSystemWrapper::IsFakeS3(uri) ? std::move(uri.substr(10)) : uri;
	}
};

// Cache management functions
shared_ptr<DiskCache> GetOrCreateDiskCache(DatabaseInstance &instance);

// Filesystem wrapping utility function
void WrapExistingFilesystems(DatabaseInstance &instance);

} // namespace duckdb
