#define DUCKDB_EXTENSION_MAIN

#include "include/diskcache_extension.hpp"
#include "include/diskcache_fs_wrapper.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Table Functions
//===----------------------------------------------------------------------===//

// Bind data for diskcache_config function
struct DiskcacheConfigBindData : public FunctionData {
	idx_t max_size_mb;
	string directory;
	idx_t nr_io_threads;
	string regex_patterns; // New regex patterns parameter
	bool query_only;       // True if no parameters provided - just query current values

	DiskcacheConfigBindData(idx_t size, string dir, idx_t threads, string regexps = "", bool query = false)
	    : max_size_mb(size), directory(std::move(dir)), nr_io_threads(threads), regex_patterns(std::move(regexps)),
	      query_only(query) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DiskcacheConfigBindData>(max_size_mb, directory, nr_io_threads, regex_patterns, query_only);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DiskcacheConfigBindData>();
		return max_size_mb == other.max_size_mb && directory == other.directory &&
		       nr_io_threads == other.nr_io_threads && regex_patterns == other.regex_patterns &&
		       query_only == other.query_only;
	}
	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, TableFunction &function) {
		auto max_size_mb = deserializer.ReadProperty<idx_t>(100, "max_size_mb");
		auto directory = deserializer.ReadProperty<string>(101, "directory");
		auto nr_io_threads = deserializer.ReadProperty<idx_t>(102, "nr_io_threads");
		auto regex_patterns = deserializer.ReadProperty<string>(103, "regex_patterns");
		auto query_only = deserializer.ReadProperty<bool>(104, "query_only");
		return make_uniq<DiskcacheConfigBindData>(max_size_mb, std::move(directory), nr_io_threads,
		                                          std::move(regex_patterns), query_only);
	}
	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const TableFunction &function) {
		auto &bind_data = bind_data_p->Cast<DiskcacheConfigBindData>();
		serializer.WriteProperty(100, "max_size_mb", bind_data.max_size_mb);
		serializer.WriteProperty(101, "directory", bind_data.directory);
		serializer.WriteProperty(102, "nr_io_threads", bind_data.nr_io_threads);
		serializer.WriteProperty(103, "regex_patterns", bind_data.regex_patterns);
		serializer.WriteProperty(104, "query_only", bind_data.query_only);
	}
};

// Empty bind data for diskcache_stats - needed for MotherDuck remote execution serialization
struct DiskcacheStatsBindData : public TableFunctionData {
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DiskcacheStatsBindData>();
	}
	bool Equals(const FunctionData &other) const override {
		return true;
	}
	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
	                      const TableFunction &function) {
	}
	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, TableFunction &function) {
		return make_uniq<DiskcacheStatsBindData>();
	}
};

// Global state for both table functions
struct DiskcacheGlobalState : public GlobalTableFunctionState {
	idx_t tuples_processed = 0;
	vector<DiskcacheRangeInfo> stats;

	idx_t MaxThreads() const override {
		return 1; // Single threaded for simplicity
	}
};

void default_cache_sizes(DatabaseInstance &db, idx_t &max_size_mb, idx_t &nr_io_threads) {
	max_size_mb = db.NumberOfThreads() * 4096; // 4GB * threads
	nr_io_threads = std::min<idx_t>(255, db.NumberOfThreads() * 12);
}

// Bind function for diskcache_config
static unique_ptr<FunctionData> DiskcacheConfigBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns useful cache statistics
	return_types.push_back(LogicalType::BIGINT);  // max_size_MB
	return_types.push_back(LogicalType::VARCHAR); // cache_path
	return_types.push_back(LogicalType::BIGINT);  // current_size_MB
	return_types.push_back(LogicalType::BIGINT);  // io_threads
	return_types.push_back(LogicalType::VARCHAR); // unsafe_regexp_caching
	return_types.push_back(LogicalType::BOOLEAN); // success

	names.push_back("max_size_MB");
	names.push_back("cache_path");
	names.push_back("current_size_MB");
	names.push_back("nr_io_threads");
	names.push_back("unsafe_regexp_caching");
	names.push_back("success");

	bool query_only = false;         // Default to configuration mode
	string directory = ".diskcache"; // Default
	string regex_patterns = "";      // Default empty (conservative mode)
	idx_t max_size_mb, nr_io_threads;
	default_cache_sizes(*context.db, max_size_mb, nr_io_threads);

	// Check if this is a query-only call (no parameters)
	if (input.inputs.size() == 0) {
		query_only = true;
	}
	// Parse arguments if provided (max_size_mb, directory, nr_io_threads, regex_patterns)
	if (input.inputs.size() >= 1) {
		if (input.inputs[0].IsNull()) {
			throw BinderException("diskcache_config: max_size_mb cannot be NULL");
		}
		auto size_val = input.inputs[0];
		if (size_val.type().id() != LogicalTypeId::BIGINT && size_val.type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("diskcache_config: max_size_mb must be an integer");
		}
		max_size_mb = size_val.GetValue<idx_t>();
	}
	if (input.inputs.size() >= 2) {
		if (input.inputs[1].IsNull()) {
			throw BinderException("diskcache_config: directory cannot be NULL");
		}
		directory = StringValue::Get(input.inputs[1]);
	}
	if (input.inputs.size() >= 3) {
		if (input.inputs[2].IsNull()) {
			throw BinderException("diskcache_config: nr_io_threads cannot be NULL");
		}
		auto threads_val = input.inputs[2];
		if (threads_val.type().id() != LogicalTypeId::BIGINT && threads_val.type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("diskcache_config: nr_io_threads must be an integer");
		}
		nr_io_threads = threads_val.GetValue<idx_t>();
		if (nr_io_threads <= 0) {
			throw BinderException("diskcache_config: nr_io_threads must be positive");
		}
		if (nr_io_threads > 256) {
			throw BinderException("diskcache_config: nr_io_threads cannot exceed 256");
		}
	}
	if (input.inputs.size() >= 4) {
		if (input.inputs[3].IsNull()) {
			throw BinderException("diskcache_config: regex_patterns cannot be NULL");
		}
		auto patterns_val = input.inputs[3];
		if (patterns_val.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("diskcache_config: regex_patterns must be a string");
		}
		regex_patterns = StringValue::Get(patterns_val);
	}
	return make_uniq<DiskcacheConfigBindData>(max_size_mb, std::move(directory), nr_io_threads,
	                                          std::move(regex_patterns), query_only);
}

// Init function for diskcache_config global state
static unique_ptr<GlobalTableFunctionState> DiskcacheConfigInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<DiskcacheGlobalState>();
}

// Init function for diskcache_stats global state (same as config)
static unique_ptr<GlobalTableFunctionState> DiskcacheStatsInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	return make_uniq<DiskcacheGlobalState>();
}

// Bind function for diskcache_stats
static unique_ptr<FunctionData> DiskcacheStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns cache statistics with 7 columns
	return_types.push_back(LogicalType::VARCHAR); // uri (with protocol)
	return_types.push_back(LogicalType::VARCHAR); // file
	return_types.push_back(LogicalType::BIGINT);  // range_start
	return_types.push_back(LogicalType::BIGINT);  // range_size
	return_types.push_back(LogicalType::BIGINT);  // usage_count
	return_types.push_back(LogicalType::BIGINT);  // bytes_from_cache
	return_types.push_back(LogicalType::BIGINT);  // bytes_from_mem

	names.push_back("uri");
	names.push_back("file");
	names.push_back("range_start");
	names.push_back("range_size");
	names.push_back("usage_count");
	names.push_back("bytes_from_cache");
	names.push_back("bytes_from_mem");

	return make_uniq<DiskcacheStatsBindData>();
}

// diskcache_config(directory, max_size_mb, nr_io_threads) - Configure the blob cache
static void DiskcacheConfigFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<DiskcacheGlobalState>();

	// Return nothing if we've already processed our single tuple
	if (global_state.tuples_processed >= 1) {
		output.SetCardinality(0);
		return;
	}
	DUCKDB_LOG_DEBUG(*context.db, "[Diskcache] diskcache_config called");

	// Process the single configuration tuple
	auto shared_cache = GetOrCreateDiskcache(*context.db);
	bool success = false;
	idx_t max_size_bytes = 0;
	string cache_path = "";
	idx_t current_size_bytes = 0;
	idx_t writer_threads = 0;
	string regex_patterns = "";

	if (data_p.bind_data && shared_cache) {
		auto &bind_data = data_p.bind_data->Cast<DiskcacheConfigBindData>();

		if (bind_data.query_only) {
			// Query-only mode - just return current values without changing anything
			DUCKDB_LOG_DEBUG(*context.db, "[Diskcache] Querying cache configuration");
			success = true; // Query always succeeds
		} else {
			// Configuration mode - actually configure the cache
			DUCKDB_LOG_DEBUG(*context.db,
			                 "[Diskcache] Configuring cache: directory='%s', max_size=%zu MB, nr_io_threads=%zu, "
			                 "regex_patterns='%s'",
			                 bind_data.directory.c_str(), bind_data.max_size_mb, bind_data.nr_io_threads,
			                 bind_data.regex_patterns.c_str());

			// Configure cache - normalize directory to have trailing separator
			auto dir =
			    bind_data.directory +
			    (StringUtil::EndsWith(bind_data.directory, shared_cache->path_sep) ? "" : shared_cache->path_sep);
			shared_cache->ConfigureCache(bind_data.max_size_mb * 1024 * 1024, dir, bind_data.nr_io_threads);

			// Update regex patterns and purge non-qualifying cache entries
			shared_cache->UpdateRegexPatterns(bind_data.regex_patterns);
			success = true;
			// Now that cache is configured, wrap the filesystem
			WrapExistingFileSystem(*context.db, !bind_data.regex_patterns.empty());
		}
	}
	// Get current cache statistics (works whether configuration succeeded or not)
	if (shared_cache && shared_cache->diskcache_initialized) {
		max_size_bytes = shared_cache->total_cache_capacity;
		cache_path = shared_cache->diskcache_dir;
		current_size_bytes = shared_cache->current_cache_size;
		writer_threads = shared_cache->nr_io_threads;
		regex_patterns = shared_cache->regex_patterns_str;
	}
	// Return the statistics tuple
	output.SetCardinality(1);
	output.data[0].SetValue(0, Value::BIGINT(max_size_bytes / (1024 * 1024)));     // max_size_MB
	output.data[1].SetValue(0, Value(cache_path));                                 // cache_path
	output.data[2].SetValue(0, Value::BIGINT(current_size_bytes / (1024 * 1024))); // current_size_MB
	output.data[3].SetValue(0, Value::BIGINT(writer_threads));                     // nr_io_threads
	output.data[4].SetValue(0, Value(regex_patterns));                             // unsafe_regexp_caching
	output.data[5].SetValue(0, Value::BOOLEAN(success));                           // success
	global_state.tuples_processed = 1;
}

// diskcache_stats() - Return cache statistics in LRU order with chunking
static void DiskcacheStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<DiskcacheGlobalState>();
	// Load data on first call
	if (global_state.tuples_processed == 0) {
		auto cache = GetOrCreateDiskcache(*context.db);
		global_state.stats = cache->GetStatistics();
	}
	auto &stats = global_state.stats;
	idx_t offset = global_state.tuples_processed;

	idx_t chunk_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, stats.size() - offset);
	output.SetCardinality(chunk_size);

	for (idx_t i = 0; i < chunk_size; i++) {
		const auto &info = stats[offset + i];
		output.data[0].SetValue(i, Value(info.uri));
		output.data[1].SetValue(i, Value(info.file));
		output.data[2].SetValue(i, Value::BIGINT(info.range_start));
		output.data[3].SetValue(i, Value::BIGINT(info.range_size));
		output.data[4].SetValue(i, Value::BIGINT(info.usage_count));
		output.data[5].SetValue(i, Value::BIGINT(info.bytes_from_cache));
		output.data[6].SetValue(i, Value::BIGINT(info.bytes_from_mem));
	}
	global_state.tuples_processed += chunk_size;
}

//===----------------------------------------------------------------------===//
// diskcache_hydrate - Scalar function to hydrate (prefetch) ranges into cache
//===----------------------------------------------------------------------===//

struct HydrateRange {
	string uri;
	idx_t start, end;    // range
	idx_t original_size; // Sum of original range sizes
};

struct DiskcacheHydrateBindData : public FunctionData {
	shared_ptr<Diskcache> cache;

	explicit DiskcacheHydrateBindData(shared_ptr<Diskcache> cache_p) : cache(std::move(cache_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DiskcacheHydrateBindData>(cache);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DiskcacheHydrateBindData>();
		return cache == other.cache;
	}
};

static unique_ptr<FunctionData> DiskcacheHydrateBind(ClientContext &context, ScalarFunction &bound_function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	auto cache = GetOrCreateDiskcache(*context.db);
	return make_uniq<DiskcacheHydrateBindData>(cache);
}

static void DiskcacheHydrateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &func_data = func_expr.bind_info->Cast<DiskcacheHydrateBindData>();
	auto &cache = func_data.cache;

	if (!cache || !cache->diskcache_initialized) {
		// Cache not initialized - return FALSE for all rows
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<bool>(result)[0] = false;
		ConstantVector::SetNull(result, false);
		return;
	}

	auto count = args.size();
	auto &uri_vec = args.data[0];
	auto &start_vec = args.data[1];
	auto &size_vec = args.data[2];

	// Flatten vectors to process them
	UnifiedVectorFormat uri_data, start_data, size_data;
	uri_vec.ToUnifiedFormat(count, uri_data);
	start_vec.ToUnifiedFormat(count, start_data);
	size_vec.ToUnifiedFormat(count, size_data);

	auto uri_ptr = UnifiedVectorFormat::GetData<string_t>(uri_data);
	auto start_ptr = UnifiedVectorFormat::GetData<int64_t>(start_data);
	auto size_ptr = UnifiedVectorFormat::GetData<int64_t>(size_data);

	// Helper lambda to schedule a range
	auto schedule_range = [&cache](const HydrateRange &range) {
		DiskcacheReadJob job;
		job.uri = range.uri;
		job.range_start = range.start;
		job.range_size = range.end - range.start;
		cache->QueueIORead(job);
	};

	// Linear traversal with merging (input is already sorted by uri, range_start)
	HydrateRange *current_range = nullptr;
	HydrateRange temp_range;

	for (idx_t i = 0; i < count; i++) {
		auto uri_idx = uri_data.sel->get_index(i);
		auto start_idx = start_data.sel->get_index(i);
		auto size_idx = size_data.sel->get_index(i);

		if (!uri_data.validity.RowIsValid(uri_idx) || !start_data.validity.RowIsValid(start_idx) ||
		    !size_data.validity.RowIsValid(size_idx)) {
			continue; // Skip null rows
		}

		string uri = uri_ptr[uri_idx].GetString();
		idx_t range_start = start_ptr[start_idx];
		idx_t range_size = size_ptr[size_idx];

		if (range_size == 0) {
			continue;
		}
		// Try to merge with current range
		if (current_range != nullptr) {
			// Check if we can merge: same URI and cost-effective
			if (current_range->uri == uri) {
				idx_t concatenated_size = (range_start + range_size) - current_range->start;
				// Concatenate if cheaper to fetch combined than separate
				if (EstimateS3(concatenated_size) < EstimateS3(current_range->original_size) + EstimateS3(range_size)) {
					// Merge this range into current
					current_range->end = range_start + range_size;
					current_range->original_size += range_size;
					continue;
				}
			}
			schedule_range(*current_range); // Cannot merge - schedule current range and start new one
		}

		// Start new range
		temp_range.uri = uri;
		temp_range.start = range_start;
		temp_range.end = range_start + range_size;
		temp_range.original_size = range_size;
		current_range = &temp_range;
	}

	// Schedule final range if any
	if (current_range != nullptr) {
		schedule_range(*current_range);
	}

	// Return TRUE for all input rows
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<bool>(result)[0] = true;
	ConstantVector::SetNull(result, false);
}

//===----------------------------------------------------------------------===//
// DiskcacheExtensionCallback - Automatic wrapping when target extensions load
//===----------------------------------------------------------------------===//
class DiskcacheExtensionCallback : public ExtensionCallback {
public:
	void OnExtensionLoaded(DatabaseInstance &db, const string &name) override {
		auto extension_name = StringUtil::Lower(name);
		if (extension_name == "httpfs" || extension_name == "azure") {
			DUCKDB_LOG_DEBUG(db, "[Diskcache] Target extension '%s' loaded, automatically wrapping filesystem",
			                 name.c_str());
			WrapExistingFileSystem(db);
		}
	}
};

void DiskcacheExtension::Load(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	DUCKDB_LOG_DEBUG(instance, "[Diskcache] Diskcache extension loaded!");
	// Get configuration for callbacks
	auto &config = DBConfig::GetConfig(instance);

	// Register table functions
	DUCKDB_LOG_DEBUG(instance, "[Diskcache] Registering table functions...");

	// Register diskcache_config table function (supports 0, 1, 2, 3, or 4 arguments)
	TableFunction diskcache_config_function("diskcache_config", {}, DiskcacheConfigFunction);
	diskcache_config_function.bind = DiskcacheConfigBind;
	diskcache_config_function.init_global = DiskcacheConfigInitGlobal;
	diskcache_config_function.varargs = LogicalType::ANY; // Allow variable arguments
	diskcache_config_function.serialize = DiskcacheConfigBindData::Serialize;
	diskcache_config_function.deserialize = DiskcacheConfigBindData::Deserialize;
	loader.RegisterFunction(diskcache_config_function);
	DUCKDB_LOG_DEBUG(instance, "[Diskcache] Registered diskcache_config function");

	// Register diskcache_stats table function
	TableFunction diskcache_stats_function("diskcache_stats", {}, DiskcacheStatsFunction);
	diskcache_stats_function.bind = DiskcacheStatsBind;
	diskcache_stats_function.init_global = DiskcacheStatsInitGlobal;
	diskcache_stats_function.serialize = DiskcacheStatsBindData::Serialize;
	diskcache_stats_function.deserialize = DiskcacheStatsBindData::Deserialize;
	loader.RegisterFunction(diskcache_stats_function);
	DUCKDB_LOG_DEBUG(instance, "[Diskcache] Registered diskcache_stats function");

	// Register diskcache_hydrate scalar function
	ScalarFunction diskcache_hydrate_function("diskcache_hydrate",
	                                          {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                                          LogicalType::BOOLEAN, DiskcacheHydrateFunction);
	diskcache_hydrate_function.bind = DiskcacheHydrateBind;
	loader.RegisterFunction(diskcache_hydrate_function);
	DUCKDB_LOG_DEBUG(instance, "[Diskcache] Registered diskcache_hydrate function");

	/// create an initial cache
	idx_t max_size_mb, nr_io_threads;
	default_cache_sizes(instance, max_size_mb, nr_io_threads);
	auto shared_cache = GetOrCreateDiskcache(instance);
	shared_cache->path_sep = instance.GetFileSystem().PathSeparator("");
	shared_cache->ConfigureCache(max_size_mb * 1024 * 1024, ".diskcache" + shared_cache->path_sep, nr_io_threads);

	// Register extension callback for automatic wrapping
	config.extension_callbacks.push_back(make_uniq<DiskcacheExtensionCallback>());

	// Wrap the filesystem (in case some extensions were already loaded)
	WrapExistingFileSystem(instance);
	DUCKDB_LOG_DEBUG(instance, "[Diskcache] Extension initialization complete!");
}

} // namespace duckdb

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(diskcache, loader) {
	duckdb::DiskcacheExtension extension;
	extension.Load(loader);
}
}
#endif

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
