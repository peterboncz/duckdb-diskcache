#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/config.hpp"
#include "diskcache.hpp"
#include "diskcache_fs_wrapper.hpp"

namespace duckdb {

class DiskcacheExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override {
		return "diskcache";
	}
	std::string Version() const override {
		return "0.1";
	}
};

} // namespace duckdb
