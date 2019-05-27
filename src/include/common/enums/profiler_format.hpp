//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/profiler_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

enum class ProfilerPrintFormat : uint8_t { NONE, QUERY_TREE, JSON };

} // namespace duckdb
