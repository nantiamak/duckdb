//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/index_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Index Types
//===--------------------------------------------------------------------===//
enum class IndexType {
	INVALID = 0, // invalid index type
	ART = 1      // Adaptive Radix Tree
};

} // namespace duckdb
