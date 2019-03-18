//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/types/static_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/vector.hpp"

#include <type_traits>

namespace duckdb {

//! The StaticVector is an alias for creating a vector of a specific type
template <class T> class StaticVector : public Vector {
public:
	StaticVector() {
		owned_data = unique_ptr<char[]>(new char[sizeof(T) * STANDARD_VECTOR_SIZE]);
		data = owned_data.get();
		if (std::is_same<T, bool>::value) {
			type = TypeId::BOOLEAN;
		} else if (std::is_same<T, int8_t>::value) {
			type = TypeId::TINYINT;
		} else if (std::is_same<T, int16_t>::value) {
			type = TypeId::SMALLINT;
		} else if (std::is_same<T, int>::value) {
			type = TypeId::INTEGER;
		} else if (std::is_same<T, int64_t>::value) {
			type = TypeId::BIGINT;
		} else if (std::is_same<T, uint64_t>::value) {
			type = TypeId::POINTER;
		} else if (std::is_same<T, float>::value) {
			type = TypeId::FLOAT;
		} else if (std::is_same<T, double>::value) {
			type = TypeId::DOUBLE;
		} else {
			// unsupported type!
			assert(0);
		}
	}
};

} // namespace duckdb
