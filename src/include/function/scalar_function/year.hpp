//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/year.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

void year_function(Vector inputs[], size_t input_count, FunctionExpression &expr, Vector &result);
bool year_matches_arguments(vector<TypeId> &arguments);
TypeId year_get_return_type(vector<TypeId> &arguments);

class YearFunction {
public:
	static const char *GetName() {
		return "year";
	}

	static scalar_function_t GetFunction() {
		return year_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return year_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return year_get_return_type;
	}
};

} // namespace function
} // namespace duckdb
