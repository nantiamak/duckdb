//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/nextval.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {
namespace function {

void nextval_function(ExpressionExecutor &exec, Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result);
bool nextval_matches_arguments(vector<SQLType> &arguments);
SQLType nextval_get_return_type(vector<SQLType> &arguments);
unique_ptr<FunctionData> nextval_bind(BoundFunctionExpression &expr, ClientContext &context);

class NextvalFunction {
public:
	static const char *GetName() {
		return "nextval";
	}

	static scalar_function_t GetFunction() {
		return nextval_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return nextval_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return nextval_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nextval_bind;
	}

	static bool HasSideEffects() {
		return true;
	}
};

} // namespace function
} // namespace duckdb
