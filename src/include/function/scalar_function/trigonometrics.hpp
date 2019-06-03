//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/scalar_function/trigonometrics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {

void sin_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                  Vector &result);

void cos_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                  Vector &result);

void tan_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                  Vector &result);

void asin_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

void acos_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

void atan_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                   Vector &result);

void cot_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                  Vector &result);

// special snowflake, two parameters
void atan2_function(ExpressionExecutor &exec, Vector inputs[], count_t input_count, BoundFunctionExpression &expr,
                    Vector &result);

bool trig_matches_arguments(vector<SQLType> &arguments);
bool atan2_matches_arguments(vector<SQLType> &arguments);
SQLType trig_get_return_type(vector<SQLType> &arguments);

class SinFunction {
public:
	static const char *GetName() {
		return "sin";
	}

	static scalar_function_t GetFunction() {
		return sin_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return trig_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return trig_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

class CosFunction {
public:
	static const char *GetName() {
		return "cos";
	}

	static scalar_function_t GetFunction() {
		return cos_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return trig_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return trig_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

class TanFunction {
public:
	static const char *GetName() {
		return "tan";
	}

	static scalar_function_t GetFunction() {
		return tan_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return trig_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return trig_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

class ASinFunction {
public:
	static const char *GetName() {
		return "asin";
	}

	static scalar_function_t GetFunction() {
		return asin_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return trig_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return trig_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

class ACosFunction {
public:
	static const char *GetName() {
		return "acos";
	}

	static scalar_function_t GetFunction() {
		return acos_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return trig_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return trig_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

class ATanFunction {
public:
	static const char *GetName() {
		return "atan";
	}

	static scalar_function_t GetFunction() {
		return atan_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return trig_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return trig_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

class CoTFunction {
public:
	static const char *GetName() {
		return "cot";
	}

	static scalar_function_t GetFunction() {
		return cot_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return trig_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return trig_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

class ATan2Function {
public:
	static const char *GetName() {
		return "atan2";
	}

	static scalar_function_t GetFunction() {
		return atan2_function;
	}

	static matches_argument_function_t GetMatchesArgumentFunction() {
		return atan2_matches_arguments;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return trig_get_return_type;
	}

	static bind_scalar_function_t GetBindFunction() {
		return nullptr;
	}

	static dependency_function_t GetDependencyFunction() {
		return nullptr;
	}

	static bool HasSideEffects() {
		return false;
	}
};

} // namespace duckdb
