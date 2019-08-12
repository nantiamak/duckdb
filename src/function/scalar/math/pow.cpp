#include "function/scalar/math_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "planner/expression/bound_function_expression.hpp"

using namespace std;

namespace duckdb {

static void pow_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr, Vector &result) {
	result.Initialize(TypeId::DOUBLE);
	VectorOperations::Pow(inputs[0], inputs[1], result);
}

void Pow::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction( { SQLType::DOUBLE, SQLType::DOUBLE }, SQLType::DOUBLE, pow_function));
}

}
