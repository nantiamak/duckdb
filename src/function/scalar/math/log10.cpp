#include "function/scalar/math_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void log10_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                    Vector &result) {
	assert(input_count == 1);
	result.Initialize(inputs[0].type);
	VectorOperations::Log10(inputs[0], result);
}

void Log10::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("log10", { SQLType::DOUBLE }, SQLType::DOUBLE, log10_function));
}

}
