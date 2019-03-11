#include "function/scalar_function/length.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {
namespace function {



void length_function(Vector inputs[], size_t input_count, BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 1);
	auto &input = inputs[0];
	assert(input.type == TypeId::VARCHAR);

	result.Initialize(TypeId::BIGINT);
	result.nullmask = input.nullmask;
	result.count = input.count;
	result.sel_vector = input.sel_vector;

	auto result_data = (int64_t *)result.data;
	auto input_data = (const char **)input.data;
	VectorOperations::Exec(input, [&](size_t i, size_t k) {
		if (input.nullmask[i]) {
			return;
		}
		int64_t length = 0;
		for(size_t str_idx = 0; input_data[i][str_idx]; str_idx++) {
			length += (input_data[i][str_idx] & 0xC0) != 0x80;
		}
		result_data[i] = length;
	});
}

bool length_matches_arguments(vector<TypeId> &arguments) {
	return arguments.size() == 1 && arguments[0] == TypeId::VARCHAR;
}

TypeId length_get_return_type(vector<TypeId> &arguments) {
	return TypeId::BIGINT;
}

} // namespace function
} // namespace duckdb
