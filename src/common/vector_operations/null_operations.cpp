//===--------------------------------------------------------------------===//
// null_operators.cpp
// Description: This file contains the implementation of the
// IS NULL/NOT IS NULL operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <bool INVERSE> void is_null_loop(Vector &input, Vector &result, idx_t count) {
	assert(result.type == TypeId::BOOL);

	if (input.vector_type == VectorType::CONSTANT_VECTOR) {
		result.vector_type = VectorType::CONSTANT_VECTOR;
		auto result_data = ConstantVector::GetData<bool>(result);
		*result_data = INVERSE ? !ConstantVector::IsNull(input) : ConstantVector::IsNull(input);
	} else {
		VectorData data;
		input.Orrify(count, data);

		result.vector_type = VectorType::FLAT_VECTOR;
		auto result_data = FlatVector::GetData<bool>(result);
		for(idx_t i = 0; i < count; i++) {
			result_data[i] = INVERSE ? !(*data.nullmask)[data.sel->get_index(i)] : (*data.nullmask)[data.sel->get_index(i)];
		}
	}
}

void VectorOperations::IsNotNull(Vector &input, Vector &result, idx_t count) {
	is_null_loop<true>(input, result, count);
}

void VectorOperations::IsNull(Vector &input, Vector &result, idx_t count) {
	is_null_loop<false>(input, result, count);
}

bool VectorOperations::HasNotNull(Vector &input, idx_t count) {
	if (count == 0) {
		return false;
	}
	if (input.vector_type == VectorType::CONSTANT_VECTOR) {
		return !ConstantVector::IsNull(input);
	} else {
		VectorData data;
		input.Orrify(count, data);

		if (data.nullmask->none()) {
			return true;
		}
		for(idx_t i = 0; i < count; i++) {
			if (!(*data.nullmask)[i]) {
				return true;
			}
		}
		return false;
	}
}

bool VectorOperations::HasNull(Vector &input, idx_t count) {
	if (count == 0) {
		return false;
	}
	return !VectorOperations::HasNotNull(input, count);
}

// idx_t VectorOperations::NotNullSelVector(Vector &vector, SelectionVector &not_null, SelectionVector &null) {
// 	throw NotImplementedException("FIXME");
// 	// vector.Normalify();
// 	// if (vector.nullmask.any()) {
// 	// 	idx_t not_null_count = 0, null_count = 0;
// 	// 	VectorOperations::Exec(vector, [&](uint64_t i, uint64_t k) {
// 	// 		if (!vector.nullmask[i]) {
// 	// 			not_null_vector->set_index([result_count++] = i;
// 	// 		} else if (null_vector) {
// 	// 			null_vector[null_count++] = i;
// 	// 		}
// 	// 	});
// 	// 	result_assignment = not_null_vector;
// 	// 	return result_count;
// 	// } else {
// 	// 	return vector.size();
// 	// }
// }
