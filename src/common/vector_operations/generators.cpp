//===--------------------------------------------------------------------===//
// generators.cpp
// Description: This file contains the implementation of different generators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class T> void templated_generate_sequence(Vector &result, int64_t start, int64_t increment) {
	assert(TypeIsNumeric(result.type));
	if (start > numeric_limits<T>::max() || increment > numeric_limits<T>::max()) {
		throw Exception("Sequence start or increment out of type range");
	}
	result.vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData<T>(result);
	auto value = (T) start;
	for (idx_t i = 0; i < result.size(); i++) {
		result_data[i] = value;
		value += increment;
	}
}

void VectorOperations::GenerateSequence(Vector &result, int64_t start, int64_t increment) {
	if (!TypeIsNumeric(result.type)) {
		throw InvalidTypeException(result.type, "Can only generate sequences for numeric values!");
	}
	switch (result.type) {
	case TypeId::INT8:
		templated_generate_sequence<int8_t>(result, start, increment);
		break;
	case TypeId::INT16:
		templated_generate_sequence<int16_t>(result, start, increment);
		break;
	case TypeId::INT32:
		templated_generate_sequence<int32_t>(result, start, increment);
		break;
	case TypeId::INT64:
		templated_generate_sequence<int64_t>(result, start, increment);
		break;
	case TypeId::FLOAT:
		templated_generate_sequence<float>(result, start, increment);
		break;
	case TypeId::DOUBLE:
		templated_generate_sequence<double>(result, start, increment);
		break;
	default:
		throw NotImplementedException("Unimplemented type for generate sequence");
	}
}

template <class T> void templated_generate_sequence(Vector &result, SelectionVector &sel, int64_t start, int64_t increment) {
	assert(TypeIsNumeric(result.type));
	if (start > numeric_limits<T>::max() || increment > numeric_limits<T>::max()) {
		throw Exception("Sequence start or increment out of type range");
	}
	result.vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData<T>(result);
	auto value = (T) start;
	for (idx_t i = 0; i < result.size(); i++) {
		auto idx = sel.get_index(i);
		result_data[idx] = value + increment * idx;
	}
}

void VectorOperations::GenerateSequence(Vector &result, SelectionVector &sel, int64_t start, int64_t increment) {
	if (!TypeIsNumeric(result.type)) {
		throw InvalidTypeException(result.type, "Can only generate sequences for numeric values!");
	}
	switch (result.type) {
	case TypeId::INT8:
		templated_generate_sequence<int8_t>(result, sel, start, increment);
		break;
	case TypeId::INT16:
		templated_generate_sequence<int16_t>(result, sel, start, increment);
		break;
	case TypeId::INT32:
		templated_generate_sequence<int32_t>(result, sel, start, increment);
		break;
	case TypeId::INT64:
		templated_generate_sequence<int64_t>(result, sel, start, increment);
		break;
	case TypeId::FLOAT:
		templated_generate_sequence<float>(result, sel, start, increment);
		break;
	case TypeId::DOUBLE:
		templated_generate_sequence<double>(result, sel, start, increment);
		break;
	default:
		throw NotImplementedException("Unimplemented type for generate sequence");
	}
}