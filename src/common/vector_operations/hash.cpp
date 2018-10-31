//===--------------------------------------------------------------------===//
// hash.cpp
// Description: This file contains the vectorized hash implementations
//===--------------------------------------------------------------------===//

#include "common/operator/hash_operators.hpp"
#include "common/vector_operations/binary_loops.hpp"
#include "common/vector_operations/unary_loops.hpp"

using namespace duckdb;
using namespace std;

void VectorOperations::Hash(Vector &input, Vector &result) {
	if (result.type != TypeId::POINTER) {
		throw InvalidTypeException(result.type,
		                           "result of hash must be a uint64_t");
	}
	switch (input.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		templated_unary_loop_process_null<int8_t, uint64_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::SMALLINT:
		templated_unary_loop_process_null<int16_t, uint64_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::DATE:
	case TypeId::INTEGER:
		templated_unary_loop_process_null<int32_t, uint64_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::TIMESTAMP:
	case TypeId::BIGINT:
		templated_unary_loop_process_null<int64_t, uint64_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::POINTER:
		templated_unary_loop_process_null<uint64_t, uint64_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::DECIMAL:
		templated_unary_loop_process_null<double, uint64_t, operators::Hash>(
		    input, result);
		break;
	case TypeId::VARCHAR:
		templated_unary_loop_process_null<const char *, uint64_t,
		                                  operators::Hash>(input, result);
		break;
	default:
		throw InvalidTypeException(input.type, "Invalid type for hash");
	}
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input) {
	if (hashes.type != TypeId::POINTER) {
		throw NotImplementedException(
		    "Hashes must be 64-bit unsigned integer hash vector");
	}
	// first hash the input to an intermediate vector
	Vector intermediate(TypeId::POINTER, true, false);
	VectorOperations::Hash(input, intermediate);
	// then XOR it together with the input
	VectorOperations::BitwiseXORInPlace(hashes, intermediate);
}