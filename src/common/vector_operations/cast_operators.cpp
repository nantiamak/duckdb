//===--------------------------------------------------------------------===//
// cast_operators.cpp
// Description: This file contains the implementation of the different casts
//===--------------------------------------------------------------------===//
#include "duckdb/common/operator/cast_operators.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <class SRC_TYPE, class DST_TYPE, class OP, bool IGNORE_NULL>
void templated_cast_loop(Vector &source, Vector &result) {
	auto ldata = (SRC_TYPE *)source.data;
	auto result_data = (DST_TYPE *)result.data;
	if (!IGNORE_NULL || !result.nullmask.any()) {
		VectorOperations::Exec(source, [&](index_t i, index_t k) {
			result_data[i] = OP::template Operation<SRC_TYPE, DST_TYPE>(ldata[i]);
		});
	} else {
		VectorOperations::Exec(source, [&](index_t i, index_t k) {
			if (!result.nullmask[i]) {
				result_data[i] = OP::template Operation<SRC_TYPE, DST_TYPE>(ldata[i]);
			}
		});
	}
}

template<class SRC, class OP>
static void string_cast(Vector &source, Vector &result) {
	assert(result.type == TypeId::VARCHAR);
	// result is VARCHAR
	// we have to place the resulting strings in the string heap
	auto ldata = (SRC *)source.data;
	auto result_data = (const char **)result.data;
	VectorOperations::Exec(source, [&](index_t i, index_t k) {
		if (source.nullmask[i]) {
			result_data[i] = nullptr;
		} else {
			auto str = OP::template Operation<SRC, string>(ldata[i]);
			result_data[i] = result.string_heap.AddString(str);
		}
	});
}

static NotImplementedException UnimplementedCast(SQLType source_type, SQLType target_type) {
	return NotImplementedException("Unimplemented type for cast (%s -> %s)", SQLTypeToString(source_type).c_str(), SQLTypeToString(target_type).c_str());
}

template <class SRC>
static void numeric_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(result.type == TypeId::BOOLEAN);
		templated_cast_loop<SRC, bool, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::TINYINT:
		assert(result.type == TypeId::TINYINT);
		templated_cast_loop<SRC, int8_t, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::SMALLINT:
		assert(result.type == TypeId::SMALLINT);
		templated_cast_loop<SRC, int16_t, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::INTEGER:
		assert(result.type == TypeId::INTEGER);
		templated_cast_loop<SRC, int32_t, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::BIGINT:
		assert(result.type == TypeId::BIGINT);
		templated_cast_loop<SRC, int64_t, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::FLOAT:
		assert(result.type == TypeId::FLOAT);
		templated_cast_loop<SRC, float, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
		assert(result.type == TypeId::DOUBLE);
		templated_cast_loop<SRC, double, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::VARCHAR: {
		string_cast<SRC, duckdb::Cast>(source, result);
		break;
	}
	default:
		throw UnimplementedCast(source_type, target_type);
	}
}

static void string_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(result.type == TypeId::BOOLEAN);
		templated_cast_loop<const char*, bool, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::TINYINT:
		assert(result.type == TypeId::TINYINT);
		templated_cast_loop<const char*, int8_t, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::SMALLINT:
		assert(result.type == TypeId::SMALLINT);
		templated_cast_loop<const char*, int16_t, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::INTEGER:
		assert(result.type == TypeId::INTEGER);
		templated_cast_loop<const char*, int32_t, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::BIGINT:
		assert(result.type == TypeId::BIGINT);
		templated_cast_loop<const char*, int64_t, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::FLOAT:
		assert(result.type == TypeId::FLOAT);
		templated_cast_loop<const char*, float, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
		assert(result.type == TypeId::DOUBLE);
		templated_cast_loop<const char*, double, duckdb::Cast, true>(source, result);
		break;
	case SQLTypeId::DATE:
		assert(result.type == TypeId::INTEGER);
		templated_cast_loop<const char*, date_t, duckdb::CastToDate, true>(source, result);
		break;
	case SQLTypeId::TIME:
		assert(result.type == TypeId::INTEGER);
		templated_cast_loop<const char*, dtime_t, duckdb::CastToTime, true>(source, result);
		break;
	case SQLTypeId::TIMESTAMP:
		assert(result.type == TypeId::BIGINT);
		templated_cast_loop<const char*, timestamp_t, duckdb::CastToTimestamp, true>(source, result);
		break;
	default:
		throw UnimplementedCast(source_type, target_type);
	}
}

static void date_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// date to varchar
		string_cast<date_t, duckdb::CastFromDate>(source, result);
		break;
	default:
		throw UnimplementedCast(source_type, target_type);
	}
}

static void time_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// time to varchar
		string_cast<dtime_t, duckdb::CastFromTime>(source, result);
		break;
	default:
		throw UnimplementedCast(source_type, target_type);
	}
}

static void timestamp_cast_switch(Vector &source, Vector &result, SQLType source_type, SQLType target_type) {
	// now switch on the result type
	switch (target_type.id) {
	case SQLTypeId::VARCHAR:
		// timestamp to varchar
		string_cast<timestamp_t, duckdb::CastFromTimestamp>(source, result);
		break;
	case SQLTypeId::DATE:
		// timestamp to date
		templated_cast_loop<timestamp_t, date_t, duckdb::CastTimestampToDate, true>(source, result);
		break;
	case SQLTypeId::TIME:
		// timestamp to time
		templated_cast_loop<timestamp_t, dtime_t, duckdb::CastTimestampToTime, true>(source, result);
		break;
	default:
		throw UnimplementedCast(source_type, target_type);
	}
}

void VectorOperations::Cast(Vector &source, Vector &result, SQLType source_type, SQLType target_type) {
	if (source_type == target_type) {
		throw NotImplementedException("Cast between equal types");
	}

	result.nullmask = source.nullmask;
	result.sel_vector = source.sel_vector;
	result.count = source.count;
	// first switch on source type
	switch (source_type.id) {
	case SQLTypeId::BOOLEAN:
		assert(source.type == TypeId::BOOLEAN);
		numeric_cast_switch<bool>(source, result, source_type, target_type);
		break;
	case SQLTypeId::TINYINT:
		assert(source.type == TypeId::TINYINT);
		numeric_cast_switch<int8_t>(source, result, source_type, target_type);
		break;
	case SQLTypeId::SMALLINT:
		assert(source.type == TypeId::SMALLINT);
		numeric_cast_switch<int16_t>(source, result, source_type, target_type);
		break;
	case SQLTypeId::INTEGER:
		assert(source.type == TypeId::INTEGER);
		numeric_cast_switch<int32_t>(source, result, source_type, target_type);
		break;
	case SQLTypeId::BIGINT:
		assert(source.type == TypeId::BIGINT);
		numeric_cast_switch<int64_t>(source, result, source_type, target_type);
		break;
	case SQLTypeId::FLOAT:
		assert(source.type == TypeId::FLOAT);
		numeric_cast_switch<float>(source, result, source_type, target_type);
		break;
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
		assert(source.type == TypeId::DOUBLE);
		numeric_cast_switch<double>(source, result, source_type, target_type);
		break;
	case SQLTypeId::DATE:
		assert(source.type == TypeId::INTEGER);
		date_cast_switch(source, result, source_type, target_type);
		break;
	case SQLTypeId::TIME:
		assert(source.type == TypeId::INTEGER);
		time_cast_switch(source, result, source_type, target_type);
		break;
	case SQLTypeId::TIMESTAMP:
		assert(source.type == TypeId::BIGINT);
		timestamp_cast_switch(source, result, source_type, target_type);
		break;
	case SQLTypeId::VARCHAR:
		assert(source.type == TypeId::VARCHAR);
		string_cast_switch(source, result, source_type, target_type);
		break;
	case SQLTypeId::SQLNULL:
		break;
	default:
		throw UnimplementedCast(source_type, target_type);
	}
}

void VectorOperations::Cast(Vector &source, Vector &result) {
	return VectorOperations::Cast(source, result, SQLTypeFromInternalType(source.type),
	                              SQLTypeFromInternalType(result.type));
}
