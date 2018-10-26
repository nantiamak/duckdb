//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/vector_operations/binary_loops.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/types/vector.hpp"

namespace duckdb {

inline void BINARY_TYPE_CHECK(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw TypeMismatchException(left.type, right.type,
		                            "left and right types must be the same");
	}
	if (left.type != result.type) {
		throw TypeMismatchException(
		    left.type, result.type,
		    "result type must be the same as input types");
	}
	if (!left.IsConstant() && !right.IsConstant() &&
	    left.count != right.count) {
		throw Exception("Cardinality exception: left and right cannot have "
		                "different cardinalities");
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static inline void
binary_loop_function_left_constant(LEFT_TYPE ldata,
                                   RIGHT_TYPE *__restrict rdata,
                                   RESULT_TYPE *__restrict result_data,
                                   size_t count, sel_t *__restrict sel_vector) {
	ASSERT_RESTRICT(rdata, rdata + count, result_data, result_data + count);
	if (sel_vector) {
		for (size_t i = 0; i < count; i++) {
			result_data[sel_vector[i]] =
			    OP::Operation(ldata, rdata[sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < count; i++) {
			result_data[i] = OP::Operation(ldata, rdata[i]);
		}
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static inline void binary_loop_function_right_constant(
    LEFT_TYPE *__restrict ldata, RIGHT_TYPE rdata,
    RESULT_TYPE *__restrict result_data, size_t count,
    sel_t *__restrict sel_vector) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	if (sel_vector) {
		for (size_t i = 0; i < count; i++) {
			result_data[sel_vector[i]] =
			    OP::Operation(ldata[sel_vector[i]], rdata);
		}
	} else {
		for (size_t i = 0; i < count; i++) {
			result_data[i] = OP::Operation(ldata[i], rdata);
		}
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static inline void
binary_loop_function_array(LEFT_TYPE *__restrict ldata,
                           RIGHT_TYPE *__restrict rdata,
                           RESULT_TYPE *__restrict result_data, size_t count,
                           sel_t *__restrict sel_vector) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	ASSERT_RESTRICT(rdata, rdata + count, result_data, result_data + count);
	if (sel_vector) {
		for (size_t i = 0; i < count; i++) {
			result_data[sel_vector[i]] =
			    OP::Operation(ldata[sel_vector[i]], rdata[sel_vector[i]]);
		}
	} else {
		for (size_t i = 0; i < count; i++) {
			result_data[i] = OP::Operation(ldata[i], rdata[i]);
		}
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
void templated_binary_loop(Vector &left, Vector &right, Vector &result) {
	auto ldata = (LEFT_TYPE *)left.data;
	auto rdata = (RIGHT_TYPE *)right.data;
	auto result_data = (RESULT_TYPE *)result.data;

	if (left.IsConstant()) {
		if (left.nullmask[0]) {
			// left side is constant NULL, set everything to NULL
			result.nullmask.set();
		} else {
			// left side is normal constant, use right nullmask and do
			// computation
			LEFT_TYPE constant = ldata[0];
			result.nullmask = right.nullmask;
			binary_loop_function_left_constant<LEFT_TYPE, RIGHT_TYPE,
			                                   RESULT_TYPE, OP>(
			    constant, rdata, result_data, right.count, right.sel_vector);
		}
		result.sel_vector = right.sel_vector;
		result.count = right.count;
	} else if (right.IsConstant()) {
		if (right.nullmask[0]) {
			// right side is constant NULL, set everything to NULL
			result.nullmask.set();
		} else {
			// right side is normal constant, use left nullmask and do
			// computation
			RIGHT_TYPE constant = rdata[0];
			result.nullmask = left.nullmask;
			binary_loop_function_right_constant<LEFT_TYPE, RIGHT_TYPE,
			                                    RESULT_TYPE, OP>(
			    ldata, constant, result_data, left.count, left.sel_vector);
		}
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	} else {
		assert(left.count == right.count);
		// OR nullmasks together
		result.nullmask = left.nullmask | right.nullmask;
		assert(left.sel_vector == right.sel_vector);
		binary_loop_function_array<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(
		    ldata, rdata, result_data, left.count, left.sel_vector);
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	}
}

} // namespace duckdb
