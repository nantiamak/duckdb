//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/binary_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <functional>

namespace duckdb {

struct DefaultNullCheckOperator {
	template<class LEFT_TYPE, class RIGHT_TYPE>
	static inline bool Operation(LEFT_TYPE left, RIGHT_TYPE right) {
		return false;
	}
};

struct BinaryStandardOperatorWrapper {
	template<class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right) {
		return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
	}
};

struct BinarySingleArgumentOperatorWrapper {
	template<class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right) {
		return OP::template Operation<LEFT_TYPE>(left, right);
	}
};

struct BinaryLambdaWrapper {
	template<class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right) {
		return fun(left, right);
	}
};

struct BinaryExecutor {
private:
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, class NULL_CHECK>
	static void ExecuteLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
											RESULT_TYPE *__restrict result_data, index_t count,
											sel_t *__restrict sel_vector, nullmask_t &nullmask, FUNC fun) {
		if (!LEFT_CONSTANT) {
			ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
		}
		if (!RIGHT_CONSTANT) {
			ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
		}
		if (IGNORE_NULL && nullmask.any()) {
			VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
				auto lentry = ldata[LEFT_CONSTANT ? 0 : i];
				auto rentry = rdata[RIGHT_CONSTANT ? 0 : i];
				if (!nullmask[i]) {
					if (NULL_CHECK::Operation(lentry, rentry)) {
						nullmask[i] = true;
					} else {
						result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(fun, lentry, rentry);
					}
				}
			});
		} else {
			VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
				auto lentry = ldata[LEFT_CONSTANT ? 0 : i];
				auto rentry = rdata[RIGHT_CONSTANT ? 0 : i];
				if (NULL_CHECK::Operation(lentry, rentry)) {
					nullmask[i] = true;
				} else {
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(fun, lentry, rentry);
				}
			});
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL, class NULL_CHECK>
	static void ExecuteConstantConstant(Vector &left, Vector &right, Vector &result, LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata, RESULT_TYPE *__restrict result_data, FUNC fun) {
		// both left and right are constant: result is constant vector
		result.vector_type = VectorType::CONSTANT_VECTOR;
		result.sel_vector = left.sel_vector;
		result.count = left.count;
		result.nullmask[0] = left.nullmask[0] || right.nullmask[0];
		if (NULL_CHECK::Operation(ldata[0], rdata[0])) {
			result.nullmask[0] = true;
			return;
		}
		if (IGNORE_NULL && result.nullmask[0]) {
			return;
		}
		result_data[0] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(fun, ldata[0], rdata[0]);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL, class NULL_CHECK>
	static void ExecuteConstantFlat(Vector &left, Vector &right, Vector &result, LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata, RESULT_TYPE *__restrict result_data, FUNC fun) {
		// left side is constant: result is flat vector
		result.vector_type = VectorType::FLAT_VECTOR;
		result.sel_vector = right.sel_vector;
		result.count = right.count;
		if (left.nullmask[0]) {
			// left side is constant NULL, set everything to NULL
			result.nullmask.set();
			return;
		}
		result.nullmask = right.nullmask;
		ExecuteLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, true, false, NULL_CHECK>(
			ldata, rdata, result_data, result.count, result.sel_vector, result.nullmask, fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL, class NULL_CHECK>
	static void ExecuteFlatConstant(Vector &left, Vector &right, Vector &result, LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata, RESULT_TYPE *__restrict result_data, FUNC fun) {
		// right side is constant: result is flat vector
		result.vector_type = VectorType::FLAT_VECTOR;
		result.sel_vector = left.sel_vector;
		result.count = left.count;
		if (right.nullmask[0]) {
			result.nullmask.set();
			return;
		}
		result.nullmask = left.nullmask;
		ExecuteLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, false, true, NULL_CHECK>(
			ldata, rdata, result_data, result.count, result.sel_vector, result.nullmask, fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL, class NULL_CHECK>
	static void ExecuteStandard(Vector &left, Vector &right, Vector &result, LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata, RESULT_TYPE *__restrict result_data, FUNC fun) {
		// neither side is a constant: loop over everything
		assert(left.count == right.count);
		assert(left.sel_vector == right.sel_vector);
		assert(left.vector_type == VectorType::FLAT_VECTOR && right.vector_type == VectorType::FLAT_VECTOR);
		// OR nullmasks together
		result.vector_type = VectorType::FLAT_VECTOR;
		result.sel_vector = left.sel_vector;
		result.count = left.count;
		result.nullmask = left.nullmask | right.nullmask;
		ExecuteLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, false, false, NULL_CHECK>(
			ldata, rdata, result_data, result.count, result.sel_vector, result.nullmask, fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL, class NULL_CHECK>
	static void ExecuteSwitch(Vector &left, Vector &right, Vector &result, FUNC fun) {		auto ldata = (LEFT_TYPE *)left.GetData();
		auto rdata = (RIGHT_TYPE *)right.GetData();
		auto result_data = (RESULT_TYPE *)result.GetData();

		if (left.vector_type == VectorType::CONSTANT_VECTOR && right.vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteConstantConstant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, NULL_CHECK>(left, right, result, ldata, rdata, result_data, fun);
		} else if (left.vector_type == VectorType::CONSTANT_VECTOR && right.vector_type == VectorType::FLAT_VECTOR) {
			ExecuteConstantFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, NULL_CHECK>(left, right, result, ldata, rdata, result_data, fun);
		} else if (left.vector_type == VectorType::FLAT_VECTOR && right.vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteFlatConstant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, NULL_CHECK>(left, right, result, ldata, rdata, result_data, fun);
		} else {
			ExecuteStandard<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, NULL_CHECK>(left, right, result, ldata, rdata, result_data, fun);
		}
	}
public:
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, bool IGNORE_NULL = false, class NULL_CHECK=DefaultNullCheckOperator, class FUNC=std::function<RESULT_TYPE(LEFT_TYPE, RIGHT_TYPE)>>
	static void Execute(Vector &left, Vector &right, Vector &result, FUNC fun) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryLambdaWrapper, bool, FUNC, IGNORE_NULL, NULL_CHECK>(left, right, result, fun);
	}

	template<class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false, class NULL_CHECK=DefaultNullCheckOperator>
	static void Execute(Vector &left, Vector &right, Vector &result) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinarySingleArgumentOperatorWrapper, OP, bool, IGNORE_NULL, NULL_CHECK>(left, right, result, false);
	}

	template<class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false, class NULL_CHECK=DefaultNullCheckOperator>
	static void ExecuteStandard(Vector &left, Vector &right, Vector &result) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryStandardOperatorWrapper, OP, bool, IGNORE_NULL, NULL_CHECK>(left, right, result, false);
	}

private:
	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static inline index_t SelectLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
											sel_t *__restrict result, index_t count, sel_t *__restrict sel_vector,
											nullmask_t &nullmask) {
		index_t result_count = 0;
		if (nullmask.any()) {
			VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
				if (!nullmask[i] && OP::Operation(ldata[LEFT_CONSTANT ? 0 : i], rdata[RIGHT_CONSTANT ? 0 : i])) {
					result[result_count++] = i;
				}
			});
		} else {
			VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
				if (OP::Operation(ldata[LEFT_CONSTANT ? 0 : i], rdata[RIGHT_CONSTANT ? 0 : i])) {
					result[result_count++] = i;
				}
			});
		}
		return result_count;
	}
public:
	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static index_t Select(Vector &left, Vector &right, sel_t result[]) {
		auto ldata = (LEFT_TYPE *)left.GetData();
		auto rdata = (RIGHT_TYPE *)right.GetData();

		if (left.vector_type == VectorType::CONSTANT_VECTOR && right.vector_type == VectorType::CONSTANT_VECTOR) {
			// both sides are constant, return either 0 or the count
			// in this case we do not fill in the result selection vector at all
			if (left.nullmask[0] || right.nullmask[0] || !OP::Operation(ldata[0], rdata[0])) {
				return 0;
			} else {
				return left.count;
			}
		} else if (left.vector_type == VectorType::CONSTANT_VECTOR && right.vector_type == VectorType::FLAT_VECTOR) {
			if (left.nullmask[0]) {
				// left side is constant NULL; no results
				return 0;
			}
			// left side is normal constant, use right nullmask and do computation
			return SelectLoop<LEFT_TYPE, RIGHT_TYPE, OP, true, false>(ldata, rdata, result, right.count,
																			right.sel_vector, right.nullmask);
		} else if (left.vector_type == VectorType::FLAT_VECTOR && right.vector_type == VectorType::CONSTANT_VECTOR) {
			if (right.nullmask[0]) {
				// right side is constant NULL, no results
				return 0;
			}
			return SelectLoop<LEFT_TYPE, RIGHT_TYPE, OP, false, true>(ldata, rdata, result, left.count,
																			left.sel_vector, left.nullmask);
		} else {
			assert(left.count == right.count);
			assert(left.sel_vector == right.sel_vector);
			assert(left.vector_type == VectorType::FLAT_VECTOR && right.vector_type == VectorType::FLAT_VECTOR);
			// OR nullmasks together
			auto nullmask = left.nullmask | right.nullmask;
			return SelectLoop<LEFT_TYPE, RIGHT_TYPE, OP, false, false>(ldata, rdata, result, left.count,
																			left.sel_vector, nullmask);
		}
	}
};

} // namespace duckdb