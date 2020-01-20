//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/unary_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <functional>

namespace duckdb {

struct UnaryOperatorWrapper {
	template<class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}
};

struct UnaryLambdaWrapper {
	template<class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input) {
		return fun(input);
	}
};

struct UnaryExecutor {
private:
	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteLoop(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, index_t count,
										sel_t *__restrict sel_vector, nullmask_t nullmask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (IGNORE_NULL && nullmask.any()) {
			VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
				if (!nullmask[i]) {
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[i]);
				}
			});
		} else {
			VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[i]);
			});
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
	static inline void ExecuteStandard(Vector &input, Vector &result, FUNC fun) {
		auto ldata = (INPUT_TYPE *)input.GetData();
		auto result_data = (RESULT_TYPE *)result.GetData();

		assert(input.vector_type == VectorType::FLAT_VECTOR || input.vector_type == VectorType::CONSTANT_VECTOR);
		ExecuteLoop<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(ldata, result_data, input.count, input.sel_vector, input.nullmask, fun);
		result.vector_type = input.vector_type;
		result.nullmask = input.nullmask;
		result.sel_vector = input.sel_vector;
		result.count = input.count;
	}
public:
	template <class INPUT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false>
	static void Execute(Vector &input, Vector &result) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, UnaryOperatorWrapper, OP, bool, IGNORE_NULL>(input, result, false);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, bool IGNORE_NULL = false, class FUNC=std::function<RESULT_TYPE(INPUT_TYPE)>>
	static void Execute(Vector &input, Vector &result, FUNC fun) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, UnaryLambdaWrapper, bool, FUNC, IGNORE_NULL>(input, result, fun);
	}
};

} // namespace duckdb
