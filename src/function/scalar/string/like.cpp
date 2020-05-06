#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

static bool like_operator(const char *s, const char *pattern, const char *escape);

struct LikeEscapeOperator {
	template <class TA, class TB, class TC, class TR> static inline TR Operation(TA left, TB right, TC escape) {
		auto s = left.GetData();
		auto pattern = right.GetData();
		auto escape = right.GetData();
		return like_operator(s, pattern, escape);
	}
};

struct NotLikeEscapeOperator {
	template <class TA, class TB, class TC, class TR> static inline TR Operation(TA left, TB right, TC escape) {
		auto s = left.GetData();
		auto pattern = right.GetData();
		auto escape = right.GetData();
		return !like_operator(s, pattern, escape);
	}
};

struct LikeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		auto s = left.GetData();
		auto pattern = right.GetData();
		auto escape = nullptr;
		return like_operator(s, pattern, escape);
	}
};

struct NotLikeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		auto s = left.GetData();
		auto pattern = right.GetData();
		auto escape = nullptr;
		return !like_operator(s, pattern, escape);
	}
};

bool like_operator(const char *s, const char *pattern, const char *escape) {
	const char *t, *p;

	t = s;
	for (p = pattern; *p && *t; p++) {
		if (escape && *p == *escape) {
			p++;
			if (*p != *t) {
				return false;
			}
			t++;
		} else if (*p == '_') {
			t++;
		} else if (*p == '%') {
			p++;
			while (*p == '%') {
				p++;
			}
			if (*p == 0) {
				return true; /* tail is acceptable */
			}
			for (; *p && *t; t++) {
				if (like_operator(t, p, escape)) {
					return true;
				}
			}
			if (*p == 0 && *t == 0) {
				return true;
			}
			return false;
		} else if (*p == *t) {
			t++;
		} else {
			return false;
		}
	}
	if (*p == '%' && *(p + 1) == 0) {
		return true;
	}
	return *t == 0 && *p == 0;
}

void LikeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("~~", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN,
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, LikeOperator, true>));
	set.AddFunction(ScalarFunction("!~~", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN,
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, NotLikeOperator, true>));
}

void LikeEscapeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("like_escape", {SQLType::VARCHAR, SQLType::VARCHAR, SQLType::VARCHAR},
	                               SQLType::VARCHAR,
	                               ScalarFunction::TernaryFunction<string_t, string_t, string_t, bool, LikeEscapeOperator, true>));
	set.AddFunction(
	    ScalarFunction("!like_escape", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::VARCHAR,
	                   ScalarFunction::TernaryFunction<string_t, string_t, string_t, bool, NotLikeEscapeOperator, true>));
}

} // namespace duckdb
