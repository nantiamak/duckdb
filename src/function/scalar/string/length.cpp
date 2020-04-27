#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "utf8proc.hpp"

using namespace std;

namespace duckdb {

// length returns the size in characters
struct StringLengthOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		for (idx_t i = 0; i < input_length; i++) {
			if (input_data[i] & 0x80) {
				int64_t length = 0;
				// non-ascii character: use grapheme iterator on remainder of string
				utf8proc_grapheme_callback(input_data, input_length, [&](size_t start, size_t end) {
					length++;
					return true;
				});
				return length;
			}
		}
		return input_length;
	}
};

// strlen returns the size in bytes
struct StrLenOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return input.GetSize();
	}
};

void LengthFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"length", "len"}, ScalarFunction({SQLType::VARCHAR}, SQLType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StringLengthOperator, true>));
	set.AddFunction(ScalarFunction("strlen", {SQLType::VARCHAR}, SQLType::BIGINT,
	                               ScalarFunction::UnaryFunction<string_t, int64_t, StrLenOperator, true>));
}

struct UnicodeOperator {
	template <class TA, class TR> static inline TR Operation(const TA &input) {
		const auto size = input.GetSize();
		//  Empty string => 0
		if (size == 0) {
			return 0;
		}
		//  Assume: valid UTF-8 string.
		const auto data = input.GetData();
		//  1-byte: 0xxxxxxx
		if ((data[0] & 0x80) == 0x00) {
			return TR(data[0]);
		}
		//  2-byte: 110xxxxx 10xxxxxx
		if ((data[0] & 0xE0) == 0xC0) {
			return (TR(data[0] & 0x1F) << 6) | (TR(data[1] & 0x3F) << 0);
		}
		//  3-byte: 1110xxxx 10xxxxxx 10xxxxxx
		if ((data[0] & 0xF0) == 0xE0) {
			return (TR(data[0] & 0x0F) << 12) | (TR(data[1] & 0x3F) << 6) | (TR(data[2] & 0x3F) << 0);
		}
		//  4-byte: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
		if ((data[0] & 0xF8) == 0xF0) {
			return (TR(data[0] & 0x07) << 18) | (TR(data[1] & 0x3F) << 12) | (TR(data[2] & 0x3F) << 6) |
			       (TR(data[3] & 0x3F) << 0);
		}
		return 0;
	}
};

void UnicodeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("unicode", {SQLType::VARCHAR}, SQLType::INTEGER,
	                               ScalarFunction::UnaryFunction<string_t, int32_t, UnicodeOperator, true>));
}

} // namespace duckdb
