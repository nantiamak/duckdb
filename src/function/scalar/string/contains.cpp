#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string.h>
#include <ctype.h>
#include <unordered_map>
#include <algorithm>    // std::max

using namespace std;

namespace duckdb {

static bool contains_instr(string_t haystack, string_t needle);

static bool contains_kmp(const string_t &str, const string_t &pattern);
static vector<uint32_t> BuildKPMTable(const string_t &pattern);

static bool contains_bm(const string_t &str, const string_t &pattern);
static unordered_map<char, uint32_t> BuildBMTable(const string_t &pattern);

struct ContainsOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return contains_instr(left, right);
	}
};

struct ContainsKMPOperator {
    template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
        return contains_kmp(left, right);
    }
};

struct ContainsBMOperator {
    template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
        return contains_bm(left, right);
    }
};

static bool contains_instr(string_t haystack, string_t needle) {
	// Getting information about the needle and the haystack
	auto input_haystack = haystack.GetData();
	auto input_needle = needle.GetData();
	auto size_haystack = haystack.GetSize();
	auto size_needle = needle.GetSize();

	// Needle needs something to proceed
	if (size_needle > 0) {
		// Haystack should be bigger or equal size to the needle
		while (size_haystack >= size_needle) {
			// Compare Needle to the Haystack
			if ((memcmp(input_haystack, input_needle, size_needle) == 0)) {
				return true;
			}
			size_haystack--;
			input_haystack++;
		}
	}
	return false;
}

static bool contains_kmp(const string_t &str, const string_t &pattern) {
    auto str_size = str.GetSize();
    auto patt_size = pattern.GetSize();
    if(patt_size > str_size)
        return 0;

    idx_t idx_patt = 0;
    idx_t idx_str = 0;
    auto kmp_table = BuildKPMTable(pattern);

    auto str_data = str.GetData();
    auto patt_data = pattern.GetData();

    while(idx_str < str_size) {
        if(str_data[idx_str] == patt_data[idx_patt]) {
            ++idx_str;
            ++idx_patt;
            if(idx_patt == patt_size)
                return true;
        } else {
            if(idx_patt > 0) {
                idx_patt = kmp_table[idx_patt - 1];
            } else {
                ++idx_str;
            }
        }
    }
    return false;
}

static vector<uint32_t> BuildKPMTable(const string_t &pattern) {
    auto patt_size = pattern.GetSize();
    auto patt_data = pattern.GetData();
    vector<uint32_t> table(patt_size);
    table[0] = 0;
    idx_t i = 1;
    idx_t j = 0;
    while(i < patt_size) {
        if(patt_data[j] == patt_data[i]) {
            ++j;
            table[i] = j;
            ++i;
        } else if(j > 0) {
            j = table[j-1];
        } else {
            table[i] = 0;
            ++i;
        }
    }
    return table;
}

static bool contains_bm(const string_t &str, const string_t &pattern) {
    auto str_size = str.GetSize();
    auto patt_size = pattern.GetSize();
    if(patt_size > str_size)
        return 0;

    auto bm_table = BuildBMTable(pattern);

    auto str_data = str.GetData();
    auto patt_data = pattern.GetData();
    idx_t skip;
    char patt_char, str_char;
	for(idx_t idx_str = 0; idx_str <= (str_size - patt_size); idx_str += skip) {
		skip = 0;
		for(int idx_patt = patt_size - 1; idx_patt >= 0; --idx_patt) {
			patt_char = patt_data[idx_patt];
			str_char = str_data[idx_str + idx_patt];
			if(patt_char != str_char) {
				if(bm_table.find(str_char) != bm_table.end()) {
					skip = bm_table[str_char];
				} else {
					skip = patt_size;
				}
				break;
			}
		}
		if(skip == 0)
			return true;
	}
	return false;
}

static unordered_map<char, uint32_t> BuildBMTable(const string_t &pattern) {
	unordered_map<char, uint32_t> table;
	auto patt_data = pattern.GetData();
	uint32_t size_patt = pattern.GetSize();
	char ch;
	for(uint32_t i = 0; i < size_patt; ++i) {
		ch = patt_data[i];
		table[ch] = std::max((uint32_t)1, size_patt - i - 1);
	}
	return table;
}

void ContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("contains_instr",                              // name of the function
	                               {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
								   SQLType::BOOLEAN,                      // return type
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, ContainsOperator, true>));

    set.AddFunction(ScalarFunction("contains_kmp",                              // name of the function
                                   {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
								   SQLType::BOOLEAN,                      // return type
                                   ScalarFunction::BinaryFunction<string_t, string_t, bool, ContainsKMPOperator, true>));

    set.AddFunction(ScalarFunction("contains_bm",                              // name of the function
                                   {SQLType::VARCHAR, SQLType::VARCHAR}, // argument list
								   SQLType::BOOLEAN,                      // return type
                                   ScalarFunction::BinaryFunction<string_t, string_t, bool, ContainsBMOperator, true>));

}

} // namespace duckdb
