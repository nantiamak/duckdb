
#include "common/types/operators.hpp"
#include "common/types/date.hpp"

#include <cstdlib>

using namespace duckdb;
using namespace std;

namespace operators {

template <> uint64_t Abs::Operation(uint64_t left) {
	return left;
}

template <> double Modulo::Operation(double left, double right) {
	throw NotImplementedException("Modulo for double not implemented!");
}

template <> int8_t Cast::Operation(const char *left) {
	int64_t value = Cast::Operation<const char *, int64_t>(left);
	if (in_bounds<int8_t>(value))
		return (int8_t)value;
	throw std::out_of_range("Cannot convert to TINYINT");
}

template <> int16_t Cast::Operation(const char *left) {
	int64_t value = Cast::Operation<const char *, int64_t>(left);
	if (in_bounds<int16_t>(value))
		return (int16_t)value;
	throw std::out_of_range("Cannot convert to SMALLINT");
}

template <> int Cast::Operation(const char *left) {
	return stoi(left, NULL, 10);
}

template <> int64_t Cast::Operation(const char *left) {
	return stoll(left, NULL, 10);
}

template <> uint64_t Cast::Operation(const char *left) {
	return stoull(left, NULL, 10);
}

template <> double Cast::Operation(const char *left) {
	return stod(left, NULL);
}

// numeric -> string
template <> std::string Cast::Operation(int8_t left) {
	return to_string(left);
}

template <> std::string Cast::Operation(int16_t left) {
	return to_string(left);
}

template <> std::string Cast::Operation(int left) {
	return to_string(left);
}

template <> std::string Cast::Operation(int64_t left) {
	return to_string(left);
}

template <> std::string Cast::Operation(uint64_t left) {
	return to_string(left);
}

template <> std::string Cast::Operation(double left) {
	return to_string(left);
}

template <> std::string CastFromDate::Operation(duckdb::date_t left) {
	return Date::ToString(left);
}

template <> date_t CastToDate::Operation(const char *left) {
	return Date::FromString(left);
}

} // namespace operators