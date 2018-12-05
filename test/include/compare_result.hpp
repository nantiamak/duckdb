#include "common/string_util.hpp"
#include "duckdb.hpp"

namespace duckdb {

bool CHECK_COLUMN(unique_ptr<duckdb::DuckDBResult> &result, size_t column_number, vector<duckdb::Value> values);

string compare_csv(duckdb::DuckDBResult &result, string csv, bool header = false);

bool parse_datachunk(string csv, DataChunk &result, bool has_header);

//! Compares the result of a pipe-delimited CSV with the given DataChunk
//! Returns true if they are equal, and stores an error_message otherwise
bool compare_result(string csv, ChunkCollection &collection, bool has_header, string &error_message);

} // namespace duckdb
