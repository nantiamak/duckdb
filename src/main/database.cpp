
#include "main/database.hpp"

using namespace duckdb;
using namespace std;

DuckDB::DuckDB(const char *path)
    : storage(*this, path ? std::string(path) : std::string()),
      catalog(storage), transaction_manager(storage) {
	// initialize the database
	storage.Initialize();
}
