
#include "planner/operator/logical_get.hpp"

using namespace duckdb;
using namespace std;

std::vector<string> LogicalGet::GetNames() {
    assert(table);
	vector<string> names;
	for (auto &column : table->columns) {
		names.push_back(column.name);
	}
	return names;
}
