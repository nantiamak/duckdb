#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)");
	con.Query("INSERT INTO integers VALUES (1, 2), (2, 3), (3, 4)");
	con.Query("CREATE TABLE integers2(k INTEGER, l INTEGER)");
	con.Query("INSERT INTO integers2 VALUES (1, 10), (2, 20)");

	auto result = con.Query("SELECT * FROM integers INNER JOIN integers2 ON "
	                   "integers.i=integers2.k");
	
	result->Print();
}
