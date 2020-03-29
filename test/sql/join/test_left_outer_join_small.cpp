#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test LEFT OUTER JOIN SMALL", "[join]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	//con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 2), (2, 3), (3, 4)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers2(k INTEGER, l INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers2 VALUES (1, 10), (2, 20)"));

	result = con.Query("SELECT * FROM integers INNER JOIN integers2 ON "
	                   "integers.i=integers2.k");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 3, {10, 20}));
}
