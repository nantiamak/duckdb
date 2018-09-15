
#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test insert into statements", "[simpleinserts]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// big insert
	con.Query("CREATE TABLE integers(i INTEGER)");
	result = con.Query(
	    "INSERT INTO integers VALUES (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), "
	    "(8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), "
	    "(2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), "
	    "(6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), "
	    "(0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), "
	    "(4), (5), (6), (7), (8), (9)");
	CHECK_COLUMN(result, 0, {1050});

	result = con.Query("SELECT COUNT(*) FROM integers");
	CHECK_COLUMN(result, 0, {1050});

	// insert into from SELECT
	result = con.Query("INSERT INTO integers SELECT * FROM integers;");
	CHECK_COLUMN(result, 0, {1050});

	result = con.Query("SELECT COUNT(*) FROM integers");
	CHECK_COLUMN(result, 0, {2100});

	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers;"));

	// insert into from query with column predicates
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER);"));

	result = con.Query("INSERT INTO integers VALUES (3, 4), (4, 3);");
	CHECK_COLUMN(result, 0, {2});

	result = con.Query("INSERT INTO integers (i) SELECT j FROM integers;");
	CHECK_COLUMN(result, 0, {2});

	result = con.Query("SELECT * FROM integers");
	CHECK_COLUMN(result, 0, {3, 4, 4, 3});
	CHECK_COLUMN(result, 1, {4, 3, Value(), Value()});
}
