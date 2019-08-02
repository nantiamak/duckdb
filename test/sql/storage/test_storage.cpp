#include "catch.hpp"
#include "common/file_system.hpp"
#include "test_helpers.hpp"
#include "main/appender.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test empty startup", "[storage]") {
	unique_ptr<DuckDB> db;
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	// create a database and close it
	REQUIRE_NOTHROW(db = make_unique<DuckDB>(storage_database));
	db.reset();
	// reload the database
	REQUIRE_NOTHROW(db = make_unique<DuckDB>(storage_database));
	db.reset();
	DeleteDatabase(storage_database);
}

TEST_CASE("Test empty table", "[storage]") {
	unique_ptr<QueryResult> result;

	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR);"));

		result = con.Query("SELECT COUNT(*) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT COUNT(*) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT COUNT(*) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
}

TEST_CASE("Test simple storage", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21), (NULL, NULL)"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (13), (12), (11)"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 22, 21, 22}));
		result = con.Query("SELECT * FROM test2 ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	}
	// reload the database from disk, we do this again because checkpointing at startup causes this to follow a
	// different code path
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 11, 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 22, 21, 22}));
		result = con.Query("SELECT * FROM test2 ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test storing NULLs and strings", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b STRING);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (NULL, 'hello'), "
		                          "(13, 'abcdefgh'), (12, NULL)"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {"hello", Value(), "abcdefgh"}));
	}
	// reload the database from disk, we do this again because checkpointing at startup causes this to follow a
	// different code path
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {"hello", Value(), "abcdefgh"}));
	}
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {"hello", Value(), "abcdefgh"}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test deletes with storage", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (12, 21), (13, 22), (12, 21)"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=12"));
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));
	}
	// reload the database from disk again
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {22, 22}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test updates with storage", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
		REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=b+1 WHERE a=11"));
		REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=b+1 WHERE a=11"));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {24, 21, 22}));
	}
	// reload the database from disk again
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {24, 21, 22}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test mix of updates and deletes with storage", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
		for (size_t i = 0; i < 1000; i++) {
			REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=b+1 WHERE a=11"));
		}
		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=12"));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {1022, 22}));
	}
	// reload the database from disk again
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT a, b FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {1022, 22}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test large inserts in a single transaction", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	int64_t expected_sum_a = 0, expected_sum_b = 0;
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		for (index_t i = 0; i < 1000; i++) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
			expected_sum_a += 11 + 13;
			expected_sum_b += 22 + 22;
		}
		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=12"));
		REQUIRE_NO_FAIL(con.Query("COMMIT"));

		result = con.Query("SELECT SUM(a), SUM(b) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum_a)}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(expected_sum_b)}));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT SUM(a), SUM(b) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum_a)}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(expected_sum_b)}));
	}
	// reload the database from disk again
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT SUM(a), SUM(b) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(expected_sum_a)}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(expected_sum_b)}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test interleaving of insertions/updates/deletes on multiple tables", "[storage][.]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2 (a INTEGER, b INTEGER);"));
		int32_t test_insert = 0, test_insert2 = 0;
		for (index_t i = 0; i < 1000; i++) {
			index_t stage = i % 7;
			switch (stage) {
			case 0:
				for (; test_insert < i; test_insert++) {
					REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(test_insert) + ")"));
				}
				break;
			case 1:
				for (; test_insert2 < i; test_insert2++) {
					REQUIRE_NO_FAIL(con.Query("INSERT INTO test2 VALUES (" + to_string(test_insert) + ", " +
					                          to_string(test_insert) + " + 2)"));
				}
				break;
			case 2:
				REQUIRE_NO_FAIL(con.Query("UPDATE test SET a = a + 1 WHERE a % 2 = 0"));
				break;
			case 3:
				REQUIRE_NO_FAIL(con.Query("UPDATE test2 SET a = a + 1 WHERE a % 2 = 0"));
				break;
			case 4:
				REQUIRE_NO_FAIL(con.Query("UPDATE test2 SET b = b + 1 WHERE b % 2 = 0"));
				break;
			case 5:
				REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a % 5 = 0"));
				break;
			default:
				REQUIRE_NO_FAIL(con.Query("DELETE FROM test2 WHERE a % 5 = 0"));
				break;
			}
		}
		REQUIRE_NO_FAIL(con.Query("COMMIT"));

		result = con.Query("SELECT SUM(a) FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {396008}));

		result = con.Query("SELECT SUM(a), SUM(b) FROM test2 ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {403915}));
		REQUIRE(CHECK_COLUMN(result, 1, {405513}));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT SUM(a) FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {396008}));

		result = con.Query("SELECT SUM(a), SUM(b) FROM test2 ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {403915}));
		REQUIRE(CHECK_COLUMN(result, 1, {405513}));
	}
	// reload the database from disk again
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT SUM(a) FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {396008}));

		result = con.Query("SELECT SUM(a), SUM(b) FROM test2 ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {403915}));
		REQUIRE(CHECK_COLUMN(result, 1, {405513}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test update/deletes on big table", "[storage][.]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a big table
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
		Appender appender(db, DEFAULT_SCHEMA, "test");
		for (int32_t i = 0; i < 100000; i++) {
			appender.BeginRow();
			appender.AppendInteger(i % 1000);
			appender.EndRow();
		}
		appender.Commit();
		// now perform some updates
		REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=2000 WHERE a=1"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=2 OR a=17"));

		result = con.Query("SELECT SUM(a), COUNT(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {50148000}));
		REQUIRE(CHECK_COLUMN(result, 1, {99800}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=0");
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=1");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=2");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=17");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT SUM(a), COUNT(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {50148000}));
		REQUIRE(CHECK_COLUMN(result, 1, {99800}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=0");
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=1");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=2");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=17");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
	// reload the database from disk again
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT SUM(a), COUNT(a) FROM test");
		REQUIRE(CHECK_COLUMN(result, 0, {50148000}));
		REQUIRE(CHECK_COLUMN(result, 1, {99800}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=0");
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=1");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=2");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		result = con.Query("SELECT COUNT(a) FROM test WHERE a=17");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test updates/deletes/insertions on persistent segments", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1, 3), (NULL, NULL)"));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (2, 2)"));
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 3, 2}));
	}
	// reload the database from disk, we do this again because checkpointing at startup causes this to follow a
	// different code path
	{
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3, 3)"));

		REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=4 WHERE a=1"));

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 4, 2, 3}));
	}
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 4, 2, 3}));

		REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=4, b=4 WHERE a=1"));

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 4}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4}));
	}
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 4}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4}));

		REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=5, a=6 WHERE a=4"));

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 6}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 5}));
	}
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 6}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 5}));

		REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=7 WHERE a=3"));

		result = con.Query("SELECT * FROM test ORDER BY a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 2, 3, 6}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 7, 5}));

	}
	DeleteDatabase(storage_database);
}