#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Single PRIMARY KEY constraint", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));

	// insert two conflicting pairs at the same time
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (3, 5)"));

	// insert unique values
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 4), (2, 5)"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5}));

	// insert a duplicate value as part of a chain of values
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (6, 6), (3, 4);"));

	// now insert just the first value
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (6, 6);"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 2, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5, 6}));

	// insert NULL value in PRIMARY KEY is not allowed
	REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (NULL, 4);"));

	// update NULL is also not allowed
	REQUIRE_FAIL(con.Query("UPDATE integers SET i=NULL;"));

	// insert the same value from multiple connections
	// NOTE: this tests current behavior
	// this can potentially change in the future
	Connection con2(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// insert from first connection succeeds
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (7, 8);"));
	// insert from second connection also succeeds
	REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (7, 33);"));

	// now committing the first transaction works
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	// but the second transaction results in a conflict
	REQUIRE_FAIL(con2.Query("COMMIT"));
}

TEST_CASE("Multiple PRIMARY KEY constraint", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// FIXME: not supported by ART yet
	REQUIRE_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j VARCHAR, PRIMARY KEY(i, j))"));

	// REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j VARCHAR, PRIMARY KEY(i, j))"));

	// // insert unique values
	// REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3, 'hello'), (3, 'world')"));

	// result = con.Query("SELECT * FROM integers");
	// REQUIRE(CHECK_COLUMN(result, 0, {3, 3}));
	// REQUIRE(CHECK_COLUMN(result, 1, {"hello", "world"}));

	// // insert a duplicate value as part of a chain of values
	// REQUIRE_FAIL(con.Query("INSERT INTO integers VALUES (6, 'bla'), (3, 'hello');"));

	// // now insert just the first value
	// REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (6, 'bla');"));

	// result = con.Query("SELECT * FROM integers");
	// REQUIRE(CHECK_COLUMN(result, 0, {3, 3, 6}));
	// REQUIRE(CHECK_COLUMN(result, 1, {"hello", "world", "bla"}));
}

TEST_CASE("PRIMARY KEY and transactions", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// rollback
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1);"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1);"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("PRIMARY KEY and update/delete", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER PRIMARY KEY, b INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)"));
	// this update affects a non-primary key column, should just work
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET b=2 WHERE b=3;"));
	//! Set only the first key higher, should not work as this introduces a duplicate key!
	REQUIRE_FAIL(con.Query("UPDATE test SET a=a+1 WHERE b=1;"));
	//! Set all keys to 4, results in a conflict!
	REQUIRE_FAIL(con.Query("UPDATE test SET a=4;"));

	result = con.Query("SELECT * FROM test;");
	REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 2}));

	// delete and insert the same value should just work
	REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=11"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 1);"));

	// insert a duplicate should fail
	REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (11, 1);"));

	// update one key
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=4 WHERE b=1;"));

	result = con.Query("SELECT * FROM test ORDER BY a;");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 12, 13}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 2}));

	// set a column to NULL should fail
	REQUIRE_FAIL(con.Query("UPDATE test SET a=NULL WHERE b=1;"));
	REQUIRE_FAIL(con.Query("UPDATE test SET a=NULL;"));
}

TEST_CASE("PRIMARY KEY and update/delete on multiple columns", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// FIXME: not supported by ART yet
	REQUIRE_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, PRIMARY KEY(a, b));"));

	// // create a table
	// REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, PRIMARY KEY(a, b));"));
	// REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 'hello'), (12, "
	//                           "'world'), (13, 'blablabla')"));
	// // update one of the columns, should work as it does not introduce duplicates
	// REQUIRE_NO_FAIL(con.Query("UPDATE test SET b='hello';"));
	// //! Set every key one higher, should also work without conflicts
	// REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+1;"));
	// //! Set only the first key higher, should not work as this introduces a
	// //! duplicate key!
	// REQUIRE_FAIL(con.Query("UPDATE test SET a=a+1 WHERE a<=12;"));
	// //! Set all keys to 4, results in a conflict!
	// REQUIRE_FAIL(con.Query("UPDATE test SET a=4;"));

	// result = con.Query("SELECT * FROM test;");
	// REQUIRE(CHECK_COLUMN(result, 0, {12, 13, 14}));
	// REQUIRE(CHECK_COLUMN(result, 1, {Value("hello"), Value("hello"), Value("hello")}));

	// // delete and insert the same value should just work
	// REQUIRE_NO_FAIL(con.Query("DELETE FROM test WHERE a=12"));
	// REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (12, 'hello');"));

	// // insert a duplicate should fail
	// REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (12, 'hello');"));

	// // update one key
	// REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=4 WHERE a=12;"));

	// result = con.Query("SELECT * FROM test ORDER BY a;");
	// REQUIRE(CHECK_COLUMN(result, 0, {4, 13, 14}));
	// REQUIRE(CHECK_COLUMN(result, 1, {Value("hello"), Value("hello"), Value("hello")}));

	// // set a column to NULL should fail
	// REQUIRE_FAIL(con.Query("UPDATE test SET b=NULL WHERE a=13;"));
}

TEST_CASE("PRIMARY KEY and update/delete in the same transaction", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	// rollback
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1);"));
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=33;"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// insert the same values again
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (33);"));

	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 33}));

	// update and then insert
	REQUIRE_NO_FAIL(con.Query("DROP TABLE integers"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1);"));
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=33;"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1);"));

	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 33}));
}

TEST_CASE("Test appending the same value many times to a primary key column", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));
	// insert a bunch of values into the index and query the index
	for (int32_t val = 0; val < 100; val++) {
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {0}));

		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES ($1)", val));

		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	for (int32_t val = 0; val < 100; val++) {
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i + i = " + to_string(val) + "+" + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	// now insert the same values, this should fail this time
	for (int32_t it = 0; it < 10; it++) {
		for (int32_t val = 64; val < 65; val++) {
			result = con.Query("SELECT COUNT(*) FROM integers WHERE i + i = 64+" + to_string(val));
			REQUIRE(CHECK_COLUMN(result, 0, {1}));
			result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
			REQUIRE(CHECK_COLUMN(result, 0, {1}));
			result = con.Query("INSERT INTO integers VALUES ($1)", val);
			REQUIRE_FAIL(result);
		}
	}

	// now test that the counts are correct
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {100}));
	REQUIRE(CHECK_COLUMN(result, 1, {100}));
}

TEST_CASE("PRIMARY KEY and concurency conflicts", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));

	// con starts a transaction and modifies the second value
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=4 WHERE i=2"));

	// con2 can't update the second value
	REQUIRE_FAIL(con2.Query("UPDATE integers SET i=4 WHERE i=2"));
	REQUIRE_FAIL(con2.Query("UPDATE integers SET i=5 WHERE i=2"));
	// nor can it delete it
	REQUIRE_FAIL(con2.Query("DELETE FROM integers WHERE i=2"));

	// we tried to set i=5 in con2 but it failed, we can set it in con1 now though
	REQUIRE_NO_FAIL(con.Query("UPDATE integers SET i=5 WHERE i=3"));
	// rollback con1
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// now we can perform the changes in con2
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("UPDATE integers SET i=4 WHERE i=2"));
	REQUIRE_NO_FAIL(con2.Query("UPDATE integers SET i=5 WHERE i=3"));

	// check the results, con1 still gets the old results
	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	result = con2.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4, 5}));

	// now commit
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// check the results again, both get the same (new) results now
	result = con.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4, 5}));
	result = con2.Query("SELECT * FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4, 5}));
}
