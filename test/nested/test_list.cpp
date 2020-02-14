#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test filter and projection of nested lists", "[nested]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	unique_ptr<QueryResult> result;

	con.Query("CREATE TABLE list_data (g INTEGER, e INTEGER)");
	con.Query("INSERT INTO list_data VALUES (1, 1), (1, 2), (2, 3), (2, 4), (2, 5), (3, 6), (5, NULL)");

	result = con.Query("SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 (a)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)})}));

	result = con.Query("SELECT UNNEST(l1) FROM (SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 (a)) t1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	result = con.Query("SELECT * FROM (SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 (a)) t1, (SELECT LIST(b) l2 "
	                   "FROM (VALUES (4), (5), (6), (7)) AS t2 (b)) t2");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)})}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(4), Value::INTEGER(5), Value::INTEGER(6), Value::INTEGER(7)})}));

	result = con.Query("SELECT UNNEST(l1) u1, UNNEST(l2) u2 FROM (SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 "
	                   "(a)) t1, (SELECT LIST(b) l2 FROM (VALUES (4), (5), (6), (7)) AS t2 (b)) t2");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5, 6, 7}));

	result = con.Query("SELECT UNNEST(l1), l2 FROM (SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 (a)) t1, 	"
	                   "(SELECT LIST(b) l2 FROM (VALUES (4), (5), (6), (7)) AS t2 (b)) t2");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(4), Value::INTEGER(5), Value::INTEGER(6), Value::INTEGER(7)}),
	                      Value::LIST({Value::INTEGER(4), Value::INTEGER(5), Value::INTEGER(6), Value::INTEGER(7)}),
	                      Value::LIST({Value::INTEGER(4), Value::INTEGER(5), Value::INTEGER(6), Value::INTEGER(7)})}));

	result = con.Query("SELECT l1, UNNEST(l2) FROM (SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 (a)) t1, "
	                   "(SELECT LIST(b) l2 FROM (VALUES (4), (5), (6), (7)) AS t2 (b)) t2");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)})}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5, 6, 7}));

	result = con.Query("SELECT UNNEST(LIST(e)) ue, LIST(g) from list_data");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5),
	                      Value::INTEGER(6), Value()}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)})}));

	result = con.Query("SELECT g, LIST(e) from list_data GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2)}),
	                      Value::LIST({Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(6)}), Value::LIST({Value()})}));

	result = con.Query("SELECT g, LIST(e) l1, LIST(e) l2 from list_data GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2)}),
	                      Value::LIST({Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(6)}), Value::LIST({Value()})}));
	REQUIRE(CHECK_COLUMN(result, 2,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2)}),
	                      Value::LIST({Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(6)}), Value::LIST({Value()})}));

	result = con.Query("SELECT g, LIST(e/2.0) from list_data GROUP BY g order by g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::DOUBLE(0.5), Value::DOUBLE(1)}),
	                      Value::LIST({Value::DOUBLE(1.5), Value::DOUBLE(2), Value::DOUBLE(2.5)}),
	                      Value::LIST({Value::DOUBLE(3)}), Value::LIST({Value()})}));

	// TODO order
	result = con.Query("SELECT g, LIST(CAST(e AS VARCHAR)) from list_data GROUP BY g order by g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value("1"), Value("2")}), Value::LIST({Value("3"), Value("4"), Value("5")}),
	                      Value::LIST({Value("6")}), Value::LIST({Value()})}));

	result = con.Query("SELECT LIST(e) from list_data");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(4),
	                                   Value::INTEGER(5), Value::INTEGER(6), Value()})}));

	result = con.Query("SELECT UNNEST(LIST(e)) ue from list_data ORDER BY ue");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4, 5, 6}));

	result = con.Query("SELECT LIST(e), LIST(g) from list_data");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(4),
	                                   Value::INTEGER(5), Value::INTEGER(6), Value()})}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)})}));

	result = con.Query("SELECT LIST(42)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({Value::INTEGER(42)})}));

	result = con.Query("SELECT LIST(42) FROM list_data");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::LIST({Value::INTEGER(42), Value::INTEGER(42), Value::INTEGER(42), Value::INTEGER(42),
	                                   Value::INTEGER(42), Value::INTEGER(42), Value::INTEGER(42)})}));

	result = con.Query("SELECT UNNEST(LIST(42))");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// unlist is alias of unnest for symmetry reasons
	result = con.Query("SELECT UNLIST(LIST(42))");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("SELECT UNNEST(LIST(e)) ue, UNNEST(LIST(g)) ug from list_data ORDER BY ue");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4, 5, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 1, 1, 2, 2, 2, 3}));

	result = con.Query("SELECT g, UNNEST(LIST(e)) ue, UNNEST(LIST(e+1)) ue2 from list_data GROUP BY g ORDER BY ue");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 1, 1, 2, 2, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3, 4, 5, 6}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 2, 3, 4, 5, 6, 7}));

	result = con.Query("SELECT g, UNNEST(l) u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1 ORDER BY u");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 1, 1, 2, 2, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3, 4, 5, 6}));

	result = con.Query("SELECT g, UNNEST(l)+1 u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1 ORDER BY u");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 1, 1, 2, 2, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4, 5, 6, 7}));

	// omg omg, list of structs, structs of lists

	result =
	    con.Query("SELECT g, STRUCT_PACK(a := g, b := le) sl FROM (SELECT g, LIST(e) le from list_data GROUP BY g) "
	              "xx WHERE g < 3 ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(
	    result, 1,
	    {Value::STRUCT(
	         {make_pair("a", Value::INTEGER(1)), make_pair("b", Value::LIST({Value::INTEGER(1), Value::INTEGER(2)}))}),
	     Value::STRUCT({make_pair("a", Value::INTEGER(2)),
	                    make_pair("b", Value::LIST({Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5)}))})}));

	// FIXME append type

	//	result = con.Query("SELECT LIST(STRUCT_PACK(a := g2, b := le)) FROM (SELECT g % 2 g2, LIST(e) le from list_data
	// GROUP BY g) xx"); 	result->Print();
	//
	//
	//	result = con.Query("SELECT g2, LIST(le) FROM (SELECT g % 2 g2, LIST(e) le from list_data GROUP BY g) sq GROUP BY
	// g2"); 	result->Print();

	// FIXME
	//		result = con.Query("SELECT g, LIST(STRUCT_PACK(a := e, b := e+1)) from list_data GROUP BY g");
	//		result->Print();

	// you're holding it wrong
	REQUIRE_FAIL(con.Query("SELECT LIST(LIST(42))"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST(UNNEST(LIST(42))"));

	REQUIRE_FAIL(con.Query("SELECT LIST()"));
	REQUIRE_FAIL(con.Query("SELECT LIST() FROM list_data"));
	REQUIRE_FAIL(con.Query("SELECT LIST(e, g) FROM list_data"));

	REQUIRE_FAIL(con.Query("SELECT g, UNNEST(l+1) u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1"));
	REQUIRE_FAIL(con.Query("SELECT g, UNNEST(g) u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1"));
	REQUIRE_FAIL(con.Query("SELECT g, UNNEST() u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1"));

	REQUIRE_FAIL(con.Query("SELECT UNNEST(42)"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST()"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST(42) from list_data"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST() from list_data"));
	REQUIRE_FAIL(con.Query("SELECT g FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1 where UNNEST(l) > 42"));

	// TODO scalar list constructor (how about array[] ?)
	// TODO ?
	// pass binddata to callbacks, add cleanup function
	// TODO group by list/struct
	// TODO lists longer than standard_vector_size
}
