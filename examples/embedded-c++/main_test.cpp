#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE nation(n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER, n_comment VARCHAR)");
	con.Query("CREATE TABLE customer(c_custkey INTEGER, c_name VARCHAR, c_address VARCHAR, c_nationkey INTEGER, c_phone VARCHAR, c_acctbal DOUBLE, c_mktsegment VARCHAR, c_comment VARCHAR)");
	//con.Query("CREATE TABLE orders(o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus VARCHAR, o_totalprice DOUBLE, o_orderdate DATE, o_orderpriority VARCHAR, o_clerk VARCHAR, o_shippriority INTEGER, o_comment VARCHAR)");
	//con.Query("COPY orders FROM '../../duckdb_benchmark_data/tpch_orders.csv'");
	con.Query("COPY customer FROM '../../duckdb_benchmark_data/tpch_customer.csv'");
	con.Query("COPY nation FROM '../../duckdb_benchmark_data/tpch_nation.csv'");
	//con.Query("CREATE TABLE lineitem(l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, l_quantity INTEGER, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE, l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR)");
	//con.Query("COPY lineitem FROM '../../duckdb_benchmark_data/tpch_lineitem.csv'");
	con.Query("CREATE TABLE supplier(s_suppkey INTEGER, s_name VARCHAR, s_address VARCHAR, s_nationkey INTEGER, s_phone VARCHAR, s_acctbal DOUBLE, s_comment VARCHAR)");
	con.Query("COPY supplier FROM '../../duckdb_benchmark_data/tpch_supplier.csv'");
	con.Query("CREATE TABLE partsupp(ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty INTEGER, ps_supplycost DOUBLE, ps_comment VARCHAR)");
	con.Query("COPY partsupp FROM '../../duckdb_benchmark_data/tpch_partsupp.csv'");

	//Buggy query - segmentation fault inside NextInnerJoin
	auto result=con.Query("copy (select ps_partkey, s_name from partsupp, supplier where ps_suppkey=s_suppkey order by ps_partkey) to '/Users/Nantia/Desktop/result.txt'");
	result->Print();

	//auto result=con.Query("copy (select ps_partkey, ps_supplycost from partsupp, supplier where ps_suppkey=s_suppkey order by ps_partkey) to '/Users/Nantia/Desktop/result_hash_join.txt'");
	//result->Print();

	//Successful query
	//auto result=con.Query("copy (select c_name, n_name from customer, nation where c_nationkey=n_nationkey) to '/Users/Nantia/Desktop/result.txt'");
	//result->Print();
	//auto result = con.Query("select count(*) from supplier");
	//result->Print();
	//auto result = con.Query("copy (select distinct(ps_partkey) from partsupp order by ps_partkey) to '/Users/Nantia/Desktop/result_hash_join.txt'");
	//result->Print();


	/*con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)");
	con.Query("INSERT INTO integers VALUES (1, 2), (2, 3), (3, 4), (5,6)");
	con.Query("CREATE TABLE integers2(k INTEGER, l INTEGER)");
	con.Query("INSERT INTO integers2 VALUES (1, 10), (2, 20), (3,30)");

	auto result = con.Query("SELECT * FROM integers INNER JOIN integers2 ON "
	                   "integers.i=integers2.k and l<30");

	result->Print();*/

	/*auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON "
	                   "integers.i=integers2.k ORDER BY i");

	result->Print();*/

	// WHERE happens AFTER the join, thus [where k IS NOT NULL] filters out any tuples with generated NULL values from
	// the LEFT OUTER JOIN. Because of this, this join is equivalent to an inner join.

	//Segmentation fault
/*	auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON "
										 "integers.i=integers2.k WHERE k IS NOT NULL ORDER BY i");
	result->Print();*/

//Segmentation fault
/*	auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON "
	                   "integers.i=integers2.k AND integers2.k IS NOT NULL ORDER BY i");

	result->Print();*/

//Does not call hash join
/*	auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON i=1 ORDER BY i, k;");
	result->Print();*/

//Does not call hash join
/*	auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON 1=1 ORDER BY i, k;");
	result->Print();*/

//Does not call hash join
/*	auto result = con.Query(
	    "SELECT * FROM integers LEFT OUTER JOIN (SELECT * FROM integers2 WHERE 1<>1) tbl2 ON 1=2 ORDER BY i;");
	result->Print();*/

//Does not call hash join
/*	auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON i=1 ORDER BY i, k;");
	result->Print();*/

//Does not call hash join
	/*auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON l=20 ORDER BY i, k;");
	result->Print();*/

//Does not call hash join
	/*auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON l>0 ORDER BY i, k;");
	result->Print();*/

//Does not call hash join
/*	auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON i=1 OR l=20 ORDER BY i, k;");
	result->Print();*/

//Does not call hash join
/*	auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON i=4 OR l=17 ORDER BY i;");
	result->Print();*/

//Does not call hash join
/*auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON i+l=21 ORDER BY i;");
result->Print();*/

//Does not call hash join
/*auto result = con.Query("SELECT * FROM integers LEFT OUTER JOIN integers2 ON i+l>12 ORDER BY i, k;");
result->Print();*/
}
