#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE nation(n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER, n_comment VARCHAR)");
	con.Query("CREATE TABLE customer(c_custkey INTEGER, c_name VARCHAR, c_address VARCHAR, c_nationkey INTEGER, c_phone VARCHAR, c_acctbal DOUBLE, c_mktsegment VARCHAR, c_comment VARCHAR)");
	con.Query("COPY customer FROM '../../duckdb_benchmark_data/tpch_customer.csv'");
	con.Query("COPY nation FROM '../../duckdb_benchmark_data/tpch_nation.csv'");

	auto result=con.Query("copy (select c_name, n_name from nation, customer where c_nationkey=n_nationkey) to '/Users/Nantia/Desktop/result.txt'");
	result->Print();
	//auto result = con.Query("select count(*) from customer");
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
