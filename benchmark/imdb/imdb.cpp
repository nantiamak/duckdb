#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "imdb.hpp"

using namespace duckdb;
using namespace std;

#define IMDB_QUERY_BODY(QNR)                                                                                           \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
		imdb::dbgen(state->db);                                                                                        \
	}                                                                                                                  \
	virtual string GetQuery() {                                                                                        \
		return imdb::get_query(QNR);                                                                                   \
	}                                                                                                                  \
	virtual string VerifyResult(QueryResult *result) {                                                                 \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}          /* FIXME */                                                                                         \
		return ""; /*return compare_csv(*result, tpch::get_answer(SF, QNR),                                            \
		              true);  */                                                                                       \
	}                                                                                                                  \
	virtual string BenchmarkInfo() {                                                                                   \
		return StringUtil::Format("IMDB (JOB) Q%d: %s", QNR, imdb::get_query(QNR).c_str());                            \
	}

DUCKDB_BENCHMARK(IMDBQ001, "[imdb]")
IMDB_QUERY_BODY(1);
FINISH_BENCHMARK(IMDBQ001);

DUCKDB_BENCHMARK(IMDBQ002, "[imdb]")
IMDB_QUERY_BODY(2);
FINISH_BENCHMARK(IMDBQ002);

DUCKDB_BENCHMARK(IMDBQ003, "[imdb]")
IMDB_QUERY_BODY(3);
FINISH_BENCHMARK(IMDBQ003);

DUCKDB_BENCHMARK(IMDBQ004, "[imdb]")
IMDB_QUERY_BODY(4);
FINISH_BENCHMARK(IMDBQ004);

DUCKDB_BENCHMARK(IMDBQ005, "[imdb]")
IMDB_QUERY_BODY(5);
FINISH_BENCHMARK(IMDBQ005);

DUCKDB_BENCHMARK(IMDBQ006, "[imdb]")
IMDB_QUERY_BODY(6);
FINISH_BENCHMARK(IMDBQ006);

DUCKDB_BENCHMARK(IMDBQ007, "[imdb]")
IMDB_QUERY_BODY(7);
FINISH_BENCHMARK(IMDBQ007);

DUCKDB_BENCHMARK(IMDBQ008, "[imdb]")
IMDB_QUERY_BODY(8);
FINISH_BENCHMARK(IMDBQ008);

DUCKDB_BENCHMARK(IMDBQ009, "[imdb]")
IMDB_QUERY_BODY(9);
FINISH_BENCHMARK(IMDBQ009);

DUCKDB_BENCHMARK(IMDBQ010, "[imdb]")
IMDB_QUERY_BODY(10);
FINISH_BENCHMARK(IMDBQ010);

DUCKDB_BENCHMARK(IMDBQ011, "[imdb]")
IMDB_QUERY_BODY(11);
FINISH_BENCHMARK(IMDBQ011);

DUCKDB_BENCHMARK(IMDBQ012, "[imdb]")
IMDB_QUERY_BODY(12);
FINISH_BENCHMARK(IMDBQ012);

DUCKDB_BENCHMARK(IMDBQ013, "[imdb]")
IMDB_QUERY_BODY(13);
FINISH_BENCHMARK(IMDBQ013);

DUCKDB_BENCHMARK(IMDBQ014, "[imdb]")
IMDB_QUERY_BODY(14);
FINISH_BENCHMARK(IMDBQ014);

DUCKDB_BENCHMARK(IMDBQ015, "[imdb]")
IMDB_QUERY_BODY(15);
FINISH_BENCHMARK(IMDBQ015);

DUCKDB_BENCHMARK(IMDBQ016, "[imdb]")
IMDB_QUERY_BODY(16);
FINISH_BENCHMARK(IMDBQ016);

DUCKDB_BENCHMARK(IMDBQ017, "[imdb]")
IMDB_QUERY_BODY(17);
FINISH_BENCHMARK(IMDBQ017);

DUCKDB_BENCHMARK(IMDBQ018, "[imdb]")
IMDB_QUERY_BODY(18);
FINISH_BENCHMARK(IMDBQ018);

DUCKDB_BENCHMARK(IMDBQ019, "[imdb]")
IMDB_QUERY_BODY(19);
FINISH_BENCHMARK(IMDBQ019);

DUCKDB_BENCHMARK(IMDBQ020, "[imdb]")
IMDB_QUERY_BODY(20);
FINISH_BENCHMARK(IMDBQ020);

DUCKDB_BENCHMARK(IMDBQ021, "[imdb]")
IMDB_QUERY_BODY(21);
FINISH_BENCHMARK(IMDBQ021);

DUCKDB_BENCHMARK(IMDBQ022, "[imdb]")
IMDB_QUERY_BODY(22);
FINISH_BENCHMARK(IMDBQ022);

DUCKDB_BENCHMARK(IMDBQ023, "[imdb]")
IMDB_QUERY_BODY(23);
FINISH_BENCHMARK(IMDBQ023);

DUCKDB_BENCHMARK(IMDBQ024, "[imdb]")
IMDB_QUERY_BODY(24);
FINISH_BENCHMARK(IMDBQ024);

DUCKDB_BENCHMARK(IMDBQ025, "[imdb]")
IMDB_QUERY_BODY(25);
FINISH_BENCHMARK(IMDBQ025);

DUCKDB_BENCHMARK(IMDBQ026, "[imdb]")
IMDB_QUERY_BODY(26);
FINISH_BENCHMARK(IMDBQ026);

DUCKDB_BENCHMARK(IMDBQ027, "[imdb]")
IMDB_QUERY_BODY(27);
FINISH_BENCHMARK(IMDBQ027);

DUCKDB_BENCHMARK(IMDBQ028, "[imdb]")
IMDB_QUERY_BODY(28);
FINISH_BENCHMARK(IMDBQ028);

DUCKDB_BENCHMARK(IMDBQ029, "[imdb]")
IMDB_QUERY_BODY(29);
FINISH_BENCHMARK(IMDBQ029);

DUCKDB_BENCHMARK(IMDBQ030, "[imdb]")
IMDB_QUERY_BODY(30);
FINISH_BENCHMARK(IMDBQ030);

DUCKDB_BENCHMARK(IMDBQ031, "[imdb]")
IMDB_QUERY_BODY(31);
FINISH_BENCHMARK(IMDBQ031);

DUCKDB_BENCHMARK(IMDBQ032, "[imdb]")
IMDB_QUERY_BODY(32);
FINISH_BENCHMARK(IMDBQ032);

DUCKDB_BENCHMARK(IMDBQ033, "[imdb]")
IMDB_QUERY_BODY(33);
FINISH_BENCHMARK(IMDBQ033);

DUCKDB_BENCHMARK(IMDBQ034, "[imdb]")
IMDB_QUERY_BODY(34);
FINISH_BENCHMARK(IMDBQ034);

DUCKDB_BENCHMARK(IMDBQ035, "[imdb]")
IMDB_QUERY_BODY(35);
FINISH_BENCHMARK(IMDBQ035);

DUCKDB_BENCHMARK(IMDBQ036, "[imdb]")
IMDB_QUERY_BODY(36);
FINISH_BENCHMARK(IMDBQ036);

DUCKDB_BENCHMARK(IMDBQ037, "[imdb]")
IMDB_QUERY_BODY(37);
FINISH_BENCHMARK(IMDBQ037);

DUCKDB_BENCHMARK(IMDBQ038, "[imdb]")
IMDB_QUERY_BODY(38);
FINISH_BENCHMARK(IMDBQ038);

DUCKDB_BENCHMARK(IMDBQ039, "[imdb]")
IMDB_QUERY_BODY(39);
FINISH_BENCHMARK(IMDBQ039);

DUCKDB_BENCHMARK(IMDBQ040, "[imdb]")
IMDB_QUERY_BODY(40);
FINISH_BENCHMARK(IMDBQ040);

DUCKDB_BENCHMARK(IMDBQ041, "[imdb]")
IMDB_QUERY_BODY(41);
FINISH_BENCHMARK(IMDBQ041);

DUCKDB_BENCHMARK(IMDBQ042, "[imdb]")
IMDB_QUERY_BODY(42);
FINISH_BENCHMARK(IMDBQ042);

DUCKDB_BENCHMARK(IMDBQ043, "[imdb]")
IMDB_QUERY_BODY(43);
FINISH_BENCHMARK(IMDBQ043);

DUCKDB_BENCHMARK(IMDBQ044, "[imdb]")
IMDB_QUERY_BODY(44);
FINISH_BENCHMARK(IMDBQ044);

DUCKDB_BENCHMARK(IMDBQ045, "[imdb]")
IMDB_QUERY_BODY(45);
FINISH_BENCHMARK(IMDBQ045);

DUCKDB_BENCHMARK(IMDBQ046, "[imdb]")
IMDB_QUERY_BODY(46);
FINISH_BENCHMARK(IMDBQ046);

DUCKDB_BENCHMARK(IMDBQ047, "[imdb]")
IMDB_QUERY_BODY(47);
FINISH_BENCHMARK(IMDBQ047);

DUCKDB_BENCHMARK(IMDBQ048, "[imdb]")
IMDB_QUERY_BODY(48);
FINISH_BENCHMARK(IMDBQ048);

DUCKDB_BENCHMARK(IMDBQ049, "[imdb]")
IMDB_QUERY_BODY(49);
FINISH_BENCHMARK(IMDBQ049);

DUCKDB_BENCHMARK(IMDBQ050, "[imdb]")
IMDB_QUERY_BODY(50);
FINISH_BENCHMARK(IMDBQ050);

DUCKDB_BENCHMARK(IMDBQ051, "[imdb]")
IMDB_QUERY_BODY(51);
FINISH_BENCHMARK(IMDBQ051);

DUCKDB_BENCHMARK(IMDBQ052, "[imdb]")
IMDB_QUERY_BODY(52);
FINISH_BENCHMARK(IMDBQ052);

DUCKDB_BENCHMARK(IMDBQ053, "[imdb]")
IMDB_QUERY_BODY(53);
FINISH_BENCHMARK(IMDBQ053);

DUCKDB_BENCHMARK(IMDBQ054, "[imdb]")
IMDB_QUERY_BODY(54);
FINISH_BENCHMARK(IMDBQ054);

DUCKDB_BENCHMARK(IMDBQ055, "[imdb]")
IMDB_QUERY_BODY(55);
FINISH_BENCHMARK(IMDBQ055);

DUCKDB_BENCHMARK(IMDBQ056, "[imdb]")
IMDB_QUERY_BODY(56);
FINISH_BENCHMARK(IMDBQ056);

DUCKDB_BENCHMARK(IMDBQ057, "[imdb]")
IMDB_QUERY_BODY(57);
FINISH_BENCHMARK(IMDBQ057);

DUCKDB_BENCHMARK(IMDBQ058, "[imdb]")
IMDB_QUERY_BODY(58);
FINISH_BENCHMARK(IMDBQ058);

DUCKDB_BENCHMARK(IMDBQ059, "[imdb]")
IMDB_QUERY_BODY(59);
FINISH_BENCHMARK(IMDBQ059);

DUCKDB_BENCHMARK(IMDBQ060, "[imdb]")
IMDB_QUERY_BODY(60);
FINISH_BENCHMARK(IMDBQ060);

DUCKDB_BENCHMARK(IMDBQ061, "[imdb]")
IMDB_QUERY_BODY(61);
FINISH_BENCHMARK(IMDBQ061);

DUCKDB_BENCHMARK(IMDBQ062, "[imdb]")
IMDB_QUERY_BODY(62);
FINISH_BENCHMARK(IMDBQ062);

DUCKDB_BENCHMARK(IMDBQ063, "[imdb]")
IMDB_QUERY_BODY(63);
FINISH_BENCHMARK(IMDBQ063);

DUCKDB_BENCHMARK(IMDBQ064, "[imdb]")
IMDB_QUERY_BODY(64);
FINISH_BENCHMARK(IMDBQ064);

DUCKDB_BENCHMARK(IMDBQ065, "[imdb]")
IMDB_QUERY_BODY(65);
FINISH_BENCHMARK(IMDBQ065);

DUCKDB_BENCHMARK(IMDBQ066, "[imdb]")
IMDB_QUERY_BODY(66);
FINISH_BENCHMARK(IMDBQ066);

DUCKDB_BENCHMARK(IMDBQ067, "[imdb]")
IMDB_QUERY_BODY(67);
FINISH_BENCHMARK(IMDBQ067);

DUCKDB_BENCHMARK(IMDBQ068, "[imdb]")
IMDB_QUERY_BODY(68);
FINISH_BENCHMARK(IMDBQ068);

DUCKDB_BENCHMARK(IMDBQ069, "[imdb]")
IMDB_QUERY_BODY(69);
FINISH_BENCHMARK(IMDBQ069);

DUCKDB_BENCHMARK(IMDBQ070, "[imdb]")
IMDB_QUERY_BODY(70);
FINISH_BENCHMARK(IMDBQ070);

DUCKDB_BENCHMARK(IMDBQ071, "[imdb]")
IMDB_QUERY_BODY(71);
FINISH_BENCHMARK(IMDBQ071);

DUCKDB_BENCHMARK(IMDBQ072, "[imdb]")
IMDB_QUERY_BODY(72);
FINISH_BENCHMARK(IMDBQ072);

DUCKDB_BENCHMARK(IMDBQ073, "[imdb]")
IMDB_QUERY_BODY(73);
FINISH_BENCHMARK(IMDBQ073);

DUCKDB_BENCHMARK(IMDBQ074, "[imdb]")
IMDB_QUERY_BODY(74);
FINISH_BENCHMARK(IMDBQ074);

DUCKDB_BENCHMARK(IMDBQ075, "[imdb]")
IMDB_QUERY_BODY(75);
FINISH_BENCHMARK(IMDBQ075);

DUCKDB_BENCHMARK(IMDBQ076, "[imdb]")
IMDB_QUERY_BODY(76);
FINISH_BENCHMARK(IMDBQ076);

DUCKDB_BENCHMARK(IMDBQ077, "[imdb]")
IMDB_QUERY_BODY(77);
FINISH_BENCHMARK(IMDBQ077);

DUCKDB_BENCHMARK(IMDBQ078, "[imdb]")
IMDB_QUERY_BODY(78);
FINISH_BENCHMARK(IMDBQ078);

DUCKDB_BENCHMARK(IMDBQ079, "[imdb]")
IMDB_QUERY_BODY(79);
FINISH_BENCHMARK(IMDBQ079);

DUCKDB_BENCHMARK(IMDBQ080, "[imdb]")
IMDB_QUERY_BODY(80);
FINISH_BENCHMARK(IMDBQ080);

DUCKDB_BENCHMARK(IMDBQ081, "[imdb]")
IMDB_QUERY_BODY(81);
FINISH_BENCHMARK(IMDBQ081);

DUCKDB_BENCHMARK(IMDBQ082, "[imdb]")
IMDB_QUERY_BODY(82);
FINISH_BENCHMARK(IMDBQ082);

DUCKDB_BENCHMARK(IMDBQ083, "[imdb]")
IMDB_QUERY_BODY(83);
FINISH_BENCHMARK(IMDBQ083);

DUCKDB_BENCHMARK(IMDBQ084, "[imdb]")
IMDB_QUERY_BODY(84);
FINISH_BENCHMARK(IMDBQ084);

DUCKDB_BENCHMARK(IMDBQ085, "[imdb]")
IMDB_QUERY_BODY(85);
FINISH_BENCHMARK(IMDBQ085);

DUCKDB_BENCHMARK(IMDBQ086, "[imdb]")
IMDB_QUERY_BODY(86);
FINISH_BENCHMARK(IMDBQ086);

DUCKDB_BENCHMARK(IMDBQ087, "[imdb]")
IMDB_QUERY_BODY(87);
FINISH_BENCHMARK(IMDBQ087);

DUCKDB_BENCHMARK(IMDBQ088, "[imdb]")
IMDB_QUERY_BODY(88);
FINISH_BENCHMARK(IMDBQ088);

DUCKDB_BENCHMARK(IMDBQ089, "[imdb]")
IMDB_QUERY_BODY(89);
FINISH_BENCHMARK(IMDBQ089);

DUCKDB_BENCHMARK(IMDBQ090, "[imdb]")
IMDB_QUERY_BODY(90);
FINISH_BENCHMARK(IMDBQ090);

DUCKDB_BENCHMARK(IMDBQ091, "[imdb]")
IMDB_QUERY_BODY(91);
FINISH_BENCHMARK(IMDBQ091);

DUCKDB_BENCHMARK(IMDBQ092, "[imdb]")
IMDB_QUERY_BODY(92);
FINISH_BENCHMARK(IMDBQ092);

DUCKDB_BENCHMARK(IMDBQ093, "[imdb]")
IMDB_QUERY_BODY(93);
FINISH_BENCHMARK(IMDBQ093);

DUCKDB_BENCHMARK(IMDBQ094, "[imdb]")
IMDB_QUERY_BODY(94);
FINISH_BENCHMARK(IMDBQ094);

DUCKDB_BENCHMARK(IMDBQ095, "[imdb]")
IMDB_QUERY_BODY(95);
FINISH_BENCHMARK(IMDBQ095);

DUCKDB_BENCHMARK(IMDBQ096, "[imdb]")
IMDB_QUERY_BODY(96);
FINISH_BENCHMARK(IMDBQ096);

DUCKDB_BENCHMARK(IMDBQ097, "[imdb]")
IMDB_QUERY_BODY(97);
FINISH_BENCHMARK(IMDBQ097);

DUCKDB_BENCHMARK(IMDBQ098, "[imdb]")
IMDB_QUERY_BODY(98);
FINISH_BENCHMARK(IMDBQ098);

DUCKDB_BENCHMARK(IMDBQ099, "[imdb]")
IMDB_QUERY_BODY(99);
FINISH_BENCHMARK(IMDBQ099);

DUCKDB_BENCHMARK(IMDBQ100, "[imdb]")
IMDB_QUERY_BODY(100);
FINISH_BENCHMARK(IMDBQ100);

DUCKDB_BENCHMARK(IMDBQ101, "[imdb]")
IMDB_QUERY_BODY(101);
FINISH_BENCHMARK(IMDBQ101);

DUCKDB_BENCHMARK(IMDBQ102, "[imdb]")
IMDB_QUERY_BODY(102);
FINISH_BENCHMARK(IMDBQ102);

DUCKDB_BENCHMARK(IMDBQ103, "[imdb]")
IMDB_QUERY_BODY(103);
FINISH_BENCHMARK(IMDBQ103);

DUCKDB_BENCHMARK(IMDBQ104, "[imdb]")
IMDB_QUERY_BODY(104);
FINISH_BENCHMARK(IMDBQ104);

DUCKDB_BENCHMARK(IMDBQ105, "[imdb]")
IMDB_QUERY_BODY(105);
FINISH_BENCHMARK(IMDBQ105);

DUCKDB_BENCHMARK(IMDBQ106, "[imdb]")
IMDB_QUERY_BODY(106);
FINISH_BENCHMARK(IMDBQ106);

DUCKDB_BENCHMARK(IMDBQ107, "[imdb]")
IMDB_QUERY_BODY(107);
FINISH_BENCHMARK(IMDBQ107);

DUCKDB_BENCHMARK(IMDBQ108, "[imdb]")
IMDB_QUERY_BODY(108);
FINISH_BENCHMARK(IMDBQ108);

DUCKDB_BENCHMARK(IMDBQ109, "[imdb]")
IMDB_QUERY_BODY(109);
FINISH_BENCHMARK(IMDBQ109);

DUCKDB_BENCHMARK(IMDBQ110, "[imdb]")
IMDB_QUERY_BODY(110);
FINISH_BENCHMARK(IMDBQ110);

DUCKDB_BENCHMARK(IMDBQ111, "[imdb]")
IMDB_QUERY_BODY(111);
FINISH_BENCHMARK(IMDBQ111);

DUCKDB_BENCHMARK(IMDBQ112, "[imdb]")
IMDB_QUERY_BODY(112);
FINISH_BENCHMARK(IMDBQ112);

DUCKDB_BENCHMARK(IMDBQ113, "[imdb]")
IMDB_QUERY_BODY(113);
FINISH_BENCHMARK(IMDBQ113);

DUCKDB_BENCHMARK(IMDBQ114, "[imdb]")
IMDB_QUERY_BODY(114);
FINISH_BENCHMARK(IMDBQ114);
