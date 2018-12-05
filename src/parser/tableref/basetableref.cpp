#include "parser/tableref/basetableref.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

void BaseTableRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);

	serializer.WriteString(schema_name);
	serializer.WriteString(table_name);
}

unique_ptr<TableRef> BaseTableRef::Deserialize(Deserializer &source) {
	auto result = make_unique<BaseTableRef>();

	result->schema_name = source.Read<string>();
	result->table_name = source.Read<string>();

	return result;
}

unique_ptr<TableRef> BaseTableRef::Copy() {
	auto copy = make_unique<BaseTableRef>();

	copy->schema_name = schema_name;
	copy->table_name = table_name;
	copy->alias = alias;

	return copy;
}
