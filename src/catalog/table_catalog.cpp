
#include "catalog/table_catalog.hpp"
#include "catalog/catalog.hpp"
#include "common/exception.hpp"

#include "storage/storage_manager.hpp"

using namespace duckdb;
using namespace std;

TableCatalogEntry::TableCatalogEntry(
    Catalog *catalog, SchemaCatalogEntry *schema, string name,
    const std::vector<ColumnDefinition> &table_columns,
    std::vector<std::unique_ptr<Constraint>> &constraints)
    : AbstractCatalogEntry(CatalogType::TABLE, catalog, name), schema(schema),
      constraints(move(constraints)) {
	for (auto entry : table_columns) {
		if (ColumnExists(entry.name)) {
			throw CatalogException("Column with name %s already exists!",
			                       entry.name.c_str());
		}

		column_t oid = columns.size();
		name_map[entry.name] = oid;
		entry.oid = oid;
		columns.push_back(entry);
	}
	storage = make_unique<DataTable>(catalog->storage, *this);
}

bool TableCatalogEntry::ColumnExists(const string &name) {
	return name_map.find(name) != name_map.end();
}

ColumnDefinition &TableCatalogEntry::GetColumn(const std::string &name) {
	if (!ColumnExists(name)) {
		throw CatalogException("Column with name %s does not exist!",
		                       name.c_str());
	}
	return columns[name_map[name]];
}

Statistics &TableCatalogEntry::GetStatistics(column_t oid) {
	return storage->GetStatistics(oid);
}

vector<TypeId> TableCatalogEntry::GetTypes() {
	vector<TypeId> types;
	for (auto &it : columns) {
		types.push_back(it.type);
	}
	return types;
}
