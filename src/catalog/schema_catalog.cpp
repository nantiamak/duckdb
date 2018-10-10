
#include "catalog/schema_catalog.hpp"
#include "catalog/catalog.hpp"

#include "common/exception.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name)
    : AbstractCatalogEntry(CatalogType::SCHEMA, catalog, name) {}

void SchemaCatalogEntry::CreateTable(Transaction &transaction,
                                     CreateTableInformation *info) {
	auto table = new TableCatalogEntry(catalog, this, info);
	auto table_entry = unique_ptr<AbstractCatalogEntry>(table);
	if (!tables.CreateEntry(transaction, info->table, move(table_entry))) {
		if (!info->if_not_exists) {
			throw CatalogException("Table with name %s already exists!",
			                       info->table.c_str());
		}
	}
}

void SchemaCatalogEntry::DropTable(Transaction &transaction,
                                   const string &table_name) {
	GetTable(transaction, table_name);
	if (!tables.DropEntry(transaction, table_name)) {
		// TODO: do we care if its already marked as deleted?
	}
}

bool SchemaCatalogEntry::TableExists(Transaction &transaction,
                                     const string &table_name) {
	return tables.EntryExists(transaction, table_name);
}

TableCatalogEntry *SchemaCatalogEntry::GetTable(Transaction &transaction,
                                                const string &table_name) {
	auto entry = tables.GetEntry(transaction, table_name);
	if (!entry) {
		throw CatalogException("Table with name %s does not exist!",
		                       table_name.c_str());
	}
	return (TableCatalogEntry *)entry;
}
