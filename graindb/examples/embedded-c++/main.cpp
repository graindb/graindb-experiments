#include "dbgen.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "imdb.hpp"
#include "ldbc.hpp"

#include <iostream>

using namespace std;
using namespace duckdb;

static double GetSizeInMB(idx_t sizeInBytes) {
	return (double)sizeInBytes / (1024.0 * 1024.0);
}

static void ProfileStorageOverheads(Connection &con, DuckDB &db, const string &benchmark) {
	if (benchmark == "LDBC") {
		ldbc::dbgen(con, (int)10, true);
	} else if (benchmark == "IMDB") {
		imdb::dbgen(con, true);
	} else if (benchmark == "TPCH") {
		tpch::dbgen(10, db, true);
	}
	if (!con.context->transaction.HasActiveTransaction()) {
		con.context->transaction.BeginTransaction();
	}
	auto &table_set = con.db.catalog->GetSchema(*con.context, DEFAULT_SCHEMA)->tables;
	auto table_names = table_set.GetEntryNames();
	idx_t regular_columns_size_total = 0.0;
	idx_t index_columns_size_total = 0;
	idx_t rid_index_size_total = 0;
	idx_t extended_rid_index_size_total = 0;
	for (auto &table_name : table_names) {
		auto table = (TableCatalogEntry *)table_set.GetEntry(con.context->transaction.ActiveTransaction(), table_name);
		// Regular columns and materialized RIDs
		auto regular_columns_size = table->GetColumnsSize(true);
		auto index_columns_size = table->GetColumnsSize(false);
		cout << table_name << ": " << regular_columns_size << ", " << GetSizeInMB(index_columns_size) << " MB." << endl;
		regular_columns_size_total += regular_columns_size;
		index_columns_size_total += index_columns_size;
		// RID Index and Extended RID Index
		for (auto &index : table->storage->info->rais) {
			auto index_size = index->GetIndexSize();
			rid_index_size_total += index_size;
			auto extended_index_size = index->GetExtendedIndexSize();
			extended_rid_index_size_total += extended_index_size;
			cout << "RIDIndex " << index->alist->alias << ": " << GetSizeInMB(index_size) << ", "
			     << GetSizeInMB(extended_index_size) << " MB." << endl;
		}
	}
	con.context->transaction.Commit();
	// DuckDB, GR-JM-RSJ, GR-JM, GR-FULL
	auto duckdb_mb = GetSizeInMB(regular_columns_size_total);
	auto gr_jm_rsj_mb = GetSizeInMB(regular_columns_size_total) + GetSizeInMB(index_columns_size_total);
	auto gr_jm_mb = GetSizeInMB(regular_columns_size_total) + GetSizeInMB(index_columns_size_total) +
	                GetSizeInMB(rid_index_size_total);
	auto gr_full_mb = GetSizeInMB(regular_columns_size_total) + GetSizeInMB(index_columns_size_total) +
	                  GetSizeInMB(extended_rid_index_size_total);
	cout << "========================" << endl;
	cout << "| " << benchmark << " |" << endl;
	cout << "========================" << endl;
	cout << duckdb_mb << ", " << gr_jm_rsj_mb << ", " << gr_jm_mb << ", " << gr_full_mb << endl;
	cout << duckdb_mb / 1024.0 << "," << gr_jm_rsj_mb / 1024.0 << "," << gr_jm_mb / 1024.0 << "," << gr_full_mb / 1024.0
	     << endl;
}

void StorageProfiling(const string &benchmark) {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	ProfileStorageOverheads(con, db, benchmark);
}

int main(int argc, char *argv[]) {
	StorageProfiling("IMDB");
	StorageProfiling("LDBC");
	StorageProfiling("TPCH");
}
