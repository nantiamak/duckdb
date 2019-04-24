
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
// THIS FILE IS GENERATED BY gentpcecode.py, DO NOT EDIT MANUALLY //
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////


#include "catalog/catalog.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "storage/data_table.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"

#include "main/BaseLoader.h"
#include "main/BaseLoaderFactory.h"
#include "main/NullLoader.h"
#include "main/TableRows.h"

namespace TPCE {
	class DuckDBLoaderFactory : public CBaseLoaderFactory {
		duckdb::ClientContext *context;
		std::string schema;
		std::string suffix;

	  public:
		DuckDBLoaderFactory(duckdb::ClientContext *context, std::string schema,
		                    std::string suffix)
		    : context(context), schema(schema), suffix(suffix) {
		}

		// Functions to create loader classes for individual tables.
		virtual CBaseLoader<ACCOUNT_PERMISSION_ROW> *
		CreateAccountPermissionLoader();
		virtual CBaseLoader<ADDRESS_ROW> *CreateAddressLoader();
		virtual CBaseLoader<BROKER_ROW> *CreateBrokerLoader();
		virtual CBaseLoader<CASH_TRANSACTION_ROW> *
		CreateCashTransactionLoader();
		virtual CBaseLoader<CHARGE_ROW> *CreateChargeLoader();
		virtual CBaseLoader<COMMISSION_RATE_ROW> *CreateCommissionRateLoader();
		virtual CBaseLoader<COMPANY_COMPETITOR_ROW> *
		CreateCompanyCompetitorLoader();
		virtual CBaseLoader<COMPANY_ROW> *CreateCompanyLoader();
		virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW> *
		CreateCustomerAccountLoader();
		virtual CBaseLoader<CUSTOMER_ROW> *CreateCustomerLoader();
		virtual CBaseLoader<CUSTOMER_TAXRATE_ROW> *
		CreateCustomerTaxrateLoader();
		virtual CBaseLoader<DAILY_MARKET_ROW> *CreateDailyMarketLoader();
		virtual CBaseLoader<EXCHANGE_ROW> *CreateExchangeLoader();
		virtual CBaseLoader<FINANCIAL_ROW> *CreateFinancialLoader();
		virtual CBaseLoader<HOLDING_ROW> *CreateHoldingLoader();
		virtual CBaseLoader<HOLDING_HISTORY_ROW> *CreateHoldingHistoryLoader();
		virtual CBaseLoader<HOLDING_SUMMARY_ROW> *CreateHoldingSummaryLoader();
		virtual CBaseLoader<INDUSTRY_ROW> *CreateIndustryLoader();
		virtual CBaseLoader<LAST_TRADE_ROW> *CreateLastTradeLoader();
		virtual CBaseLoader<NEWS_ITEM_ROW> *CreateNewsItemLoader();
		virtual CBaseLoader<NEWS_XREF_ROW> *CreateNewsXRefLoader();
		virtual CBaseLoader<SECTOR_ROW> *CreateSectorLoader();
		virtual CBaseLoader<SECURITY_ROW> *CreateSecurityLoader();
		virtual CBaseLoader<SETTLEMENT_ROW> *CreateSettlementLoader();
		virtual CBaseLoader<STATUS_TYPE_ROW> *CreateStatusTypeLoader();
		virtual CBaseLoader<TAX_RATE_ROW> *CreateTaxRateLoader();
		virtual CBaseLoader<TRADE_HISTORY_ROW> *CreateTradeHistoryLoader();
		virtual CBaseLoader<TRADE_ROW> *CreateTradeLoader();
		virtual CBaseLoader<TRADE_REQUEST_ROW> *CreateTradeRequestLoader();
		virtual CBaseLoader<TRADE_TYPE_ROW> *CreateTradeTypeLoader();
		virtual CBaseLoader<WATCH_ITEM_ROW> *CreateWatchItemLoader();
		virtual CBaseLoader<WATCH_LIST_ROW> *CreateWatchListLoader();
		virtual CBaseLoader<ZIP_CODE_ROW> *CreateZipCodeLoader();
	};

void CreateTPCESchema(duckdb::DuckDB &db, duckdb::Transaction &transaction, std::string &schema, std::string &suffix);

} /* namespace TPCE */
