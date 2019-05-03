#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;

std::string Transformer::NodetypeToString(postgres::NodeTag type) {
	switch(type){
	case T_Invalid: return "T_Invalid";
	case T_IndexInfo: return "T_IndexInfo";
	case T_ExprContext: return "T_ExprContext";
	case T_ProjectionInfo: return "T_ProjectionInfo";
	case T_JunkFilter: return "T_JunkFilter";
	case T_ResultRelInfo: return "T_ResultRelInfo";
	case T_EState: return "T_EState";
	case T_TupleTableSlot: return "T_TupleTableSlot";
	case T_Plan: return "T_Plan";
	case T_Result: return "T_Result";
	case T_ProjectSet: return "T_ProjectSet";
	case T_ModifyTable: return "T_ModifyTable";
	case T_Append: return "T_Append";
	case T_MergeAppend: return "T_MergeAppend";
	case T_RecursiveUnion: return "T_RecursiveUnion";
	case T_BitmapAnd: return "T_BitmapAnd";
	case T_BitmapOr: return "T_BitmapOr";
	case T_Scan: return "T_Scan";
	case T_SeqScan: return "T_SeqScan";
	case T_SampleScan: return "T_SampleScan";
	case T_IndexScan: return "T_IndexScan";
	case T_IndexOnlyScan: return "T_IndexOnlyScan";
	case T_BitmapIndexScan: return "T_BitmapIndexScan";
	case T_BitmapHeapScan: return "T_BitmapHeapScan";
	case T_TidScan: return "T_TidScan";
	case T_SubqueryScan: return "T_SubqueryScan";
	case T_FunctionScan: return "T_FunctionScan";
	case T_ValuesScan: return "T_ValuesScan";
	case T_TableFuncScan: return "T_TableFuncScan";
	case T_CteScan: return "T_CteScan";
	case T_NamedTuplestoreScan: return "T_NamedTuplestoreScan";
	case T_WorkTableScan: return "T_WorkTableScan";
	case T_ForeignScan: return "T_ForeignScan";
	case T_CustomScan: return "T_CustomScan";
	case T_Join: return "T_Join";
	case T_NestLoop: return "T_NestLoop";
	case T_MergeJoin: return "T_MergeJoin";
	case T_HashJoin: return "T_HashJoin";
	case T_Material: return "T_Material";
	case T_Sort: return "T_Sort";
	case T_Group: return "T_Group";
	case T_Agg: return "T_Agg";
	case T_WindowAgg: return "T_WindowAgg";
	case T_Unique: return "T_Unique";
	case T_Gather: return "T_Gather";
	case T_GatherMerge: return "T_GatherMerge";
	case T_Hash: return "T_Hash";
	case T_SetOp: return "T_SetOp";
	case T_LockRows: return "T_LockRows";
	case T_Limit: return "T_Limit";
	case T_NestLoopParam: return "T_NestLoopParam";
	case T_PlanRowMark: return "T_PlanRowMark";
	case T_PlanInvalItem: return "T_PlanInvalItem";
	case T_PlanState: return "T_PlanState";
	case T_ResultState: return "T_ResultState";
	case T_ProjectSetState: return "T_ProjectSetState";
	case T_ModifyTableState: return "T_ModifyTableState";
	case T_AppendState: return "T_AppendState";
	case T_MergeAppendState: return "T_MergeAppendState";
	case T_RecursiveUnionState: return "T_RecursiveUnionState";
	case T_BitmapAndState: return "T_BitmapAndState";
	case T_BitmapOrState: return "T_BitmapOrState";
	case T_ScanState: return "T_ScanState";
	case T_SeqScanState: return "T_SeqScanState";
	case T_SampleScanState: return "T_SampleScanState";
	case T_IndexScanState: return "T_IndexScanState";
	case T_IndexOnlyScanState: return "T_IndexOnlyScanState";
	case T_BitmapIndexScanState: return "T_BitmapIndexScanState";
	case T_BitmapHeapScanState: return "T_BitmapHeapScanState";
	case T_TidScanState: return "T_TidScanState";
	case T_SubqueryScanState: return "T_SubqueryScanState";
	case T_FunctionScanState: return "T_FunctionScanState";
	case T_TableFuncScanState: return "T_TableFuncScanState";
	case T_ValuesScanState: return "T_ValuesScanState";
	case T_CteScanState: return "T_CteScanState";
	case T_NamedTuplestoreScanState: return "T_NamedTuplestoreScanState";
	case T_WorkTableScanState: return "T_WorkTableScanState";
	case T_ForeignScanState: return "T_ForeignScanState";
	case T_CustomScanState: return "T_CustomScanState";
	case T_JoinState: return "T_JoinState";
	case T_NestLoopState: return "T_NestLoopState";
	case T_MergeJoinState: return "T_MergeJoinState";
	case T_HashJoinState: return "T_HashJoinState";
	case T_MaterialState: return "T_MaterialState";
	case T_SortState: return "T_SortState";
	case T_GroupState: return "T_GroupState";
	case T_AggState: return "T_AggState";
	case T_WindowAggState: return "T_WindowAggState";
	case T_UniqueState: return "T_UniqueState";
	case T_GatherState: return "T_GatherState";
	case T_GatherMergeState: return "T_GatherMergeState";
	case T_HashState: return "T_HashState";
	case T_SetOpState: return "T_SetOpState";
	case T_LockRowsState: return "T_LockRowsState";
	case T_LimitState: return "T_LimitState";
	case T_Alias: return "T_Alias";
	case T_RangeVar: return "T_RangeVar";
	case T_TableFunc: return "T_TableFunc";
	case T_Expr: return "T_Expr";
	case T_Var: return "T_Var";
	case T_Const: return "T_Const";
	case T_Param: return "T_Param";
	case T_Aggref: return "T_Aggref";
	case T_GroupingFunc: return "T_GroupingFunc";
	case T_WindowFunc: return "T_WindowFunc";
	case T_ArrayRef: return "T_ArrayRef";
	case T_FuncExpr: return "T_FuncExpr";
	case T_NamedArgExpr: return "T_NamedArgExpr";
	case T_OpExpr: return "T_OpExpr";
	case T_DistinctExpr: return "T_DistinctExpr";
	case T_NullIfExpr: return "T_NullIfExpr";
	case T_ScalarArrayOpExpr: return "T_ScalarArrayOpExpr";
	case T_BoolExpr: return "T_BoolExpr";
	case T_SubLink: return "T_SubLink";
	case T_SubPlan: return "T_SubPlan";
	case T_AlternativeSubPlan: return "T_AlternativeSubPlan";
	case T_FieldSelect: return "T_FieldSelect";
	case T_FieldStore: return "T_FieldStore";
	case T_RelabelType: return "T_RelabelType";
	case T_CoerceViaIO: return "T_CoerceViaIO";
	case T_ArrayCoerceExpr: return "T_ArrayCoerceExpr";
	case T_ConvertRowtypeExpr: return "T_ConvertRowtypeExpr";
	case T_CollateExpr: return "T_CollateExpr";
	case T_CaseExpr: return "T_CaseExpr";
	case T_CaseWhen: return "T_CaseWhen";
	case T_CaseTestExpr: return "T_CaseTestExpr";
	case T_ArrayExpr: return "T_ArrayExpr";
	case T_RowExpr: return "T_RowExpr";
	case T_RowCompareExpr: return "T_RowCompareExpr";
	case T_CoalesceExpr: return "T_CoalesceExpr";
	case T_MinMaxExpr: return "T_MinMaxExpr";
	case T_SQLValueFunction: return "T_SQLValueFunction";
	case T_XmlExpr: return "T_XmlExpr";
	case T_NullTest: return "T_NullTest";
	case T_BooleanTest: return "T_BooleanTest";
	case T_CoerceToDomain: return "T_CoerceToDomain";
	case T_CoerceToDomainValue: return "T_CoerceToDomainValue";
	case T_SetToDefault: return "T_SetToDefault";
	case T_CurrentOfExpr: return "T_CurrentOfExpr";
	case T_NextValueExpr: return "T_NextValueExpr";
	case T_InferenceElem: return "T_InferenceElem";
	case T_TargetEntry: return "T_TargetEntry";
	case T_RangeTblRef: return "T_RangeTblRef";
	case T_JoinExpr: return "T_JoinExpr";
	case T_FromExpr: return "T_FromExpr";
	case T_OnConflictExpr: return "T_OnConflictExpr";
	case T_IntoClause: return "T_IntoClause";
	case T_ExprState: return "T_ExprState";
	case T_AggrefExprState: return "T_AggrefExprState";
	case T_WindowFuncExprState: return "T_WindowFuncExprState";
	case T_SetExprState: return "T_SetExprState";
	case T_SubPlanState: return "T_SubPlanState";
	case T_AlternativeSubPlanState: return "T_AlternativeSubPlanState";
	case T_DomainConstraintState: return "T_DomainConstraintState";
	case T_PlannerInfo: return "T_PlannerInfo";
	case T_PlannerGlobal: return "T_PlannerGlobal";
	case T_RelOptInfo: return "T_RelOptInfo";
	case T_IndexOptInfo: return "T_IndexOptInfo";
	case T_ForeignKeyOptInfo: return "T_ForeignKeyOptInfo";
	case T_ParamPathInfo: return "T_ParamPathInfo";
	case T_Path: return "T_Path";
	case T_IndexPath: return "T_IndexPath";
	case T_BitmapHeapPath: return "T_BitmapHeapPath";
	case T_BitmapAndPath: return "T_BitmapAndPath";
	case T_BitmapOrPath: return "T_BitmapOrPath";
	case T_TidPath: return "T_TidPath";
	case T_SubqueryScanPath: return "T_SubqueryScanPath";
	case T_ForeignPath: return "T_ForeignPath";
	case T_CustomPath: return "T_CustomPath";
	case T_NestPath: return "T_NestPath";
	case T_MergePath: return "T_MergePath";
	case T_HashPath: return "T_HashPath";
	case T_AppendPath: return "T_AppendPath";
	case T_MergeAppendPath: return "T_MergeAppendPath";
	case T_ResultPath: return "T_ResultPath";
	case T_MaterialPath: return "T_MaterialPath";
	case T_UniquePath: return "T_UniquePath";
	case T_GatherPath: return "T_GatherPath";
	case T_GatherMergePath: return "T_GatherMergePath";
	case T_ProjectionPath: return "T_ProjectionPath";
	case T_ProjectSetPath: return "T_ProjectSetPath";
	case T_SortPath: return "T_SortPath";
	case T_GroupPath: return "T_GroupPath";
	case T_UpperUniquePath: return "T_UpperUniquePath";
	case T_AggPath: return "T_AggPath";
	case T_GroupingSetsPath: return "T_GroupingSetsPath";
	case T_MinMaxAggPath: return "T_MinMaxAggPath";
	case T_WindowAggPath: return "T_WindowAggPath";
	case T_SetOpPath: return "T_SetOpPath";
	case T_RecursiveUnionPath: return "T_RecursiveUnionPath";
	case T_LockRowsPath: return "T_LockRowsPath";
	case T_ModifyTablePath: return "T_ModifyTablePath";
	case T_LimitPath: return "T_LimitPath";
	case T_EquivalenceClass: return "T_EquivalenceClass";
	case T_EquivalenceMember: return "T_EquivalenceMember";
	case T_PathKey: return "T_PathKey";
	case T_PathTarget: return "T_PathTarget";
	case T_RestrictInfo: return "T_RestrictInfo";
	case T_PlaceHolderVar: return "T_PlaceHolderVar";
	case T_SpecialJoinInfo: return "T_SpecialJoinInfo";
	case T_AppendRelInfo: return "T_AppendRelInfo";
	case T_PartitionedChildRelInfo: return "T_PartitionedChildRelInfo";
	case T_PlaceHolderInfo: return "T_PlaceHolderInfo";
	case T_MinMaxAggInfo: return "T_MinMaxAggInfo";
	case T_PlannerParamItem: return "T_PlannerParamItem";
	case T_RollupData: return "T_RollupData";
	case T_GroupingSetData: return "T_GroupingSetData";
	case T_StatisticExtInfo: return "T_StatisticExtInfo";
	case T_MemoryContext: return "T_MemoryContext";
	case T_AllocSetContext: return "T_AllocSetContext";
	case T_SlabContext: return "T_SlabContext";
	case T_Value: return "T_Value";
	case T_Integer: return "T_Integer";
	case T_Float: return "T_Float";
	case T_String: return "T_String";
	case T_BitString: return "T_BitString";
	case T_Null: return "T_Null";
	case T_List: return "T_List";
	case T_IntList: return "T_IntList";
	case T_OidList: return "T_OidList";
	case T_ExtensibleNode: return "T_ExtensibleNode";
	case T_RawStmt: return "T_RawStmt";
	case T_Query: return "T_Query";
	case T_PlannedStmt: return "T_PlannedStmt";
	case T_InsertStmt: return "T_InsertStmt";
	case T_DeleteStmt: return "T_DeleteStmt";
	case T_UpdateStmt: return "T_UpdateStmt";
	case T_SelectStmt: return "T_SelectStmt";
	case T_AlterTableStmt: return "T_AlterTableStmt";
	case T_AlterTableCmd: return "T_AlterTableCmd";
	case T_AlterDomainStmt: return "T_AlterDomainStmt";
	case T_SetOperationStmt: return "T_SetOperationStmt";
	case T_GrantStmt: return "T_GrantStmt";
	case T_GrantRoleStmt: return "T_GrantRoleStmt";
	case T_AlterDefaultPrivilegesStmt: return "T_AlterDefaultPrivilegesStmt";
	case T_ClosePortalStmt: return "T_ClosePortalStmt";
	case T_ClusterStmt: return "T_ClusterStmt";
	case T_CopyStmt: return "T_CopyStmt";
	case T_CreateStmt: return "T_CreateStmt";
	case T_DefineStmt: return "T_DefineStmt";
	case T_DropStmt: return "T_DropStmt";
	case T_TruncateStmt: return "T_TruncateStmt";
	case T_CommentStmt: return "T_CommentStmt";
	case T_FetchStmt: return "T_FetchStmt";
	case T_IndexStmt: return "T_IndexStmt";
	case T_CreateFunctionStmt: return "T_CreateFunctionStmt";
	case T_AlterFunctionStmt: return "T_AlterFunctionStmt";
	case T_DoStmt: return "T_DoStmt";
	case T_RenameStmt: return "T_RenameStmt";
	case T_RuleStmt: return "T_RuleStmt";
	case T_NotifyStmt: return "T_NotifyStmt";
	case T_ListenStmt: return "T_ListenStmt";
	case T_UnlistenStmt: return "T_UnlistenStmt";
	case T_TransactionStmt: return "T_TransactionStmt";
	case T_ViewStmt: return "T_ViewStmt";
	case T_LoadStmt: return "T_LoadStmt";
	case T_CreateDomainStmt: return "T_CreateDomainStmt";
	case T_CreatedbStmt: return "T_CreatedbStmt";
	case T_DropdbStmt: return "T_DropdbStmt";
	case T_VacuumStmt: return "T_VacuumStmt";
	case T_ExplainStmt: return "T_ExplainStmt";
	case T_CreateTableAsStmt: return "T_CreateTableAsStmt";
	case T_CreateSeqStmt: return "T_CreateSeqStmt";
	case T_AlterSeqStmt: return "T_AlterSeqStmt";
	case T_VariableSetStmt: return "T_VariableSetStmt";
	case T_VariableShowStmt: return "T_VariableShowStmt";
	case T_DiscardStmt: return "T_DiscardStmt";
	case T_CreateTrigStmt: return "T_CreateTrigStmt";
	case T_CreatePLangStmt: return "T_CreatePLangStmt";
	case T_CreateRoleStmt: return "T_CreateRoleStmt";
	case T_AlterRoleStmt: return "T_AlterRoleStmt";
	case T_DropRoleStmt: return "T_DropRoleStmt";
	case T_LockStmt: return "T_LockStmt";
	case T_ConstraintsSetStmt: return "T_ConstraintsSetStmt";
	case T_ReindexStmt: return "T_ReindexStmt";
	case T_CheckPointStmt: return "T_CheckPointStmt";
	case T_CreateSchemaStmt: return "T_CreateSchemaStmt";
	case T_AlterDatabaseStmt: return "T_AlterDatabaseStmt";
	case T_AlterDatabaseSetStmt: return "T_AlterDatabaseSetStmt";
	case T_AlterRoleSetStmt: return "T_AlterRoleSetStmt";
	case T_CreateConversionStmt: return "T_CreateConversionStmt";
	case T_CreateCastStmt: return "T_CreateCastStmt";
	case T_CreateOpClassStmt: return "T_CreateOpClassStmt";
	case T_CreateOpFamilyStmt: return "T_CreateOpFamilyStmt";
	case T_AlterOpFamilyStmt: return "T_AlterOpFamilyStmt";
	case T_PrepareStmt: return "T_PrepareStmt";
	case T_ExecuteStmt: return "T_ExecuteStmt";
	case T_DeallocateStmt: return "T_DeallocateStmt";
	case T_DeclareCursorStmt: return "T_DeclareCursorStmt";
	case T_CreateTableSpaceStmt: return "T_CreateTableSpaceStmt";
	case T_DropTableSpaceStmt: return "T_DropTableSpaceStmt";
	case T_AlterObjectDependsStmt: return "T_AlterObjectDependsStmt";
	case T_AlterObjectSchemaStmt: return "T_AlterObjectSchemaStmt";
	case T_AlterOwnerStmt: return "T_AlterOwnerStmt";
	case T_AlterOperatorStmt: return "T_AlterOperatorStmt";
	case T_DropOwnedStmt: return "T_DropOwnedStmt";
	case T_ReassignOwnedStmt: return "T_ReassignOwnedStmt";
	case T_CompositeTypeStmt: return "T_CompositeTypeStmt";
	case T_CreateEnumStmt: return "T_CreateEnumStmt";
	case T_CreateRangeStmt: return "T_CreateRangeStmt";
	case T_AlterEnumStmt: return "T_AlterEnumStmt";
	case T_AlterTSDictionaryStmt: return "T_AlterTSDictionaryStmt";
	case T_AlterTSConfigurationStmt: return "T_AlterTSConfigurationStmt";
	case T_CreateFdwStmt: return "T_CreateFdwStmt";
	case T_AlterFdwStmt: return "T_AlterFdwStmt";
	case T_CreateForeignServerStmt: return "T_CreateForeignServerStmt";
	case T_AlterForeignServerStmt: return "T_AlterForeignServerStmt";
	case T_CreateUserMappingStmt: return "T_CreateUserMappingStmt";
	case T_AlterUserMappingStmt: return "T_AlterUserMappingStmt";
	case T_DropUserMappingStmt: return "T_DropUserMappingStmt";
	case T_AlterTableSpaceOptionsStmt: return "T_AlterTableSpaceOptionsStmt";
	case T_AlterTableMoveAllStmt: return "T_AlterTableMoveAllStmt";
	case T_SecLabelStmt: return "T_SecLabelStmt";
	case T_CreateForeignTableStmt: return "T_CreateForeignTableStmt";
	case T_ImportForeignSchemaStmt: return "T_ImportForeignSchemaStmt";
	case T_CreateExtensionStmt: return "T_CreateExtensionStmt";
	case T_AlterExtensionStmt: return "T_AlterExtensionStmt";
	case T_AlterExtensionContentsStmt: return "T_AlterExtensionContentsStmt";
	case T_CreateEventTrigStmt: return "T_CreateEventTrigStmt";
	case T_AlterEventTrigStmt: return "T_AlterEventTrigStmt";
	case T_RefreshMatViewStmt: return "T_RefreshMatViewStmt";
	case T_ReplicaIdentityStmt: return "T_ReplicaIdentityStmt";
	case T_AlterSystemStmt: return "T_AlterSystemStmt";
	case T_CreatePolicyStmt: return "T_CreatePolicyStmt";
	case T_AlterPolicyStmt: return "T_AlterPolicyStmt";
	case T_CreateTransformStmt: return "T_CreateTransformStmt";
	case T_CreateAmStmt: return "T_CreateAmStmt";
	case T_CreatePublicationStmt: return "T_CreatePublicationStmt";
	case T_AlterPublicationStmt: return "T_AlterPublicationStmt";
	case T_CreateSubscriptionStmt: return "T_CreateSubscriptionStmt";
	case T_AlterSubscriptionStmt: return "T_AlterSubscriptionStmt";
	case T_DropSubscriptionStmt: return "T_DropSubscriptionStmt";
	case T_CreateStatsStmt: return "T_CreateStatsStmt";
	case T_AlterCollationStmt: return "T_AlterCollationStmt";
	case T_A_Expr: return "T_A_Expr";
	case T_ColumnRef: return "T_ColumnRef";
	case T_ParamRef: return "T_ParamRef";
	case T_A_Const: return "T_A_Const";
	case T_FuncCall: return "T_FuncCall";
	case T_A_Star: return "T_A_Star";
	case T_A_Indices: return "T_A_Indices";
	case T_A_Indirection: return "T_A_Indirection";
	case T_A_ArrayExpr: return "T_A_ArrayExpr";
	case T_ResTarget: return "T_ResTarget";
	case T_MultiAssignRef: return "T_MultiAssignRef";
	case T_TypeCast: return "T_TypeCast";
	case T_CollateClause: return "T_CollateClause";
	case T_SortBy: return "T_SortBy";
	case T_WindowDef: return "T_WindowDef";
	case T_RangeSubselect: return "T_RangeSubselect";
	case T_RangeFunction: return "T_RangeFunction";
	case T_RangeTableSample: return "T_RangeTableSample";
	case T_RangeTableFunc: return "T_RangeTableFunc";
	case T_RangeTableFuncCol: return "T_RangeTableFuncCol";
	case T_TypeName: return "T_TypeName";
	case T_ColumnDef: return "T_ColumnDef";
	case T_IndexElem: return "T_IndexElem";
	case T_Constraint: return "T_Constraint";
	case T_DefElem: return "T_DefElem";
	case T_RangeTblEntry: return "T_RangeTblEntry";
	case T_RangeTblFunction: return "T_RangeTblFunction";
	case T_TableSampleClause: return "T_TableSampleClause";
	case T_WithCheckOption: return "T_WithCheckOption";
	case T_SortGroupClause: return "T_SortGroupClause";
	case T_GroupingSet: return "T_GroupingSet";
	case T_WindowClause: return "T_WindowClause";
	case T_ObjectWithArgs: return "T_ObjectWithArgs";
	case T_AccessPriv: return "T_AccessPriv";
	case T_CreateOpClassItem: return "T_CreateOpClassItem";
	case T_TableLikeClause: return "T_TableLikeClause";
	case T_FunctionParameter: return "T_FunctionParameter";
	case T_LockingClause: return "T_LockingClause";
	case T_RowMarkClause: return "T_RowMarkClause";
	case T_XmlSerialize: return "T_XmlSerialize";
	case T_WithClause: return "T_WithClause";
	case T_InferClause: return "T_InferClause";
	case T_OnConflictClause: return "T_OnConflictClause";
	case T_CommonTableExpr: return "T_CommonTableExpr";
	case T_RoleSpec: return "T_RoleSpec";
	case T_TriggerTransition: return "T_TriggerTransition";
	case T_PartitionElem: return "T_PartitionElem";
	case T_PartitionSpec: return "T_PartitionSpec";
	case T_PartitionBoundSpec: return "T_PartitionBoundSpec";
	case T_PartitionRangeDatum: return "T_PartitionRangeDatum";
	case T_PartitionCmd: return "T_PartitionCmd";
	case T_IdentifySystemCmd: return "T_IdentifySystemCmd";
	case T_BaseBackupCmd: return "T_BaseBackupCmd";
	case T_CreateReplicationSlotCmd: return "T_CreateReplicationSlotCmd";
	case T_DropReplicationSlotCmd: return "T_DropReplicationSlotCmd";
	case T_StartReplicationCmd: return "T_StartReplicationCmd";
	case T_TimeLineHistoryCmd: return "T_TimeLineHistoryCmd";
	case T_SQLCmd: return "T_SQLCmd";
	case T_TriggerData: return "T_TriggerData";
	case T_EventTriggerData: return "T_EventTriggerData";
	case T_ReturnSetInfo	: return "T_ReturnSetInfo";
	case T_WindowObjectData: return "T_WindowObjectData";
	case T_TIDBitmap: return "T_TIDBitmap";
	case T_InlineCodeBlock: return "T_InlineCodeBlock";
	case T_FdwRoutine: return "T_FdwRoutine";
	case T_IndexAmRoutine: return "T_IndexAmRoutine";
	case T_TsmRoutine: return "T_TsmRoutine";
	case T_ForeignKeyCacheInfo: return "T_ForeignKeyCacheInfo";
	default:
		assert(0);
		return "";
	}
}
