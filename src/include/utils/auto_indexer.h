#ifndef AUTO_INDEXER_H
#define AUTO_INDEXER_H

#include "postgres.h"
#include "nodes/pathnodes.h"

typedef enum AutoIndexTouchKind
{
    AUTO_INDEX_TOUCH_ACCESS,
    AUTO_INDEX_TOUCH_INSERT,
    AUTO_INDEX_TOUCH_UPDATE,
    AUTO_INDEX_TOUCH_DELETE
} AutoIndexTouchKind;

void AutoIndex_Init(void);
void AutoIndex_BeginQuery(const char *query_string, int command_type);
void AutoIndex_Update(Oid relid, int attno);
void AutoIndex_RecordTouch(Oid relid, int attno, AutoIndexTouchKind kind);
void AutoIndex_ObserveSeqScan(Oid relid);
void AutoIndex_ConsiderRel(PlannerInfo *root, RelOptInfo *rel);
void AutoIndex_Print(void);
void AutoIndex_EnterInternalQuery(void);
void AutoIndex_LeaveInternalQuery(void);

#endif
