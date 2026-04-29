#ifndef AUTO_INDEXER_H
#define AUTO_INDEXER_H

#include "postgres.h"
#include "nodes/pathnodes.h"

void AutoIndex_Init(void);
void AutoIndex_Update(Oid relid, int attno);
void AutoIndex_ConsiderRel(PlannerInfo *root, RelOptInfo *rel);
void AutoIndex_Print(void);

#endif
