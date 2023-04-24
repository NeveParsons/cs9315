#ifndef RO_H
#define RO_H
#include "db.h"
#include "bufpool.h"

struct Node {
    struct Node* next;
    INT   data[];
};

struct tupleItem {
   int offset;   
   int slot;
   int key;
   struct tupleItem *next;
};


void init();
void release();

// equality test for one attribute
// idx: index of the attribute for comparison, 0 <= idx < nattrs
// cond_val: the compared value
// table_name: table name
_Table* sel(const UINT idx, const INT cond_val, const char* table_name);

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name);
#endif