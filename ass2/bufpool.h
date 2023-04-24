#include "db.h"

typedef struct buffer {
	UINT64 page_id;
	UINT   table_oid;
	int    pin;
	int    usage_count;
	int    ntuples;
	INT    data[];
}buffer;

typedef struct file {
	UINT   oid;
	int    pin;
	FILE*  fp;
}file;


typedef struct BufPool {
	int   	nbufs;     
	int 	page_size;
	int 	file_limit;
	buffer 	**bufs;
	file    *files;
}BufPool;

BufPool* initBufPool(int, int, int);

int request_page(BufPool* pool, UINT table_oid, UINT64 page_id, Database* db);
void release_page(BufPool* pool, UINT table_oid, UINT64 page_id);
FILE* getFilePointer(int oid, const char* filename, BufPool* pool);
void release_file_pointer(int oid, BufPool* pool) ;
