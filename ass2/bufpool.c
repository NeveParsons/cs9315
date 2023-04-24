#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include <time.h>
#include "bufpool.h"
#include "db.h"

#define USAGE_LIMIT 10 // limit on usage count

void writePageToBuffer(UINT64 page_id, UINT oid, Database* db, BufPool* pool, int slot);

BufPool* initBufPool(int nbufs, int page_size, int file_limit)
{
	BufPool* newPool;

	newPool = malloc(sizeof(BufPool));
	assert(newPool != NULL);

	newPool->nbufs = nbufs;
	newPool->page_size = page_size;
	newPool->file_limit = file_limit;

	newPool->files = malloc(file_limit * sizeof(file));
	assert(newPool->files != NULL);
	for (int i = 0 ; i < file_limit; i++) {
		newPool->files[i].oid = -1;
		newPool->files[i].pin = 0;
		newPool->files[i].fp = NULL;
	}
	
	newPool->bufs = malloc(nbufs * sizeof(buffer*));
	assert(newPool->bufs != NULL);
	int i;
	for (i = 0; i < nbufs; i++) {
		newPool->bufs[i] = malloc(sizeof(buffer) + page_size);
		newPool->bufs[i]->page_id = -1;
		newPool->bufs[i]->table_oid = -1;
		newPool->bufs[i]->pin = 0;
		newPool->bufs[i]->usage_count = 0;
		newPool->bufs[i]->ntuples = 0;
		
	}
	return newPool;
}


static unsigned int clock1 = 0;
static unsigned int numFileEntries = 0;

static
int pageInPool(BufPool* pool, UINT table_oid, UINT64 page_id)
{
	for (int i = 0; i < pool->nbufs; i++) {
		if ( (pool->bufs[i]->page_id == page_id) && (pool->bufs[i]->table_oid == table_oid) ) {
			return i;
		}
	}
	return -1;
}

static
int grabNextSlot(BufPool* pool)
{	
    int slot = -1;
	while (slot < 0) {
		if (pool->bufs[clock1]->pin == 0 && pool->bufs[clock1]->usage_count == 0) {
			pool->bufs[clock1]->page_id = -1;
			pool->bufs[clock1]->table_oid = -1;
			pool->bufs[clock1]->pin = 0;
			slot = clock1;
		}
		else if(pool->bufs[clock1]->usage_count > 0) {
			pool->bufs[clock1]->usage_count--;
		}
		clock1 = (clock1 + 1) % pool->nbufs;
	}
	return slot;
}
     
static Table* getTable(Database* db, UINT oid) {
    for (int i = 0; i < db->ntables; i++) {
        if(db->tables[i].oid == oid) {
            return &db->tables[i];
        }
    }
	return NULL;
}

static FILE* isFileOpen(UINT oid, BufPool* pool) {
    for (int i = 0; i < numFileEntries; i++) {
        if (pool->files[i].oid == oid) {
			pool->files[i].pin = 1;
            return pool->files[i].fp;
        }
    }
    return NULL;
}

static FILE* addOrReplaceFileEntry(int oid, const char* filename, BufPool* pool) {
	
	FILE* fp = fopen(filename, "r");
	
    log_open_file(oid);
    if (fp == NULL) {
        perror("Failed to open file: %s\n");
        exit(1);
    }

    if (numFileEntries < pool->file_limit) {
        pool->files[numFileEntries].oid = oid;
        pool->files[numFileEntries].fp = fp;
		pool->files[numFileEntries].pin = 1;
        numFileEntries++;
    } else {
        int lruIndex = 0;
		int indexFound = 0;
        time_t lruTime = time(NULL);
        for (int i = 0; i < numFileEntries; i++) {
			if(pool->files[i].pin != 1) {
				time_t currTime = ftell(pool->files[i].fp);
				if (currTime < lruTime) {
					lruTime = currTime;
					lruIndex = i;
				}
				indexFound = 1;
			}
            
        }
		if(indexFound == 0) {
			perror("All file pointers are currently being used");
			exit(1);
		}

        fclose(pool->files[lruIndex].fp);
        log_close_file(pool->files[lruIndex].oid);
        pool->files[lruIndex].oid = oid;
        pool->files[lruIndex].fp = fp;
		pool->files[lruIndex].pin = 1;
    }
	fseek(fp, 0, SEEK_SET);
    return fp;
}

FILE* getFilePointer(int oid, const char* filename, BufPool* pool) {
    FILE* fp = isFileOpen(oid, pool);
    if (fp == NULL) {
        fp = addOrReplaceFileEntry(oid, filename, pool);
        if (fp == NULL) {
            perror("Failed to find new slot in file storage\n");
			exit(1);
        }
    }
	fseek(fp, 0 ,SEEK_SET);
    return fp;
}

void release_file_pointer(int oid, BufPool* pool) {
	for (int i = 0; i < numFileEntries; i++) {
        if (pool->files[i].oid == oid) {
            pool->files[i].pin = 0;
        }
    }
}

int request_page(BufPool* pool, UINT table_oid, UINT64 page_id, Database* db)
{
	int slot;
	slot = pageInPool(pool,table_oid,page_id);
	if (slot < 0) {
		slot = grabNextSlot(pool);
		writePageToBuffer(page_id, table_oid, db, pool, slot);
		pool->bufs[slot]->page_id = page_id;
		pool->bufs[slot]->table_oid = table_oid;
	}
	pool->bufs[slot]->usage_count++;
	pool->bufs[slot]->pin = 1;
	return slot;
}

void release_page(BufPool* pool, UINT table_oid, UINT64 page_id)
{
	int i;
	i = pageInPool(pool, table_oid, page_id);
	assert(i >= 0);
	pool->bufs[i]->pin = 0;
	log_release_page(page_id);
}

void writePageToBuffer(UINT64 page_id, UINT oid, Database* db, BufPool* pool, int slot) {
	
	char table_path[200];
    sprintf(table_path,"%s/%u",db->path,oid);
	FILE* fp = getFilePointer(oid, table_path, pool);

	pool->bufs[slot]->page_id = page_id;
	Table* table = getTable(db, oid);
    pool->bufs[slot]->table_oid = table->oid;

	UINT64 current_page_id = -1;
    int page_exists = 0;
	int current_page = 1;
    
	while(fread(&current_page_id, sizeof(UINT64), 1, fp)) {
        if(current_page_id == page_id) {
            page_exists = 1;
            break;
        }
        fseek(fp, pool->page_size - sizeof(UINT64), SEEK_CUR);
		current_page++;
    }

    if(!page_exists) {
        perror("Page not found in table storage\n");
        exit(1);
    }
    fread(&pool->bufs[slot]->data, pool->page_size - sizeof(UINT64), 1, fp);

	INT tuples_per_page = (pool->page_size-sizeof(UINT64))/sizeof(INT)/table->nattrs;
	if( (current_page)*tuples_per_page > table->ntuples) {
		pool->bufs[slot]->ntuples = table->ntuples - (current_page - 1)*tuples_per_page;
	}
	else {
		pool->bufs[slot]->ntuples = tuples_per_page;
	}
	log_read_page(page_id);

}

