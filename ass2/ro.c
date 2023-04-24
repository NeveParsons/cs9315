#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "ro.h"
#include "db.h"

BufPool* pool;
Conf* my_cf;
Database* my_db;

int linkListLength(struct Node* head);
void freeLinkList(struct Node* head);
void sel_on_page(Conf* my_cf, int nattrs, int ntuples, INT cond_val, UINT idx, BufPool* pool, UINT64 page_num, int slot, struct Node** head);
void join_on_pages(INT outer, INT inner, UINT outerIdx, UINT innerIdx, struct Node** head, INT outerAttrs, INT innerAttrs, int innerFirst);
Table* getTable(const char* table_name);
void hash_join(Table *t1,  UINT Idx1, Table *t2,  UINT Idx2, struct Node** head);
void block_nested_join(Table *innerTable, INT innerPages, UINT innerIdx, Table* outerTable, INT outerPages, UINT outerIdx, struct Node** head, int innerFirst);
void freeHashArray(struct tupleItem** hashArray, int ntuples);

void init(){

    my_cf = get_conf();
    my_db = get_db();
    pool = initBufPool(my_cf->buf_slots, my_cf->page_size, my_cf->file_limit);
    printf("init() is invoked.\n");
}

void release(){
    // optional
    // do some end tasks here.
    // free space to avoid memory leak

    if (pool == NULL) {
        return;
    }
    if (pool->bufs != NULL) {
        for (int i = 0; i < pool->nbufs; i++) {
            if (pool->bufs[i] != NULL) {
                free(pool->bufs[i]);
            }
        }
        free(pool->bufs);
    }
    if (pool->files != NULL) {
        for (int i = 0; i < my_cf->file_limit; i++) {
            if(pool->files[i].fp != NULL) {
                fclose(pool->files[i].fp);
                log_close_file(pool->files[i].oid);
            }
        }
        free(pool->files);
    }

    free(pool);

    printf("release() is invoked.\n");
}

_Table* sel(const UINT idx, const INT cond_val, const char* table_name){
    
    printf("sel() is invoked.\n");

    Table* t = getTable(table_name);
    if(t == NULL) {
        perror("Could not find table in Database");
        exit(1);
    }

    struct Node* head = NULL;

    char table_path[200];
    strcpy(table_path, my_db->path);
    strcat(table_path, "/");
    sprintf(table_path + strlen(table_path), "%u", t->oid);
	FILE* fp = getFilePointer(t->oid, table_path, pool);

    UINT64 current_page_id = -1;
    UINT64 page_num = 0;
    
    while(fread(&current_page_id, sizeof(UINT64), 1, fp)) {
        int slot = request_page(pool, t->oid, current_page_id, my_db);
        sel_on_page(my_cf, t->nattrs, t->ntuples, cond_val, idx, pool, page_num, slot, &head);
        release_page(pool, t->oid, current_page_id);
        page_num++;

        //request page may have caused file pointer to move so set back next page
        fseek(fp, page_num*(pool->page_size), SEEK_SET);
    }
    release_file_pointer(t->oid, pool);
    

    int totalTuples = linkListLength(head);
    _Table* res = malloc(sizeof(_Table)+totalTuples*sizeof(Tuple));

    
    int index = 0;
    struct Node* currentNode = head;
    while(currentNode != NULL) {
        Tuple tuple = malloc(sizeof(INT)*t->nattrs);
        memcpy(tuple, &currentNode->data, t->nattrs*sizeof(INT));
        res->tuples[index] = tuple;
        currentNode = currentNode->next;
        index++;
    }

    res->nattrs = t->nattrs;
    res->ntuples = totalTuples;
    if(head == NULL) {
        res->nattrs = 0;
    }


    freeLinkList(head);

    return res;
}

Table* getTable(const char* table_name) {
    for (int i = 0; i < my_db->ntables; i++) {
        if(!strcmp(my_db->tables[i].name, table_name)) {
            return &my_db->tables[i];
        }
    }
    return NULL;
}

int linkListLength(struct Node* head)
{
    struct Node* curr = head;
    int cnt = 0;

    while (curr != NULL) {
        cnt++;
        curr = curr->next;
    }
    return cnt;
}

void freeLinkList(struct Node* head) {
    struct Node* tmp;
    while (head != NULL) {
        tmp = head;
        head = head->next;
        free(tmp);
    }
}


void sel_on_page(Conf* my_cf, int nattrs, int ntuples, INT cond_val, UINT idx, BufPool* pool, UINT64 page_num, int slot, struct Node** head) {
    
    INT ntuples_per_page = (my_cf->page_size-sizeof(UINT64))/sizeof(INT)/nattrs;
    INT current_table_tuple = ntuples_per_page*page_num;

    struct Node* lastNode = *head;

    while (lastNode != NULL && lastNode->next != NULL){
        lastNode = lastNode->next;
    }
    
    for (UINT i = 0; i < ntuples_per_page; i++){
        if(current_table_tuple == ntuples) {
            break;
        }
        if(pool->bufs[slot]->data[i*nattrs + idx] == cond_val) {
            struct Node* newNode = malloc(sizeof(struct Node)  + nattrs * sizeof(INT));
            memcpy(newNode->data, &pool->bufs[slot]->data[i*nattrs], nattrs*sizeof(INT));
            newNode->next = NULL;
            if(lastNode == NULL) {
                *head = newNode;
            }
            else {
                lastNode->next = newNode;
            }
            lastNode = newNode;
        }
        current_table_tuple++;
    }

}

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name){

    printf("join() is invoked.\n");

    Table* t1 = getTable(table1_name);
    if(t1 == NULL) {
        perror("Could not find table in Database");
        exit(1);
    }
    Table* t2 = getTable(table2_name);
    if(t2 == NULL) {
        perror("Could not find table in Database");
        exit(1);
    }

    struct Node* head = NULL;

    float t1_tuples_per_page =((my_cf->page_size-sizeof(UINT64))/sizeof(INT)/t1->nattrs);
    INT t1_pages = ceil(t1->ntuples/t1_tuples_per_page);

    float t2_tuples_per_page = (my_cf->page_size-sizeof(UINT64))/sizeof(INT)/t2->nattrs;
    INT t2_pages = ceil(t2->ntuples/t2_tuples_per_page);

    if(my_cf->buf_slots < (t1_pages + t2_pages) ) {
        if(t1_pages > t2_pages) {
            block_nested_join(t1, t1_pages, idx1, t2, t2_pages, idx2, &head, 1);
        }
        else {
            block_nested_join(t2, t2_pages, idx2, t1, t1_pages, idx1, &head, 0);
        }
    }
    else {
        hash_join(t1, idx1, t2, idx2, &head);
    }

   
    INT nattrs = t1->nattrs + t2->nattrs;
    if(head == NULL) {
        nattrs = 0;
    }

     int totalTuples = linkListLength(head);;
    _Table* res = malloc(sizeof(_Table)+totalTuples*sizeof(Tuple));

    int index = 0;
    struct Node* currentNode = head;
    while(currentNode != NULL) {
        Tuple tuple = malloc(sizeof(INT)*nattrs);
        res->tuples[index] = tuple;
        memcpy(tuple, &currentNode->data, nattrs*sizeof(INT));
        currentNode = currentNode->next;
        index++;
    }

    freeLinkList(head);

    res->nattrs = nattrs;
    res->ntuples = totalTuples;

    return res;
}


void hash_join(Table *t1,  UINT Idx1, Table *t2,  UINT Idx2, struct Node** head) {

    struct tupleItem* hashArray[t1->ntuples];
    memset(hashArray, 0, t1->ntuples*sizeof(struct tupleItem*));

    struct Node* lastNode = *head;

    while (lastNode != NULL && lastNode->next != NULL){
        lastNode = lastNode->next;
    }

    char table_path1[200];
    strcpy(table_path1, my_db->path);
    strcat(table_path1, "/");
    sprintf(table_path1 + strlen(table_path1), "%u", t1->oid);
	FILE* fp1 = getFilePointer(t1->oid, table_path1, pool);

    UINT page_id = -1;
    int page_num = 0;
    while(fread(&page_id, sizeof(UINT64), 1, fp1)) {
        int slot = request_page(pool, t1->oid, page_id, my_db);
        for(int i = 0; i < pool->bufs[slot]->ntuples; i++) {
            int key = pool->bufs[slot]->data[i*t1->nattrs + Idx1];
            int hashIndex = key % t1->ntuples;
            struct tupleItem *item = malloc(sizeof(struct tupleItem));
            item->slot = slot;
            item->offset = i;
            item->key = key;
            item->next = hashArray[hashIndex];
            hashArray[hashIndex] = item;
        }
        page_num++;
         
        //request page may have caused file pointer to move so set back next page
        fseek(fp1, page_num*(pool->page_size), SEEK_SET);
    }

    char table_path2[200];
    strcpy(table_path2, my_db->path);
    strcat(table_path2, "/");
    sprintf(table_path2 + strlen(table_path2), "%u", t2->oid);
	FILE* fp2 = getFilePointer(t2->oid, table_path2, pool);

    page_id = -1;
    page_num = 0;
    while(fread(&page_id, sizeof(UINT64), 1, fp2)) {
        int slot = request_page(pool, t2->oid, page_id, my_db);
        for(int i = 0; i < pool->bufs[slot]->ntuples; i++) {
            int key = pool->bufs[slot]->data[i*t2->nattrs + Idx2];
            int hashIndex = key % t1->ntuples;
            struct tupleItem* currTuple = hashArray[hashIndex];
            while(currTuple != NULL) {
                if(currTuple->key == key) {
                    struct Node* newNode = malloc(sizeof(struct Node) + (t1->nattrs + t2->nattrs)*sizeof(INT));
                    int k;
                    for(k = 0; k < t1->nattrs; k++) {
                        newNode->data[k] = pool->bufs[currTuple->slot]->data[k + currTuple->offset*t1->nattrs];
                    }
                    for(int u= 0; u< t2->nattrs; u++) {
                        newNode->data[k] = pool->bufs[slot]->data[u + i*t2->nattrs];
                        k++;
                    }   
                    
                    newNode->next = NULL;
                    if(lastNode == NULL) {
                        *head = newNode;
                    }
                    else {
                        lastNode->next = newNode;
                    }
                    lastNode = newNode;
                }
                currTuple = currTuple->next;
                
            }
        }
        release_page(pool, t2->oid, page_id);
        page_num++;
        //request page may have caused file pointer to move so set back next page
        fseek(fp2, page_num*(pool->page_size), SEEK_SET);
    }

    
    release_file_pointer(t2->oid, pool);

    page_id = -1;
    page_num = 1;

    fseek(fp1, 0, SEEK_SET);
    while(fread(&page_id, sizeof(UINT64), 1, fp1)) {
        release_page(pool, t1->oid, page_id);
        fseek(fp1, page_num*(pool->page_size), SEEK_SET);
        page_num++;
    }
    release_file_pointer(t1->oid, pool);

   freeHashArray(hashArray, t1->ntuples);

}

void freeHashArray(struct tupleItem** hashArray, int ntuples) {
    if (hashArray == NULL) {
        return;
    }
    for (int i = 0; i < ntuples; i++) {
        struct tupleItem* currentItem = hashArray[i];
        while (currentItem != NULL) {
            struct tupleItem* nextItem = currentItem->next;
            free(currentItem);
            currentItem = nextItem;
        }
    }
}

void block_nested_join(Table *innerTable, INT innerPages, UINT innerIdx, Table* outerTable, INT outerPages, UINT outerIdx, struct Node** head, int innerFirst) {
    
    char table_path_outer[200];
    strcpy(table_path_outer, my_db->path);
    strcat(table_path_outer, "/");
    sprintf(table_path_outer + strlen(table_path_outer), "%u", outerTable->oid);
	FILE* fpOuter = getFilePointer(outerTable->oid, table_path_outer, pool);

    char table_path_inner[200];
    strcpy(table_path_inner, my_db->path);
    strcat(table_path_inner, "/");
    sprintf(table_path_inner + strlen(table_path_inner), "%u", innerTable->oid);
	FILE* fpInner = getFilePointer(innerTable->oid, table_path_inner, pool);
    
    UINT64 outerPage_id = -1;
    UINT64 innerPage_id = -1;

    int currPage = 0;
    int block = my_cf->buf_slots - 1;
    int pagesNotEvaluated = outerPages;
    
   
    while(pagesNotEvaluated > 0) {
        if(block > pagesNotEvaluated) {
            block = pagesNotEvaluated;
        }
        for(int i = 0; i < block; i++) {
            fseek(fpOuter, currPage*my_cf->page_size ,SEEK_SET);
            fread(&outerPage_id, sizeof(UINT64), 1, fpOuter);
            request_page(pool, outerTable->oid, outerPage_id, my_db);
            pagesNotEvaluated--;
            currPage++;
        }
        int page_num = 1;
        fseek(fpInner, 0 ,SEEK_SET);
        while(fread(&innerPage_id, sizeof(UINT64), 1, fpInner)) {
            int slot = request_page(pool, innerTable->oid, innerPage_id, my_db);
            for(int i = 0; i < my_cf->buf_slots; i ++) {
                if(i == slot || pool->bufs[i]->pin == 0) {
                    continue;
                }
                join_on_pages(i, slot, outerIdx, innerIdx, head, outerTable->nattrs, innerTable->nattrs, innerFirst);
            }
            release_page(pool, innerTable->oid, innerPage_id);
            fseek(fpInner, page_num*my_cf->page_size ,SEEK_SET);
            page_num++;
        }
        currPage = currPage - block;
        fseek(fpOuter, 0 ,SEEK_SET);
        for(int i = 0; i < block; i++) {
            fseek(fpOuter, currPage*my_cf->page_size ,SEEK_SET);
            fread(&outerPage_id, sizeof(UINT64), 1, fpOuter);
            release_page(pool, outerTable->oid, outerPage_id);
            currPage++;
        }
    }

    release_file_pointer(outerTable->oid, pool);
    release_file_pointer(innerTable->oid, pool);
}

void join_on_pages(INT outer, INT inner, UINT outerIdx, UINT innerIdx, struct Node** head, INT outerAttrs, INT innerAttrs, int innerFirst) {
    
    struct Node* lastNode = *head;

    while (lastNode != NULL && lastNode->next != NULL){
        lastNode = lastNode->next;
    }

    for(int i = 0; i < pool->bufs[outer]->ntuples; i++) {
        for(int j = 0; j <  pool->bufs[inner]->ntuples; j++) {
            if(pool->bufs[outer]->data[i*outerAttrs + outerIdx] == pool->bufs[inner]->data[j*innerAttrs + innerIdx] ) {
                struct Node* newNode = malloc(sizeof(struct Node) + (innerAttrs + outerAttrs)*sizeof(INT));
                int k;
                if(innerFirst) {
                    for(k = 0; k < innerAttrs; k++) {
                        newNode->data[k] = pool->bufs[inner]->data[k + j*innerAttrs];
                    }   
                    for(int u = 0; u < outerAttrs; u++) {
                        newNode->data[k] = pool->bufs[outer]->data[u + i*outerAttrs];
                        k++;
                    }
                }
                else {
                    for(k = 0; k < outerAttrs; k++) {
                        newNode->data[k] = pool->bufs[outer]->data[k + i*outerAttrs];
                    }   
                    for(int u = 0; u < innerAttrs; u++) {
                        newNode->data[k] = pool->bufs[inner]->data[u + j*innerAttrs];
                        k++;
                    }
                }
               newNode->next = NULL;
                if(lastNode == NULL) {
                    *head = newNode;
                }
                else {
                    lastNode->next = newNode;
                }
                lastNode = newNode;
                }
        }
    }
}

