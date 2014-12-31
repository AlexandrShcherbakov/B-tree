#include "mydb.h"

#define OK printf("OK\n");
#define FLAG printf("%d\n", flag);
#define OPCACHE 100
#define ISDIRTY(x) (x->info & 1)
#define SETPAGESTATE(x, y) (x->info = x->info | y)
#define DELPAGE(x) (x->info = x->info | 1)
#define ISDELETED(x) (x->info & 2)
#define LOG_TYPE(x) (logflag ? x : EMPTY_RECORD)
#define SLEEP_TIME 100000

static int ops = 0;
static int log_flag = 0;
static int cycle_flag = 1;
static int checkpoint_flag = 0;

//Declaration of functions
void put_in_cache(struct DB *db, struct DBBlock *block, int index);
void add_to_cache(struct DB *db, struct DBBlock *block, int index);
struct cacheList *index_in_cache(struct DB *db, int index);
void insert_element_in_front(struct DB *db, struct cacheList *iter);
void remove_from_cache(struct DB *db, int index, struct cacheList *iter);
void printblock(struct DB *db, int xindex, int height);
int printsize(struct DB *db, int xindex, int height);
void update_ops(struct DB *db);
int write_block_indb(struct DB *db, struct DBBlock *block, int page);
int log_seek(struct Log *log);
struct DBBlock *read_block(struct DB *db, int page);
int delet(struct DB *db, struct DBT *key);
int put(struct DB *db, struct DBT *key, struct DBT *data);
int write_dbinf(struct DB *db);

/*Log API*/
struct Log *log_open() {
    struct Log *log = calloc(sizeof(struct Log), 1);
    log->fd = fopen("log", "a+");
    log_seek(log);
    fseek(log->fd, 0, SEEK_END);
    return log;
}


void log_close(struct Log *log)
{
    fclose(log->fd);
}

int log_write(struct Log *log, struct Record *rec)
{
    fprintf(log->fd, "%d", log->LSN);
    fprintf(log->fd, "%c", rec->type);
    fprintf(log->fd, "%d ", rec->page);
    fprintf(log->fd, "%u ", (unsigned)rec->key->size);
    fwrite(rec->key->data, rec->key->size, 1, log->fd);
    if (rec->type != LOG_DELETE) {
        fprintf(log->fd, " %u ", (unsigned)rec->data->size);
        fwrite(rec->data->data, rec->data->size, 1, log->fd);
    }
    fprintf(log->fd, "\n");
    fflush(log->fd);
    log->LSN++;
    return log->LSN;
}

int log_seek(struct Log *log)
{
    fseek(log->fd, 0, SEEK_END);
    int label = 0;
    fseek(log->fd, -sizeof(label) - 2, SEEK_END);
    while (label != MAGIC_CONST && ftell(log->fd) > 0) {
        fread(&label, sizeof(label), 1, log->fd);
        fseek(log->fd, -sizeof(label) - 1, SEEK_CUR);
        //fseek(log->fd, -1, SEEK_CUR);
        fseek(log->fd, -1, SEEK_CUR);
    }
}

struct Record *log_read(struct Log *log)
{
    struct Record *record = calloc(sizeof(struct Record), 1);
    if (fscanf(log->fd, "%d", &log->LSN) != 1) {
        return NULL;
    }
    record->LSN = log->LSN;
    fscanf(log->fd, "%c", (char *)&record->type);
    fscanf(log->fd, "%d", &record->page);
    record->key = calloc(sizeof(record->key), 1);
    fscanf(log->fd, "%u", (unsigned *)&record->key->size);
    fseek(log->fd, 1, SEEK_CUR);
    record->key->data = calloc(record->key->size, 1);
    fread(record->key->data, record->key->size, 1, log->fd);
    if (record->type != LOG_DELETE) {
        record->data = calloc(sizeof(record->data), 1);
        fscanf(log->fd, "%u", (unsigned *)&record->data->size);
        fseek(log->fd, 1, SEEK_CUR);
        record->data->data = calloc(record->data->size, 1);
        fread(record->data->data, record->data->size, 1, log->fd);
        fseek(log->fd, 1, SEEK_CUR);
    }
    return record;
}

int log_recovery (struct DB *db)
{
    log_flag = 1;
    log_seek(db->log);
    while (1){
        struct Record *r = log_read(db->log);
        if (!r) {
            log_flag = 0;
            return 0;
        }
        struct DBBlock *b;
        if (*db->root != -1) {
            b = read_block(db, r->page);
        }
        //printf("%p\n", b);
        if (*db->root == -1 || !b || b->LSN < r->LSN) {
            if (r->type == LOG_DELETE) {
                delet(db, r->key);
            } else {
                put(db, r->key, r->data);
            }
        }
    }
    db->log->LSN++;
    log_flag = 0;
}

/*Checkpoint functions*/
void checkpoint_realize(struct DB *db)
{
    while (db->cacheListBegin != NULL) {
        if (ISDIRTY(db->cacheListBegin)) {
            write_block_indb(db, &db->cacheContainer[db->cacheListBegin->id], db->cacheIndex[db->cacheListBegin->id]);
        }
        remove_from_cache(db, db->cacheIndex[db->cacheListBegin->id], db->cacheListBegin);
    }
    write_dbinf(db);
    int label = MAGIC_CONST;
    fwrite(&label, sizeof(label), 1, db->log->fd);
    fprintf(db->log->fd, "\n");
    fflush(db->log->fd);
}

void *Checkpoint_func(void *args)
{
    struct DB *db = args;
    while(cycle_flag) {
        usleep(SLEEP_TIME);
        //pthread_mutex_lock(&db->mutex);
        pthread_mutex_lock(&db->mutex);
        checkpoint_flag = 1;
        checkpoint_realize(db);
        checkpoint_flag = 0;
        pthread_mutex_unlock(&db->mutex);
        //printf("%d\n", clock());
    }
}

/*DB API*/
int db_close(struct DB *db) {
	return db->close(db);
}


int db_del(struct DB *db, void *key, size_t key_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	return db->del(db, &keyt);
}


int db_get(struct DB *db, void *key, size_t key_len,
		void **val, size_t *val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {0, 0};
	int rc = db->get(db, &keyt, &valt);
	*val = valt.data;
	*val_len = valt.size;
	return rc;
}


int db_put(struct DB *db, void *key, size_t key_len,
		void *val, size_t val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {
		.data = val,
		.size = val_len
	};
	return db->put(db, &keyt, &valt);
}
/* Functions allocate and free for blocks.*/
int block_alloc(struct DB *db) {
    int page_count = db->conf.db_size / db->conf.chunk_size;
    for (int i = 0; i < page_count; ++i) {
        if (db->pages[i] != CHAR_MAX) {
            int index = 0;
            char pg = db->pages[i];
            while (pg & 1) {
                index++;
                pg >>= 1;
            }
            db->pages[i] = db->pages[i] | (1 << index);
            return index + i * 8;
        }
    }
    return -1;
}

int block_free(struct DB *db, int index)
{
    db->pages[index / 8] = db->pages[index / 8] & ~(1 << (index % 8));
    //remove_from_cache(db, index, index_in_cache(db, index));
    struct cacheList *x = index_in_cache(db, index);
    if (x) {
        DELPAGE(x);
    }
    return 0;
}

int block_is_allocated(struct DB *db, int index) {
    //printf("%d, %d, %d\n", index / 8, (index % 8), (db->pages[index / 8] >> (index % 8)) & 1);
    return (db->pages[index / 8] >> (index % 8)) & 1;
}

int free_var(struct DBBlock *block) {
    if (!block) {
        return -1;
    }
    /*if (!block->isleaf) {
        free(block->childs_pages);
    }*/
    //free(block->keys);
    free(block);
    return 0;
}

/*Functions for write db-information and work with offset of it.*/
int write_dbinf(struct DB *db)
{
    pthread_mutex_lock(&db->mutex);
    lseek(db->f, 0, SEEK_SET);
    write(db->f, db->root, sizeof(*db->root));
    write(db->f, &db->conf, sizeof(db->conf));
    write(db->f, db->pages, db->conf.db_size / db->conf.chunk_size * sizeof(*db->pages));
    pthread_mutex_unlock(&db->mutex);
    return 0;
}

int offset_dbinf(struct DB *db)
{
    return sizeof(*db->root) + sizeof(db->conf) + db->conf.db_size / db->conf.chunk_size * sizeof(*db->pages);
}

/*
Functions for reading and writing blocks
Node template:
    1 byte: isleaf
    4 bytes: size
    4 bytes: LSN
    size * 4 + 4 bytes: links
    other bytes: 4 bytes - size of key
                    size of key bytes - key
                4 bytes - size of data
                    size of data bytes - data
*/
int write_block_indb(struct DB *db, struct DBBlock *block, int page) {
    unsigned hyp_size = sizeof(block->LSN) + sizeof(block->isleaf) + sizeof(block->size) +
                    (block->size + 1) * sizeof(*block->childs_pages);
    for (int i = 0; i < block->size; ++i) {
        hyp_size += block->keys[i].key.size + block->keys[i].data.size +
            sizeof(block->keys[i].key.size) + sizeof(block->keys[i].data.size);
    }
    if (hyp_size > db->conf.chunk_size) {
        fprintf(stderr, "Error: Very big page %d\n", hyp_size);
        return -1;
    }
    lseek(db->f, page * db->conf.chunk_size + offset_dbinf(db), SEEK_SET);//printf("Start write %d in %d\n", tell(db->f), page);
    write(db->f, &block->isleaf, sizeof(block->isleaf));//printf("1 write %d in %d\n", tell(db->f), page);
    //int start = tell(db->f);
    write(db->f, &block->LSN, sizeof(block->LSN));//printf("3 write %d in %d\n", tell(db->f), page);
    //lseek(db->f, start + 4 - tell(db->f), SEEK_CUR);
    write(db->f, &block->size, sizeof(block->size));//printf("2 write %d in %d\n", tell(db->f), page);
    //int start = tell(db->f);
    //lseek(db->f, start + 4 - tell(db->f), SEEK_CUR);
    if (!block->isleaf) {
        write(db->f, block->childs_pages, sizeof(*block->childs_pages) * (block->size + 1));
    }//printf("4 write %d in %d\n", tell(db->f), page);
    for (int i = 0; i < block->size; ++i) {
        struct DBT *field = &block->keys[i].key;
        write(db->f, &field->size, sizeof(field->size));
        write(db->f, field->data, field->size);
        field = &block->keys[i].data;
        write(db->f, &field->size, sizeof(field->size));
        write(db->f, field->data, field->size);//printf("5 + %d write %d in %d\n", i, tell(db->f), page);
    }//printf("End write %d in %d\n", tell(db->f), page);
    return 0;
}

struct DBBlock *read_block(struct DB *db, int page)
{
    if (!checkpoint_flag) pthread_mutex_lock(&db->mutex);
    if (!block_is_allocated(db, page)) {
        if (!checkpoint_flag) pthread_mutex_unlock(&db->mutex);
        return NULL;
    }
    struct cacheList *iter = index_in_cache(db, page);
    if (iter != NULL) {
        insert_element_in_front(db, iter);
        struct DBBlock *tmp = calloc(sizeof(*tmp), 1);
        *tmp = db->cacheContainer[iter->id];
        if (!checkpoint_flag) pthread_mutex_unlock(&db->mutex);
        return tmp;
    }
    struct DBBlock *block = calloc(sizeof(*block), 1);
    lseek(db->f, page * db->conf.chunk_size + offset_dbinf(db), SEEK_SET);//printf("Start read %d in %d\n", tell(db->f), page);
    read(db->f, &block->isleaf, sizeof(block->isleaf));
    read(db->f, &block->LSN, sizeof(block->LSN));
    read(db->f, &block->size, sizeof(block->size));
    if (!block->isleaf) {
        block->childs_pages = calloc(db->t * 2,
                                        sizeof(*block->childs_pages));
        read(db->f, block->childs_pages, (block->size + 1) *
                sizeof(*block->childs_pages));
    }
    block->keys = calloc(db->t * 2 - 1, sizeof(*block->keys));
    for (int i = 0; i < block->size; ++i) {
        struct DBT *field;
        field = &block->keys[i].key;
        read(db->f, &field->size, sizeof(field->size));
        field->data = malloc(field->size);
        read(db->f, field->data, field->size);
        field = &block->keys[i].data;
        read(db->f, &field->size, sizeof(field->size));
        field->data = malloc(field->size);
        read(db->f, field->data, field->size);
    }//printf("End read %d in %d\n", tell(db->f), page);
    if (db->pagesInCache < db->conf.mem_size) {
        add_to_cache(db, block, page);
    } else {
        put_in_cache(db, block, page);
    }
    update_ops(db);
    if (!checkpoint_flag) pthread_mutex_unlock(&db->mutex);
    return block;
}

//***Functions for cache***
//Functions for list
void update_ops(struct DB *db)
{
    ops++;
    if (ops == OPCACHE) {
        struct cacheList *it = db->cacheListBegin;
        for (; it != NULL; it = it->next) {
            if (ISDIRTY(it)) {
                write_block_indb(db, db->cacheContainer + it->id, db->cacheIndex[it->id]);
                SETPAGESTATE(it, 0);
            }
        }
        ops = 0;
    }
}


void insert_element_in_front(struct DB *db, struct cacheList *iter)
{
    if (iter == db->cacheListBegin) {
        return;
    }
    iter->perv->next = iter->next;
    if (iter->next != NULL) {
        iter->next->perv = iter->perv;
    } else {
        db->cacheListEnd = iter->perv;
    }
    iter->next = db->cacheListBegin;
    iter->perv = NULL;
    db->cacheListBegin->perv = iter;
    db->cacheListBegin = iter;
}

struct cacheList *index_in_cache(struct DB *db, int index)
{
    struct cacheList *iter = db->cacheListBegin;
    for (; iter != NULL; iter = iter->next) {
        if (db->cacheIndex[iter->id] == index) {
            return iter;
        }
    }
    return iter;
}

void add_to_cache(struct DB *db, struct DBBlock *block, int index)
{
    db->cacheContainer[db->pagesInCache] = *block;
    db->cacheIndex[db->pagesInCache] = index;
    struct cacheList *newBegin = calloc(sizeof(*newBegin), 1);
    newBegin->id = db->pagesInCache;
    newBegin->next = db->cacheListBegin;
    newBegin->perv = NULL;
    if (db->cacheListBegin != NULL) {
        db->cacheListBegin->perv = newBegin;
    } else {
        db->cacheListEnd = newBegin;
    }
    db->cacheListBegin = newBegin;
    db->pagesInCache++;
}

void update_in_cache(struct DB *db, struct DBBlock *block, struct cacheList *iter)
{
    db->cacheContainer[iter->id] = *block;
}

void work_after_miss(struct DB *db, struct cacheList *it)
{
    if (ISDELETED(it)) {
        remove_from_cache(db, db->cacheIndex[it->id], it);
    } else {
        if (ISDIRTY(it)) {
            write_block_indb(db, db->cacheContainer + it->id, db->cacheIndex[it->id]);
        }
    }
}

void put_in_cache(struct DB *db, struct DBBlock *block, int index)
{
    db->cacheListEnd->perv->next = NULL;
    struct cacheList *oldEnd = db->cacheListEnd;
    work_after_miss(db, oldEnd);
    db->cacheListEnd = db->cacheListEnd->perv;
    db->cacheContainer[oldEnd->id] = *block;
    db->cacheIndex[oldEnd->id] = index;
    oldEnd->perv = NULL;
    oldEnd->next = db->cacheListBegin;
    db->cacheListBegin->perv = oldEnd;
    db->cacheListBegin = oldEnd;
}

void remove_from_cache(struct DB *db, int index, struct cacheList *iter)
{
    db->pagesInCache--;
    struct cacheList *it;
    int i = 0;
    for (; i < db->pagesInCache && db->cacheIndex[i] != index; ++i) {}
    db->cacheIndex[i] = db->cacheIndex[db->pagesInCache];
    db->cacheContainer[i] = db->cacheContainer[db->pagesInCache];
    it = db->cacheListBegin;
    while(it->id != db->pagesInCache) {
        it = it->next;
    }
    it->id = i;
    if (iter != db->cacheListBegin) {
        iter->perv->next = iter->next;
    } else {
        db->cacheListBegin = iter->next;
    }
    if (iter != db->cacheListEnd) {
        iter->next->perv = iter->perv;
    } else {
        db->cacheListEnd = iter->perv;
    }
}

int write_block(struct DB *db, struct DBBlock *block, int index, enum RECORD_TYPE tp, struct DBT *key, struct DBT *data)
{
    if (!checkpoint_flag) pthread_mutex_lock(&db->mutex);
    if (tp != EMPTY_RECORD && !log_flag) {
        struct Record r;
        r.type = tp;
        r.page = index;
        r.key = key;
        if (r.type != LOG_DELETE) {
            r.data = data;
        }
        block->LSN = log_write(db->log, &r);
    }
    struct cacheList *iter = index_in_cache(db, index);
    if (iter != NULL) {
        update_in_cache(db, block, iter);
    } else {
        if (db->pagesInCache < db->conf.mem_size) {
            add_to_cache(db, block, index);
        } else {
            put_in_cache(db, block, index);
        }
    }
    SETPAGESTATE(index_in_cache(db, index), 1);
    update_ops(db);
    if (!checkpoint_flag) pthread_mutex_unlock(&db->mutex);
    return 0;
}

/*Comparator for keys*/
int keycmp(struct DBT *a, struct DBT *b)
{
    int s = a->size > b->size ? b->size : a->size;
    int res = memcmp(a->data, b->data, s);
    if (res == 0) {
        return a->size - b->size;
    }
    return res;
}

/*Function put key*/
int split_child(struct DB *db, int xindex, int yindex, int iter)
{
    int zindex = block_alloc(db);
    if (zindex == -1) {
        return -1;
    }
    struct DBBlock *y = read_block(db, yindex);
    struct DBBlock *z = malloc(sizeof(*z));
    z->isleaf = y->isleaf;
    z->size = db->t - 1;
    z->keys = malloc(sizeof(*z->keys) * (2 * db->t - 1));
    z->childs_pages = malloc(sizeof(*z->childs_pages) * 2 * db->t);
    z->LSN = -1;
    for (int i = 0; i < db->t - 1; ++i) {
        z->keys[i] = y->keys[i + db->t];
    }
    if (!z->isleaf) {
        for (int i = 0; i < db->t; ++i) {
            z->childs_pages[i] = y->childs_pages[i + db->t];
        }
    }
    y->size = db->t - 1;
    struct DBBlock *x;
    if (xindex != yindex) {
        x = read_block(db, xindex);
        if (x->size == db->t * 2 - 1) {
            xindex = split_child(db, xindex, xindex, 0);
            free_var(x);
            x = read_block(db, xindex);
        }
        for (int i = x->size + 1; i > iter + 1; --i) {
            x->childs_pages[i] = x->childs_pages[i - 1];
        }
        x->childs_pages[iter + 1] = zindex;
        for (int i = x->size; i > iter; --i) {
            x->keys[i] = x->keys[i - 1];
        }
        x->keys[iter] = y->keys[db->t - 1];
        x->size++;
    } else {
        xindex = block_alloc(db);
        if (xindex == -1) {
            free_var(y);
            free_var(z);
            return -1;
        }
        x = malloc(sizeof(*x));
        x->isleaf = 0;
        x->keys = malloc(sizeof(*x->keys) * (2 * db->t - 1));
        x->childs_pages = malloc(sizeof(*x->childs_pages) * 2 * db->t);
        x->size = 1;
        x->childs_pages[0] = yindex;
        x->childs_pages[1] = zindex;
        x->keys[0] = y->keys[db->t - 1];
        x->LSN = -1;
    }
    int result = xindex;
    if (write_block(db, x, xindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    if (write_block(db, y, yindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    if (write_block(db, z, zindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    free_var(x);
    free_var(y);
    free_var(z);
    return result;
}

int put_node(struct DB *db, int xindex, struct DBT *key, struct DBT *data)
{
    //printblock(db, xindex, 0);getchar();
    struct DBBlock *x;
    enum RECORD_TYPE logtype = LOG_INSERT;
    if (xindex == -1) {
        xindex = block_alloc(db);
        x = malloc(sizeof(*x));
        x->size = 0;
        x->isleaf = 1;
        x->keys = calloc(sizeof(*x->keys), 2 * db->t - 1);
        x->childs_pages = calloc(sizeof(*x->childs_pages), 2 * db->t);
        x->LSN = -1;
    } else {
        x = read_block(db, xindex);
    }
    if (x->size < 0) {
        exit(0);
    }
    if (x->size == db->t * 2 - 1 && xindex == *db->root) {
        free_var(x);
        *db->root = xindex = split_child(db, xindex, xindex, 0);
        x = read_block(db, xindex);
    }
    if (*db->root == -1) {
        *db->root = xindex;
    }
    int i;
    for (i = x->size - 1; i >= 0 && keycmp(key, &x->keys[i].key) < 0; --i) {}
    if (i >= 0 && keycmp(key, &x->keys[i].key) == 0) {
        logtype = LOG_REPLACE;
        x->keys[i].data = *data;
    } else {
        i = x->size;
        if (x->isleaf) {
            while (i > 0 && keycmp(key, &x->keys[i - 1].key) < 0) {
                x->keys[i] = x->keys[i - 1];
                i--;
            }
            x->keys[i].key = *key;
            x->keys[i].data = *data;
            x->size++;
        } else {
            i--;
            while (i >= 0 && keycmp(key, &x->keys[i].key) < 0) {
                i--;
            }
            i++;
            struct DBBlock *y = read_block(db, x->childs_pages[i]);
            if (y->size == 2 * db->t - 1) {
                xindex = split_child(db, xindex, x->childs_pages[i], i);
                free_var(x);
                x = read_block(db, xindex);
                if (keycmp(key, &x->keys[i].key) > 0) {
                    i++;
                }
            }
            free_var(y);
            put_node(db, x->childs_pages[i], key, data);
            logtype = EMPTY_RECORD;
        }
    }
    int result = xindex;
    if (write_block(db, x, xindex, logtype, key, data) == -1) result = -1;
    free_var(x);
    return result;
}

int put(struct DB *db, struct DBT *key, struct DBT *data) {
    //pthread_mutex_lock(&db->mutex);
    int res = put_node(db, *db->root, key, data);
    //pthread_mutex_unlock(&db->mutex);
    return res;
}

/*Delete block*/
int getmax(struct DB *db, int xindex, struct DBT *key) {
    if (xindex == -1) {
        return -1;
    }
    struct DBBlock *x = read_block(db, xindex);
    if (x->isleaf) {
        *key = x->keys[x->size - 1].key;
        return 0;
    }
    return getmax(db, x->childs_pages[x->size], key);
}

int getmin(struct DB *db, int xindex, struct DBT *key) {
    if (xindex == -1) {
        return -1;
    }
    struct DBBlock *x = read_block(db, xindex);
    if (x->isleaf) {
        *key = x->keys[0].key;
        return 0;
    }
    return getmin(db, x->childs_pages[0], key);
}

int delblock(struct DB *db, int xindex, struct DBT key, struct DBKey *node, int logflag);

int rebuildleft(struct DB *db, int xindex, int i) {
    struct DBBlock *x = read_block(db, xindex);
    int yindex = x->childs_pages[i];
    struct DBBlock *y = read_block(db, yindex);
    int zlindex = x->childs_pages[i - 1];
    struct DBBlock *zl = read_block(db, zlindex);
    for (int j = y->size - 1; j >= 0; --j) {
        y->keys[j + 1] = y->keys[j];
    }
    y->keys[0] = x->keys[i - 1];
    x->keys[i - 1] = zl->keys[zl->size - 1];
    if (!y->isleaf) {
        for (int j = y->size; j >= 0; --j) {
            y->childs_pages[j + 1] = y->childs_pages[j];
        }
        y->childs_pages[0] = zl->childs_pages[zl->size];
    }
    zl->size--;
    y->size++;
    int result = 0;
    if (write_block(db, x, xindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    if (write_block(db, y, yindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    if (write_block(db, zl, zlindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    free_var(x);
    free_var(y);
    free_var(zl);
    return result;
}

int rebuildright(struct DB *db, int xindex, int i) {
    struct DBBlock *x = read_block(db, xindex);
    int yindex = x->childs_pages[i];
    struct DBBlock *y = read_block(db, yindex);
    int zrindex = x->childs_pages[i + 1];
    struct DBBlock *zr = read_block(db, zrindex);
    y->keys[y->size] = x->keys[i];
    x->keys[i] = zr->keys[0];
    for (int j = 1; j < zr->size; ++j) {
        zr->keys[j - 1] = zr->keys[j];
    }
    if (!y->isleaf) {
        y->childs_pages[y->size + 1] = zr->childs_pages[0];
        for (int j = 1; j <= zr->size; ++j) {
            zr->childs_pages[j - 1] = zr->childs_pages[j];
        }
    }
    zr->size--;
    y->size++;
    int result = -1;
    if (write_block(db, x, xindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    if (write_block(db, y, yindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    if (write_block(db, zr, zrindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    free_var(x);
    free_var(y);
    free_var(zr);
    return result;
}

int mergesmallnodes(struct DB *db, int xindex, int iter) {
    struct DBBlock *x = read_block(db, xindex);
    int yindex = x->childs_pages[iter];
    struct DBBlock *y = read_block(db, yindex);
    int zindex = x->childs_pages[iter + 1];
    struct DBBlock *z = read_block(db, zindex);
    for (int i = 0; i < z->size; ++i) {
        y->keys[i + y->size + 1] = z->keys[i];
    }
    y->keys[y->size] = x->keys[iter];
    if (!y->isleaf) {
        for (int i = 0; i <= z->size; ++i) {
            y->childs_pages[i + y->size + 1] = z->childs_pages[i];
        }
    }
    x->size--;
    for (int i = iter; i < x->size; ++i) {
        x->keys[i] = x->keys[i + 1];
    }
    for (int i = iter + 1; i <= x->size; ++i) {
        x->childs_pages[i] = x->childs_pages[i + 1];
    }
    y->size = db->t * 2 - 1;
    //printblock(db, yindex, 0);
    block_free(db, zindex);
    int result = xindex;
    if (x->size == 0) {
        block_free(db, xindex);
        xindex = yindex;
        result = yindex;
    } else {
        if (write_block(db, x, xindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    }
    if (write_block(db, y, yindex, EMPTY_RECORD, NULL, NULL) == -1) result = -1;
    free_var(x);
    free_var(y);
    free_var(z);
    return result;
}

int delblock(struct DB *db, int xindex, struct DBT key, struct DBKey *node, int logflag) {
    if (xindex == -1) {
        return -1;
    }
    struct DBBlock *x = read_block(db, xindex);
    int i;
    //Part 1
    for (i = 0; i < x->size && keycmp(&key, &x->keys[i].key) > 0; ++i) {};
    if (i < x->size && keycmp(&key, &x->keys[i].key) == 0) {
        if (x->isleaf) {
            *node = x->keys[i];
            x->size--;
            for (; i < x->size; ++i) {
                x->keys[i] = x->keys[i + 1];
            }
            write_block(db, x, xindex, LOG_TYPE(LOG_DELETE), &key, NULL);
            free_var(x);
            return xindex;
        }
        int yindex = x->childs_pages[i];
        struct DBBlock *y = read_block(db, yindex);
        if (y->size >= db->t) {
            struct DBKey tmpnd;
            struct DBT tmpk;
            getmax(db, yindex, &tmpk);
            x->childs_pages[i] = delblock(db, yindex, tmpk, &tmpnd, 0);
            *node = x->keys[i];
            x->keys[i] = tmpnd;
            write_block(db, x, xindex, LOG_TYPE(LOG_DELETE), &key, NULL);
            free_var(x);
            free_var(y);
            return xindex;
        }
        int zindex = x->childs_pages[i + 1];
        struct DBBlock *z = read_block(db, zindex);
        if (z->size >= db->t) {
            struct DBKey tmpnd;
            struct DBT tmpk;
            getmin(db, zindex, &tmpk);
            x->childs_pages[i + 1] = delblock(db, zindex, tmpk, &tmpnd, 0);
            *node = x->keys[i];
            x->keys[i] = tmpnd;
            write_block(db, x, xindex, LOG_TYPE(LOG_DELETE), &key, NULL);
            free_var(x);
            free_var(z);
            free_var(y);
            return xindex;
        }
        free_var(x);
        free_var(y);
        free_var(z);
        xindex = mergesmallnodes(db, xindex, i);
        xindex = delblock(db, xindex, key, node, logflag);
        return xindex;
    }
    if (x->isleaf) {
        free_var(x);
        node->key.size = -1;
        return xindex;
    }
    int yindex = x->childs_pages[i];
    struct DBBlock *y = read_block(db, yindex);
    if (y->size >= db->t) {
        x->childs_pages[i] = delblock(db, yindex, key, node, logflag);
        write_block(db, x, xindex, EMPTY_RECORD, &key, NULL);
        free_var(y);
        free_var(x);
        return xindex;
    }
    int zlindex, zrindex;
    struct DBBlock *zl, *zr;
    if (i > 0) {
        zlindex = x->childs_pages[i - 1];
        zl = read_block(db, zlindex);
        if (zl->size >= db->t) {
            rebuildleft(db, xindex, i);
            free_var(x);
            x = read_block(db, xindex);
            x->childs_pages[i] = delblock(db, yindex, key, node, logflag);
            write_block(db, x, xindex, LOG_TYPE(LOG_DELETE), &key, NULL);
            free_var(x);
            free_var(zl);
            free_var(y);
            return xindex;
        }
        if (i < x->size) {
            zrindex = x->childs_pages[i + 1];
            zr = read_block(db, zrindex);
            if (zr->size >= db->t) {
                rebuildright(db, xindex, i);
                free_var(x);
                x = read_block(db, xindex);
                x->childs_pages[i] = delblock(db, yindex, key, node, logflag);
                write_block(db, x, xindex, LOG_TYPE(LOG_DELETE), &key, NULL);
                free_var(x);
                free_var(zr);
                free_var(y);
                return xindex;
            }
        }
        free_var(x);
        free_var(y);
        free_var(zl);
        xindex = mergesmallnodes(db, xindex, i - 1);
        //printblock(db, xindex, 0);
        xindex = delblock(db, xindex, key, node, logflag);
        return xindex;
    }
    if (i < x->size) {
        zrindex = x->childs_pages[i + 1];
        zr = read_block(db, zrindex);
        if (zr->size >= db->t) {
            rebuildright(db, xindex, i);
            free_var(x);
            x = read_block(db, xindex);
            x->childs_pages[i] = delblock(db, yindex, key, node, logflag);
            write_block(db, x, xindex, LOG_TYPE(LOG_DELETE), &key, NULL);
            free_var(x);
            free_var(zr);
            free_var(y);
            return xindex;
        }
        free_var(x);
        free_var(y);
        free_var(zr);
        xindex = mergesmallnodes(db, xindex, i);
        xindex = delblock(db, xindex, key, node, logflag);
        return xindex;
    }
    node->key.size = -1;
    return -1;
}

int delet(struct DB *db, struct DBT *key) {
    //pthread_mutex_lock(&db->mutex);
    struct DBKey *data = calloc(sizeof(*data), 1);
    *db->root = delblock(db, *db->root, *key, data, 1);
    //pthread_mutex_unlock(&db->mutex);
    if (data->key.size == -1) {
        return 1;
    }
    return 0;
}

/*Function of get data by key*/
int getfromblock(struct DB *db, int xindex, struct DBT *key, struct DBT *data)
{
    if (*db->root == -1) {
        return -1;
    }
    struct DBBlock *block = read_block(db, xindex);
    int l = 0, r = block->size;
    while (r - l > 1) {
        int m = (l + r) / 2;
        if (keycmp(key, &block->keys[m].key) >= 0) {
            l = m;
        } else {
            r = m;
        }
    }
    if (keycmp(key, &block->keys[l].key) == 0) {
        data->data = block->keys[l].data.data;
        data->size = block->keys[l].data.size;
        free_var(block);
        return 0;
    }
    if (!block->isleaf) {
        if (keycmp(key, &block->keys[l].key) < 0) {
            int result = getfromblock(db, block->childs_pages[l], key, data);
            free_var(block);
            return result;
        }
        int result = getfromblock(db, block->childs_pages[r], key, data);
        free_var(block);
        return result;
    }
    free_var(block);
    return -1;
}

int get(struct DB *db, struct DBT *key, struct DBT *data)
{
    //pthread_mutex_lock(&db->mutex);
    int res = getfromblock(db, *db->root, key, data);
    //pthread_mutex_unlock(&db->mutex);
    return res;
}

/*Close DB*/
int close_db(struct DB *db) {
    //pthread_cancel(&db->logthread);
    //cycle_flag = 0;
    //pthread_join(&db->logthread, NULL);
    pthread_kill(&db->logthread, SIGSEGV);
    pthread_kill(&db->logthread, SIGSEGV);
    checkpoint_flag = 1;exit(0);
    checkpoint_realize(db);
    write_dbinf(db);
    close(db->f);
    free(db->pages);
    free(db->root);
    free(db);
    return 0;
}

/*Create and open*/
struct DB *dbcreate(const char *file, struct DBC conf)
{
    char buf[1024];
    sprintf(buf, "%s/db", file);
    struct DB *res = calloc(sizeof(*res), 1);
    res->f = open(buf, O_RDWR | O_CREAT | O_TRUNC, 0666);
    //res->f = fopen(file, "w+");
    res->conf = conf;
    //res->mem_size = 100;
    res->pages = calloc(conf.db_size / conf.chunk_size, 1);
    res->t = 25;
    res->root = malloc(sizeof(*res->root));
    *res->root = -1;
    res->put = &put;
    res->get = &get;
    res->del = &delet;
    res->close = &close_db;
    res->pagesInCache = 0;
    res->cacheContainer = calloc(sizeof(*res->cacheContainer), res->conf.mem_size);
    res->cacheIndex = calloc(sizeof(int), res->conf.mem_size);
    res->cacheListBegin = NULL;
    res->cacheListEnd = res->cacheListBegin;
    res->log = log_open();
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_RECURSIVE_NP);
    pthread_mutex_init(&res->mutex, NULL);
    pthread_mutexattr_destroy(&mattr);
    pthread_create(&res->logthread, NULL, Checkpoint_func, res);
    write_dbinf(res);
    return res;
}

struct DB *dbopen(const char *file, struct DBC conf) {
    struct DB *res = malloc(sizeof(*res));
    char buf[1024];
    sprintf(buf, "%s/db", file);
    res->f = open(buf, O_RDWR, 0666);
    lseek(res->f, 0, SEEK_SET);
    res->root = calloc(sizeof(*res->root), 1);
    read(res->f, res->root, sizeof(*res->root));
    read(res->f, &res->conf, sizeof(res->conf));
    res->pages = calloc(res->conf.db_size / res->conf.chunk_size, 1);
    read(res->f, res->pages, res->conf.db_size / res->conf.chunk_size * sizeof(*res->pages));
    res->t = 25;
    res->put = &put;
    res->get = &get;
    res->del = &delet;
    res->close = &close_db;
    res->pagesInCache = 0;
    res->cacheContainer = calloc(sizeof(*res->cacheContainer), res->conf.mem_size);
    res->cacheIndex = calloc(sizeof(int), res->conf.mem_size);
    res->cacheListBegin = NULL;
    res->cacheListEnd = res->cacheListBegin;
    res->log = log_open();
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_RECURSIVE_NP);
    pthread_mutex_init(&res->mutex, NULL);
    pthread_mutexattr_destroy(&mattr);
    log_recovery(res);
    pthread_create(&res->logthread, NULL, Checkpoint_func, res);
    printblock(res, *res->root, 0);
    return res;
}

int printsize(struct DB *db, int xindex, int height)
{
    if (xindex == -1) return 0 ;
    struct DBBlock *x = read_block(db, xindex);
    int res = x->size;
    if (!x->isleaf) {
        for (int i = 0; i <= x->size; ++i) {
            res += printsize(db, x->childs_pages[i], height + 1);
        }
    }
    if (height == 0)  {
        for (int i = 0; i < height; ++i) {
            printf("-");
        }
        printf("size: %d\n", res);
    }
    return res;
}

/*Print tree*/
void printblock(struct DB *db, int xindex, int height) {
    if (xindex == -1) {
        printf("Broken block!\n");
        return;
    }//printf("page number: %d\n", xindex);
    struct DBBlock *x = read_block(db, xindex);
    //printf("block pointer: %p\n", x);
    for (int i = 0; i < height; ++i) {
        printf("-");
    }
    printf("isleaf: ");
    if (x->isleaf) {
        printf("true\n");
    } else {
        printf("false\n");
    }
    for (int i = 0; i < height; ++i) {
        printf("-");
    }
    printf("size: %d\n", x->size);
    for (int i = 0; i < height; ++i) {
        printf("-");
    }
    printf("keys:\n");
    for (int j = 0; j < x->size; ++j) {
        for (int i = 0; i < height; ++i) {
            printf("-");
        }
        printf("key: ");
        fwrite(x->keys[j].key.data, x->keys[j].key.size, 1, stdout);
        printf("\n");
        for (int i = 0; i < height; ++i) {
            printf("-");
        }
        printf("data: ");
        fwrite(x->keys[j].data.data, x->keys[j].data.size, 1, stdout);
        printf("\n");
    }
    if (!x->isleaf) {
        for (int i = 0; i <= x->size; ++i) {
            printblock(db, x->childs_pages[i], height + 1);
        }
    }
    printf("\n");
}

/*int main(void)
{
    struct DBC conf;
    conf.chunk_size = 64 * 1024;
    conf.db_size = 512 * 1024 * 1024;
    conf.mem_size = 2;
    struct DB *db;
    db = dbcreate("db", conf);
    db_put(db, "ololo", 5, "sos", 3);
    db_put(db, "lol", 3, "so slow", 7);
    db_put(db, "key 1", 5, "data 1", 6);
    db_put(db, "sos", 3, "may be", 6);
    db_put(db, "sneg", 4, "cold", 4);
    db_put(db, "300", 3, "tractor", 7);
    db_put(db, "kolhoz", 6, "galin", 5);
    db_put(db, "devil", 5, "evil", 4);
    db_put(db, "angel", 5, "good", 4);
    db_put(db, "uk", 2, "rf", 2);
    int *p = 0;
    //*p = 300;
    //db = dbopen("db", conf);
    db_put(db, "size", 4, "toobig", 6);
    char *s[] = {"ololo", "lol", "key 1", "sos", "sneg", "300", "kolhoz",
    "devil", "angel", "uk", "size"};
    for (int i = 0; i < 30; ++i) {
        int k = rand() % 11;
        char *str = malloc(300);
        int len;
        db_get(db, s[k], strlen(s[k]), (void **)&str, &len);
        printf("%s: ", s[k]);
        puts(str);
    }
    db_del(db, "size", 4);
    db_del(db, "uk", 2);
    for (int i = 0; i < 300; ++i) {
        int k = rand() % 9;
        char *str = malloc(300);
        int len;
        db_get(db, s[k], strlen(s[k]), (void **)&str, &len);
        printf("%s: ", s[k]);
        puts(str);
    }
    db_del(db, "lol", 3);
    db_del(db, "devil", 5);
    db_del(db, "ololo", 5);
    db_del(db, "key 1", 5);
    db_del(db, "sos", 3);
    db_put(db, "uk", 2, "rf", 2);
    db_del(db, "angel", 5);
    db_del(db, "sneg", 4);
    db_del(db, "kolhoz", 6);
    db_del(db, "300", 3);
    printblock(db, *db->root, 0);
    db_close(db);
    return 0;
}*/
