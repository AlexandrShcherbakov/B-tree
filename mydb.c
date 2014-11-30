#include "mydb.h"

#define OK printf("OK\n");
#define FLAG printf("%d\n", flag);
#define OPCACHE 3
#define ISDIRTY(x) (x->info & 1)
#define SETPAGESTATE(x, y) (x->info = x->info | y)
#define DELPAGE(x) (x->info = x->info | 1)
#define ISDELETED(x) (x->info & 2)

static int flag = 0;
static int ops = 0;

//Declaration of functions
void put_in_cache(struct DB *db, struct DBBlock *block, int index);
void add_to_cache(struct DB *db, struct DBBlock *block, int index);
struct cacheList *index_in_cache(struct DB *db, int index);
void insert_element_in_front(struct DB *db, struct cacheList *iter);
void remove_from_cache(struct DB *db, int index, struct cacheList *iter );
void printblock(struct DB *db, int xindex, int height);
int printsize(struct DB *db, int xindex, int height);

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

int block_free(struct DB *db, int index) {
    db->pages[index / 8] = db->pages[index] & ~(1 << (index % 8));
    //remove_from_cache(db, index, index_in_cache(db, index));
    DELPAGE(index_in_cache(db, index));
    return 0;
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
    fseek(db->f, 0, SEEK_SET);
    fwrite(db->root, sizeof(*db->root), 1, db->f);
    fwrite(&db->conf, sizeof(db->conf), 1, db->f);
    fwrite(db->pages, sizeof(*db->pages), db->conf.db_size / db->conf.chunk_size, db->f);
    return 0;
}

int offset_dbinf(struct DB *db)
{
    return sizeof(*db->root) + sizeof(db->conf) + sizeof(*db->pages) * db->conf.db_size / db->conf.chunk_size;
}

/*
Functions for reading and writing blocks
Node template:
    1 byte: isleaf
    4 bytes: size
    size * 4 + 4 bytes: links
    other bytes: 4 bytes - size of key
                    size of key bytes - key
                4 bytes - size of data
                    size of data bytes - data
*/
int write_block_indb(struct DB *db, struct DBBlock *block, int page) {
    unsigned hyp_size = sizeof(block->isleaf) + sizeof(block->size) +
                    (block->size + 1) * sizeof(*block->childs_pages);
    for (int i = 0; i < block->size; ++i) {
        hyp_size += block->keys[i].key.size + block->keys[i].data.size +
            sizeof(block->keys[i].key.size) + sizeof(block->keys[i].data.size);
    }
    if (hyp_size > db->conf.chunk_size) {
        return -1;
    }
    fseek(db->f, page * db->conf.chunk_size + offset_dbinf(db), SEEK_SET);
    fwrite(&block->isleaf, sizeof(block->isleaf), 1, db->f);
    fwrite(&block->size, sizeof(block->size), 1, db->f);
    if (!block->isleaf) {
        fwrite(block->childs_pages, sizeof(*block->childs_pages),
                block->size + 1,db->f);
    }
    for (int i = 0; i < block->size; ++i) {
        struct DBT *field = &block->keys[i].key;
        fwrite(&field->size, sizeof(field->size), 1, db->f);
        fwrite(field->data, field->size, 1, db->f);
        field = &block->keys[i].data;
        fwrite(&field->size, sizeof(field->size), 1, db->f);
        fwrite(field->data, field->size, 1, db->f);
    }
    return 0;
}

struct DBBlock *read_block(struct DB *db, int page)
{
    struct cacheList *iter = index_in_cache(db, page);
    if (iter != NULL) {
        insert_element_in_front(db, iter);
        struct DBBlock *tmp = calloc(sizeof(*tmp), 1);
        *tmp = db->cacheContainer[iter->id];
        return tmp;
    }
    struct DBBlock *block = calloc(sizeof(*block), 1);
    fseek(db->f, page * db->conf.chunk_size + offset_dbinf(db), SEEK_SET);
    fread(&block->isleaf, sizeof(block->isleaf), 1, db->f);
    fread(&block->size, sizeof(block->size), 1, db->f);
    if (!block->isleaf) {
        block->childs_pages = calloc(db->t * 2,
                                        sizeof(*block->childs_pages));
        fread(block->childs_pages, block->size + 1,
                sizeof(*block->childs_pages), db->f);
    }
    block->keys = calloc(db->t * 2 - 1, sizeof(*block->keys));
    for (int i = 0; i < block->size; ++i) {
        struct DBT *field;
        field = &block->keys[i].key;
        fread(&field->size, sizeof(field->size), 1, db->f);
        field->data = malloc(field->size);
        fread(field->data, field->size, 1, db->f);
        field = &block->keys[i].data;
        fread(&field->size, sizeof(field->size), 1, db->f);
        field->data = malloc(field->size);
        fread(field->data, field->size, 1, db->f);
    }
    if (db->pagesInCache < db->mem_size) {
        add_to_cache(db, block, page);
    } else {
        put_in_cache(db, block, page);
    }
    update_ops(db);
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

int write_block(struct DB *db, struct DBBlock *block, int index)
{
    struct cacheList *iter = index_in_cache(db, index);
    if (iter != NULL) {
        update_in_cache(db, block, iter);
    } else {
        if (db->pagesInCache < db->mem_size) {
            add_to_cache(db, block, index);
        } else {
            put_in_cache(db, block, index);
        }
    }
    SETPAGESTATE(index_in_cache(db, index), 1);
    update_ops(db);
    //write_block_indb(db, block, index);
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
    }
    int result = xindex;
    if (write_block(db, x, xindex) == -1) result = -1;
    if (write_block(db, y, yindex) == -1) result = -1;
    if (write_block(db, z, zindex) == -1) result = -1;
    free_var(x);
    free_var(y);
    free_var(z);
    return result;
}

int put_node(struct DB *db, int xindex, struct DBT *key, struct DBT *data)
{
    struct DBBlock *x;
    if (xindex == -1) {
        xindex = block_alloc(db);
        x = malloc(sizeof(*x));
        x->size = 0;
        x->isleaf = 1;
        x->keys = calloc(sizeof(*x->keys), 2 * db->t - 1);
        x->childs_pages = calloc(sizeof(*x->childs_pages), 2 * db->t);
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
        }
    }
    int result = xindex;
    if (write_block(db, x, xindex) == -1) result = -1;
    free_var(x);
    return result;
}

int put(struct DB *db, struct DBT *key, struct DBT *data) {
    int res = put_node(db, *db->root, key, data);
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

int delblock(struct DB *db, int xindex, struct DBT key, struct DBKey *node);

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
    if (write_block(db, x, xindex) == -1) result = -1;
    if (write_block(db, y, yindex) == -1) result = -1;
    if (write_block(db, zl, zlindex) == -1) result = -1;
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
    if (write_block(db, x, xindex) == -1) result = -1;
    if (write_block(db, y, yindex) == -1) result = -1;
    if (write_block(db, zr, zrindex) == -1) result = -1;
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
    block_free(db, zindex);
    int result = xindex;
    if (x->size == 0) {
        block_free(db, xindex);
        xindex = yindex;
        result = yindex;
    } else {
        if (write_block(db, x, xindex) == -1) result = -1;
    }
    if (write_block(db, y, yindex) == -1) result = -1;
    free_var(x);
    free_var(y);
    free_var(z);
    return result;
}

int delblock(struct DB *db, int xindex, struct DBT key, struct DBKey *node) {
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
            write_block(db, x, xindex);
            free_var(x);
            return xindex;
        }
        int yindex = x->childs_pages[i];
        struct DBBlock *y = read_block(db, yindex);
        if (y->size >= db->t) {
            struct DBKey tmpnd;
            struct DBT tmpk;
            getmax(db, yindex, &tmpk);
            x->childs_pages[i] = delblock(db, yindex, tmpk, &tmpnd);
            *node = x->keys[i];
            x->keys[i] = tmpnd;
            write_block(db, x, xindex);
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
            x->childs_pages[i + 1] = delblock(db, zindex, tmpk, &tmpnd);
            *node = x->keys[i];
            x->keys[i] = tmpnd;
            write_block(db, x, xindex);
            free_var(x);
            free_var(z);
            free_var(y);
            return xindex;
        }
        free_var(x);
        free_var(y);
        free_var(z);
        xindex = mergesmallnodes(db, xindex, i);
        xindex = delblock(db, xindex, key, node);
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
        x->childs_pages[i] = delblock(db, yindex, key, node);
        write_block(db, x, xindex);
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
            x->childs_pages[i] = delblock(db, yindex, key, node);
            write_block(db, x, xindex);
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
                x->childs_pages[i] = delblock(db, yindex, key, node);
                write_block(db, x, xindex);
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
        xindex = delblock(db, xindex, key, node);
        return xindex;
    }
    if (i < x->size) {
        zrindex = x->childs_pages[i + 1];
        zr = read_block(db, zrindex);
        if (zr->size >= db->t) {
            rebuildright(db, xindex, i);
            free_var(x);
            x = read_block(db, xindex);
            x->childs_pages[i] = delblock(db, yindex, key, node);
            write_block(db, x, xindex);
            free_var(x);
            free_var(zr);
            free_var(y);
            return xindex;
        }
        free_var(x);
        free_var(y);
        free_var(zr);
        xindex = mergesmallnodes(db, xindex, i);
        xindex = delblock(db, xindex, key, node);
        return xindex;
    }
    node->key.size = -1;
    return -1;
}

int delet(struct DB *db, struct DBT *key) {
    struct DBKey *data = calloc(sizeof(*data), 1);
    *db->root = delblock(db, *db->root, *key, data);
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
        return 0;
    }
    if (!block->isleaf) {
        if (keycmp(key, &block->keys[l].key) < 0) {
            return getfromblock(db, block->childs_pages[l], key, data);
        }
        return getfromblock(db, block->childs_pages[r], key, data);
    }
    return -1;
}

int get(struct DB *db, struct DBT *key, struct DBT *data)
{
    return getfromblock(db, *db->root, key, data);
}

/*Close DB*/
int close(struct DB *db) {
    write_dbinf(db);
    fclose(db->f);
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
    //res->f = fopen(buf, "w+");
    res->f = fopen(file, "w+");
    res->conf = conf;
    res->mem_size = 3;
    res->pages = calloc(conf.db_size / conf.chunk_size, 1);
    res->t = 2;
    res->root = malloc(sizeof(*res->root));
    *res->root = -1;
    res->put = &put;
    res->get = &get;
    res->del = &delet;
    res->close = &close;
    res->pagesInCache = 0;
    res->cacheContainer = calloc(sizeof(*res->cacheContainer), res->mem_size);
    res->cacheIndex = calloc(sizeof(int), res->mem_size);
    res->cacheListBegin = NULL;
    res->cacheListEnd = res->cacheListBegin;
    return res;
}

struct DB *dbopen(const char *file, struct DBC conf) {
    struct DB *res = malloc(sizeof(*res));
    res->f = fopen(file, "r+");
    fseek(res->f, 0, SEEK_SET);
    res->root = calloc(sizeof(*res->root), 1);
    fread(res->root, sizeof(*res->root), 1, res->f);
    fread(&res->conf, sizeof(res->conf), 1, res->f);
    res->pages = calloc(res->conf.db_size / res->conf.chunk_size, 1);
    fread(res->pages, sizeof(*res->pages), res->conf.db_size / res->conf.chunk_size, res->f);
    res->t = 25;
    res->put = &put;
    res->get = &get;
    res->del = &delet;
    res->close = &close;
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
        OK
        for (int i = 0; i < height; ++i) {
            printf("-");
        }
        printf("size: %d\n", res);
    }
    return res;
}

/*Print tree*/
void printblock(struct DB *db, int xindex, int height) {
    if (xindex == -1) return;
    struct DBBlock *x = read_block(db, xindex);
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

int main(void) {
    struct DBC conf;
    conf.chunk_size = 4 * 1024;
    conf.db_size = 512 * 1024 * 1024;
    struct DB *db = dbcreate("db", conf);
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
    db_put(db, "size", 4, "toobig", 6);
    char *s[] = {"ololo", "lol", "key 1", "sos", "sneg", "300", "kolhoz", "devil", "angel", "uk", "size"};
    for (int i = 0; i < 30; ++i) {
        int k = rand() % 11;
        char *str = malloc(300);
        int len;
        db_get(db, s[k], strlen(s[k]), (void **)&str, &len);
        printf("%s: ", s[k]);
        puts(str);

        //getchar();
    }
    printf("\n");
    db_del(db, "size", 4);
    db_del(db, "uk", 2);
    for (int i = 0; i < 30; ++i) {
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
    //printblock(db, *db->root, 0);
    db_put(db, "uk", 2, "rf", 2);
    db_del(db, "angel", 5);
    db_del(db, "sneg", 4);
    db_del(db, "kolhoz", 6);
    db_del(db, "300", 3);
    printblock(db, *db->root, 0);
    db_close(db);
}

