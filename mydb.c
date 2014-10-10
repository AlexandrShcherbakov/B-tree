#include "mydb.h"


int db_close(struct DB *db) {
	return db->close(db);
}

int db_del(const struct DB *db, void *key, size_t key_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	return db->del(db, &keyt);
}

int db_get(const struct DB *db, void *key, size_t key_len,
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

int db_put(const struct DB *db, void *key, size_t key_len,
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
int block_alloc(const struct DB *db) {
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
    return 0;
}

/*Functions for write db-information and work with offset of it.*/
int write_dbinf(const struct DB *db)
{
    fseek(db->f, 0, SEEK_SET);
    fwrite(&db->t, sizeof(db->t), 1, db->f);
    fwrite(db->root, sizeof(*db->root), 1, db->f);
    fwrite(&db->conf, sizeof(db->conf), 1, db->f);
    fwrite(db->pages, sizeof(*db->pages), db->conf.db_size / db->conf.chunk_size, db->f);
    return 0;
}

int offset_dbinf(const struct DB *db)
{
    return sizeof(db->t) + sizeof(*db->root) + sizeof(db->conf) + sizeof(*db->pages) * db->conf.db_size / db->conf.chunk_size;
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
int write_block(const struct DB *db, struct DBBlock *block, int page) {
    int hyp_size = sizeof(block->isleaf) + sizeof(block->size) +
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

struct DBBlock *read_block(const struct DB *db, int page) {
    struct DBBlock *block = malloc(sizeof(*block));
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
    return block;
}

/*Comparator for keys*/
int keycmp(const struct DBT *a, const struct DBT *b)
{
    int s = a->size > b->size ? b->size : a->size;
    return memcmp(a->data, b->data, s);
}

/*Function put key*/
int split_child(const struct DB *db, int xindex, int yindex, int iter)
{
    struct DBBlock *y = read_block(db, yindex);
    int zindex = block_alloc(db);
    if (zindex == -1) {
        return -1;
    }
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
        for (int i = x->size + 1; i > iter + 1; --i) {
            x->childs_pages[i] = x->childs_pages[i - 1];
        }
        x->childs_pages[iter + 1] = zindex;
        for (int i = x->size; i > iter; --i) {
            x->keys[i] = x->keys[i - 1];
        }
        x->keys[iter] = y->keys[db->t];
        x->size++;
    } else {
        xindex = block_alloc(db);
        if (xindex == -1) {
            return -1;
        }
        x = malloc(sizeof(*x));
        x->isleaf = 0;
        x->keys = malloc(sizeof(*x->keys) * (2 * db->t - 1));
        x->childs_pages = malloc(sizeof(*x->childs_pages) * 2 * db->t);
        x->size = 1;
        x->keys[0] = y->keys[db->t];
        x->childs_pages[0] = yindex;
        x->childs_pages[1] = zindex;
    }
    write_block(db, x, xindex);
    write_block(db, y, yindex);
    write_block(db, z, zindex);
    return xindex;
}

int put_node(const struct DB *db, int xindex, const struct DBT *key, const struct DBT *data)
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
    if (*db->root == -1) {
        *db->root = xindex;
    }
    int i = x->size;
    if (x->isleaf) {
        while (i > 0 && keycmp(key, &x->keys[i].key) < 0) {
            x->keys[i + 1] = x->keys[i];
            i--;
        }
        x->keys[i].key = *key;
        x->keys[i].data = *data;
        x->size++;
    } else {
        while (i >= 0 && keycmp(key, &x->keys[i].key) < 0) {
            i--;
        }
        i++;
        struct DBBlock *y = read_block(db, x->childs_pages[i]);
        if (y->size == 2 * db->t - 1) {
            xindex = split_child(db, xindex, x->childs_pages[i], i);
            if (keycmp(key, &x->keys[i].key) > 0) {
                i++;
            }
        }
        put_node(db, x->childs_pages[i], key, data);
    }
    write_block(db, x, xindex);
    return 0;
}

int put(const struct DB *db, const struct DBT *key, const struct DBT *data) {
    return put_node(db, *db->root, key, data);
}

/*Function of get data by key*/
int getfromblock(const struct DB *db, int xindex, const struct DBT *key, struct DBT *data)
{
    if (*db->root == -1) {
        return -1;
    }
    struct DBBlock *block = read_block(db, xindex);
    int l = 0, r = block->size;
    if (keycmp(key, &block->keys[l].key) > 0) {
        return -1;
    }
    while (r - l > 1) {
        int m = (l + r) / 2;
        if (keycmp(key, &block->keys[m].key) <= 0) {
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
    if (r != block->size && !block->isleaf) {
        return getfromblock(db, block->childs_pages[r], key, data);
    }
    return -1;
}

int get(const struct DB *db, const struct DBT *key, struct DBT *data)
{
    return getfromblock(db, *db->root, key, data);
}

/*Create and open*/
struct DB *dbcreate(const char *file, struct DBC conf)
{
    struct DB *res = malloc(sizeof(*res));
    res->f = fopen(file, "w+");
    res->conf = conf;
    res->pages = malloc(conf.db_size / conf.chunk_size);
    res->t = 3;
    res->root = malloc(sizeof(*res->root));
    *res->root = -1;
    res->put = &put;
    res->get = &get;
    return res;
}

int main(void) {
    struct DBC conf;
    conf.chunk_size = 4 * 1024;
    conf.db_size = 512 * 1024 * 1024;
    struct DB *db = dbcreate("db.txt", conf);
    char a[] = "ololo";
    char *b = "sos";
    db_put(db, a, 5, b, 3);
    int x;
    printf("%d\n", db_get(db, a, 5, (void **)&b, &x));
    printf("%s\n", b);
}
