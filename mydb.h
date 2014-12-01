#include <stddef.h>
#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

/* check `man dbopen` */
struct DBT {
	void  *data;
	size_t size;
};

struct DBC {
        /* Maximum on-disk file size
         * 512MB by default
	 * */
        size_t db_size;
        /* Maximum chunk (node/data chunk) size
         * 4KB by default
	 * */
        size_t chunk_size;
	/* For future uses - maximum cached memory size
	 * 16MB by default */
	    //size_t mem_size;
};

struct DB {
	/* Public API */
	/* Returns 0 on OK, -1 on Error */
	int (*close)(struct DB *db);
	int (*del)(struct DB *db, struct DBT *key);
	/* * * * * * * * * * * * * *
	 * Returns malloc'ed data into 'struct DBT *data'.
	 * Caller must free data->data. 'struct DBT *data' must be alloced in
	 * caller.
	 * * * * * * * * * * * * * */
	int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
	int (*put)(struct DB *db, struct DBT *key, struct DBT *data);
	/* For future uses - sync cached pages with disk
	 * int (*sync)(const struct DB *db)
	 * */
	/* Private API */
	//FILE *f;
	int f;
	int t;
	int *root;
	struct DBC conf;
	char *pages;
	struct cacheList *cacheListBegin, *cacheListEnd;
	struct DBBlock *cacheContainer;
	int *cacheIndex;
	int pagesInCache;
	int mem_size;
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DBKey {
    struct DBT key;
    struct DBT data;
};

struct DBBlock {
    int *childs_pages;
    int size;
    char isleaf;
    struct DBKey *keys;
};

struct cacheList {
    struct cacheList *next, *perv;
    int id;
    char info;
};

struct DB *dbcreate(const char *file, struct DBC conf);
struct DB *dbopen  (const char *file, struct DBC conf);

int db_close(struct DB *db);
int db_del(struct DB *, void *, size_t);
int db_get(struct DB *, void *, size_t, void **, size_t *);
int db_put(struct DB *, void *, size_t, void * , size_t  );
/* For future uses - sync cached pages with disk
 * int db_sync(const struct DB *db);
 * */
