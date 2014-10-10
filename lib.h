#include <string.h>
#include <stdlib.h>
#include <stdio.h>
/* check `man dbopen` */

enum {MAGIC_CONST = 3};

struct DBT {
     void  *data;
     size_t size;
};

struct DBC {
        /* Maximum on-disk file size */
        /* 512MB by default */
        size_t db_size;
        /* Maximum page (node/data chunk) size */
        /* 4KB by default */
        size_t page_size;
        /* Maximum memory size */
        /* 16MB by default */
        size_t mem_size;
};

struct DB{
    /* Public API */
    int (*close)(const struct DB *db);
    int (*del)(const struct DB *db, const struct DBT *key);
    int (*get)(const struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(const struct DB *db, struct DBT *key, const struct DBT *data);
    int (*sync)(const struct DB *db);
    /* Private API */
    struct DBC conf;
    struct DBT dbt;
    struct BT_node *root;
    FILE *fl;
    int *pg;
    int pages;
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DB *dbcreate(const char *file, const struct DBC conf);
struct DB *dbopen  (const char *file); /* Metadata in file */

struct BT_node {
    int isleaf;
    int size;
    struct BT_key *keys;
    struct BT_node **lnk;
    int t;
};

struct BT_key {
    struct DBT key;
    struct DBT val;
};
