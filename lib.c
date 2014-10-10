#include "lib.h"

struct DB *
dbcreate(const char *file, const struct DBC conf)
{
    struct DB *db = malloc(sizeof(*db));
    db->fl = fopen(file, "w+");
    db->conf = conf;
}

struct BT_node *
block_alloc()
{

}

/*int
keycmp(const struct DBT a, const struct DBT b)
{
    int s = a.size > b.size ? b.size : a.size;
    return memcmp(a.data, b.data, s);
}

int
getfromnode(const struct BT_node *node, const struct DBT *key, struct DBT *data)
{
    int l = 0, r = node->size;
    if (keycmp(key, &node->keys[l].key) > 0) {
        return -1;
    }
    while (r - l > 1) {
        int m = (l + r) / 2;
        if (keycmp(key, &node->keys[m].key) <= 0) {
            l = m;
        } else {
            r = m;
        }
    }
    if (keycmp(key, &node->keys[l].key) == 0) {
        data->data = node->keys[l].val.data;
        data->size = node->keys[l].val.size;
        return 0;
    }
    if (r != node->size && !node->isleaf) {
        return getfromnode(node->lnk[r], key, data);
    }
    return -1;
}

int
db_get(const struct DB *db, struct DBT *key, struct DBT *data)
{
    if (db->root == NULL) {
        return -1;
    }
    return getfromnode(db->root, key, data);
}

struct BT_node *
split_child(struct BT_node *x, struct BT_node *y, int iter)
{
    struct BT_node *z = malloc(sizeof(struct BT_node));
    z->isleaf = y->isleaf;
    z->t = y->t;
    z->size = z->t - 1;
    z->keys = malloc(sizeof(struct BT_key *) * (2 * y->t - 1));
    z->lnk = malloc(sizeof(struct BT_node *) * 2 * y->t);
    for (int i = 0; i < y->t - 1; ++i) {
        z->keys[i] = y->keys[i + y->t];
    }
    if (!z->lnk) {
        for (int i = 0; i < y->t; ++i) {
            z->lnk[i] = y->lnk[i + y->t];
        }
    }
    y->size = y->t - 1;
    if (x != y) {
        for (int i = x->size + 1; i > iter + 1; --i) {
            x->lnk[i] = x->lnk[i - 1];
        }
        x->lnk[iter + 1] = z;
        for (int i = x->size; i > iter; --i) {
            x->keys[i] = x->keys[i - 1];
        }
        x->keys[iter] = y->keys[y->t];
        x->size++;
    } else {
        x = malloc(sizeof(struct BT_node));
        x->isleaf = 0;
        x->keys = malloc(sizeof(struct BT_key) * (2 * y->t - 1));
        x->lnk = malloc(sizeof(struct BT_node) * 2 * y->t);
        x->size = 1;
        x->t = y->t;
        x->keys[0] = y->keys[y->t];
        x->lnk[0] = y;
        x->lnk[1] = z;
    }
    return x;
}

int
put_node(struct BT_node *x, struct DBT key, struct DBT *data)
{
    int i = x->size;
    if (x->isleaf) {
        while (i >= 0 && keycmp(key, x->keys[i].key) < 0) {
            x->keys[i + 1] = x->keys[i];
            i--;
        }
        x->keys[i + 1].key = key;
        x->size++;
    } else {
        while (i >= 0 && keycmp(key, x->keys[i].key) < 0) {
            i--;
        }
        i++;
        if (x->lnk[i]->size == 2 * x->t - 1) {
            split_child(x, x->lnk[i], i);
            if (keycmp(key, x->keys[i]) > 0) {
                i++;
            }
        }
        put_node(x->keys[i], key, data);
    }
    return 0;
}

int
db_put(const struct DB *db, struct DBT *key, const struct DBT *data)
{
    if (db == NULL || db->root->size == 0) {
        return -1;
    }
    if (db->root->size == 2 * db->root->t - 1) {
        db->root = split_child(root, root, 0);
    }
    return put_node(root, key, data);
}*/
