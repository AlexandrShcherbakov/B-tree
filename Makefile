all:
	gcc mydb.c -pthread -std=c99 -shared -fPIC  -o libmydb.so
