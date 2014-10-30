all:
	gcc mydb.c -shared -fPIC -o mydb.so -std=c99
