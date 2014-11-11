all:
	g++ mydb.cpp -std=c++11 -shared -fPIC -o libmydb.so -fpermissive
