
all: TestStorageManager

Parsing: 
	g++ -o dbparser StorageManager.o TestStorageManager.o dbparser.cc

TestStorageManager: StorageManager.o 
	g++ -c parse_tree.cc 

StorageManager.o: Block.h Disk.h Field.h MainMemory.h Relation.h Schema.h SchemaManager.h Tuple.h Config.h StorageSingleton.h parse_tree.h dbparser.h
	g++ -c StorageManager.cpp dbparser.cc

clean:
	rm *.o TestStorageManager
