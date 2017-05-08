#ifndef __STORAGE_SINGLETON_H__
#define __STORAGE_SINGLETON_H__
#include<ctime>
#include <iostream>
#include "SchemaManager.h"
#include "MainMemory.h"
#include "Disk.h"

class StorageSingleton
{
    public:
        static StorageSingleton& getInstance()
        {
            static StorageSingleton instance;
            return instance;
        }
        MainMemory mem;
        Disk disk;
        
        SchemaManager* schema_manager;
        clock_t start_time;
        StorageSingleton() {
                std::cout<<"The memory contains " << mem.getMemorySize() << " blocks" << endl;
                std::cout<<mem<<endl<<endl;
              schema_manager = new SchemaManager(&mem,&disk);
              disk.resetDiskIOs();
              disk.resetDiskTimer();
              start_time=clock();
        }                    




    public:
        StorageSingleton(StorageSingleton const&)               = delete;
        void operator=(StorageSingleton const&)  = delete;
};
#endif