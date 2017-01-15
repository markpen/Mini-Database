#include "pfm.h"
#include <fstream>


#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>
#include <math.h>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

///@brief	check the file has been existed or not
///@param	fileName	the name of file
///@return	if the file has been there, return false. Or return true
bool PagedFileManager::fileExists(const string& fileName) {
    struct stat stFileInfo;

    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}

///@brief	create the file named as fileName
///@param	fileName	the name of the new file
///@return	if the file is created successfully, return 0. Or return -1
RC PagedFileManager::createFile(const string &fileName)
{
	if (!fileExists(fileName)) {
		ofstream fout(fileName);
		return 0;
	}
    return -1;
}

///@brief	delete the file named as fileName
///@param	fileName	the name of the file to be deleted
///@return	if the file exists, return 0. Otherwise, return -1
RC PagedFileManager::destroyFile(const string &fileName)
{
	const char* fileName_cchar = fileName.c_str();
	if (fileExists(fileName)) {
		remove(fileName_cchar);	//	the parameter should be const char*
		return 0;
	}
    return -1;
}

///@brief	open file named as fileName
///@param	fileName	the name of the file going to be open
///@param	fileHandle	the access to the file
///@return	if the file can be opened correctly, return 0; Or return -1.
RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if (fileExists(fileName) && !fileHandle.isAllocated) {
		fileHandle.isAllocated = true;
		fileHandle.specifiedFileName = fileName;
		fileHandle.fstr.open(fileName, ios::in | ios::out | ios::binary);


		//	set the pagesCounter here
		if (fileHandle.fstr.is_open()) {
			streampos start, end;
			fileHandle.fstr.seekg(0, ios::beg);
			start = fileHandle.fstr.tellg();
			fileHandle.fstr.seekg(0, ios::end);
			end = fileHandle.fstr.tellg();
			fileHandle.pagesCounter = (end - start) / PAGE_SIZE;	//	the page size is 4K Bytes

			return 0;
		}
	}
    return -1;
}

///@brief	close the file allocated the file handler names as fileHandle
///@param	fileHandle	the name of the file handles
///@return	if the file can be closed successfully, return 0. Otherwise return -1
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	if (fileHandle.fstr.is_open()) {
		fileHandle.fstr.close();
		fileHandle.isAllocated = false;
		fileHandle.specifiedFileName = "";
		fileHandle.pagesCounter = 0;
		return 0;
	}
    return -1;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;

    isAllocated = false;
    specifiedFileName = "";
    pagesCounter = 0;
}


FileHandle::~FileHandle()
{
}

///@brief	read the pointed page from file
///@param	pageNum		the number of the page which is going to be read
///@param	data		store the data from page into this memory area
///@return	if the read operation is successful, return 0. Else, return -1
RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if (fstr.is_open()) {
		if (pageNum >= pagesCounter)	return -1;		//	if the page request doesn't exist, return -1

		char* memBlock = new char[PAGE_SIZE];		//	new the memory block for the read content
		fstr.seekg(pageNum * PAGE_SIZE, ios::beg);	//	set the start position to read in the file
		fstr.read(memBlock, PAGE_SIZE);
		memcpy(data, memBlock, PAGE_SIZE);			//	copy the data to the output parameter

		readPageCounter++;							//	increment the read operation counter

		delete []memBlock;							//	release the memory space applied before

		return 0;
	}
    return -1;
}

///@brief	write the appointed page in the file
///@param	pageNum	the number of the page to be written
///@param	data	the data is going to be written
///@return	if the write operation is successful, return 0. Else, return -1
RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if (fstr.is_open()) {
		if (pageNum >= pagesCounter)	return -1;

		fstr.seekp(pageNum * PAGE_SIZE, ios::beg);
		char* memBlock = new char[PAGE_SIZE];
		memcpy(memBlock, data, PAGE_SIZE);
		fstr.write(memBlock, PAGE_SIZE);

		writePageCounter++;

		delete []memBlock;

		return 0;
	}
    return -1;
}

///@brief	append a page of data at the end of the file
///@param	data	the start address of the data which are going to be stored
///@return	if the data can be stored into the file correctly, return 0. Else return -1
///@note	the operation on the page is limited in the size of 4KB - one page's size. no more, no less.
RC FileHandle::appendPage(const void *data)
{
	if (fstr.is_open()) {
		char* memBlock = new char[PAGE_SIZE];
		memcpy(memBlock, data, PAGE_SIZE);

		fstr.seekp(0, ios::end);	//	set the end of the file as the beginning of writing position
		fstr.write(memBlock, PAGE_SIZE);	//	write to the file

		pagesCounter++;	//	page number increments 1
		appendPageCounter++;

		cout << "page" << pagesCounter << endl;
		delete []memBlock;

		return 0;
	}
    return -1;
}


///@brief	get the counters' value of the operations
///@param	readPageCount	the number of operation "read"
///@param	writePageCount	the number of operation "write"
///@param	appendPageCount	the number of operation "append"
///@return	if the values can be obtained validly, return 0, else return -1
RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	if (this->isAllocated) {
		readPageCount = this->readPageCounter;
		writePageCount = this->writePageCounter;
		appendPageCount = this->appendPageCounter;
		return 0;
	}
    return -1;
}

unsigned FileHandle::getNumberOfPages() {
	return pagesCounter;

}










// own implementation
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

myPagedFileManager* myPagedFileManager::_pf_manager = 0;

// singleton
myPagedFileManager* myPagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new myPagedFileManager();

    return _pf_manager;
}

// constructor
myPagedFileManager::myPagedFileManager()
{
}

// destructor
myPagedFileManager::~myPagedFileManager()
{
}


RC myPagedFileManager::createFile(const string &fileName)
{
    // if file doesn't exist
    if ( access( fileName.c_str(), F_OK ) == -1  ){
	//dprintf("file doen't exsit \n");
	FILE* file = fopen(fileName.c_str(),"w+b");
	if( file != NULL )
	   fclose(file);
	else
	   return FAILURE;

	return SUCCESS;
    }

    return FAILURE;
}


RC myPagedFileManager::destroyFile(const string &fileName)
{
    if( remove( fileName.c_str() ) != 0 )
	return FAILURE;
    else
	return SUCCESS;
}


RC myPagedFileManager::openFile(const string &fileName, myFileHandle &myFileHandle)
{

    // file does exist, open it
    if ( access( fileName.c_str(), F_OK ) == 0  ){
	//dprintf("file does exist, open it \n");
	if( myFileHandle.initFilePointer( fileName ) == FAILURE )
	    return FAILURE;

	myFileHandle.readPageCounter = 0;
	myFileHandle.writePageCounter = 0;
	myFileHandle.appendPageCounter = 0;

	myFileHandle.isOpen = true;

	return SUCCESS;
    }


    return FAILURE;
}


RC myPagedFileManager::closeFile(myFileHandle &myFileHandle)
{
    return myFileHandle.closeFilePointer();
}


myFileHandle::myFileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
	numberOfPages = 0;

	isOpen = false;
}


myFileHandle::~myFileHandle()
{
}


RC myFileHandle::readPage(PageNum pageNum, void *data)
{
    fseek( filePointer , pageNum * PAGE_SIZE ,SEEK_SET );
    int result = fread(data, sizeof(char), PAGE_SIZE, filePointer);
    if( result != PAGE_SIZE ){
	return FAILURE;
    }

    readPageCounter++;
    return SUCCESS;
}


RC myFileHandle::writePage(PageNum pageNum, const void *data)
{
/*
    debug = true;
    //dprintf("w%d\n",pageNum * PAGE_SIZE );

    assert( pageNum <= numberOfPages && "pageNum should smaller than numberofPages" );
    if( pageNum > numberOfPages ){
	return FAILURE;
    }
*/
    fseek( filePointer , pageNum * PAGE_SIZE , SEEK_SET );
    int result = fwrite(data, sizeof(char), PAGE_SIZE, filePointer);

    if( result != PAGE_SIZE ){
	//dprintf("page hasn't been opened yet\n");
	return FAILURE;
    }
    writePageCounter++;
    return SUCCESS;
}


RC myFileHandle::appendPage(const void *data)
{
    // append page
    fseek(filePointer, 0, SEEK_END);
    long int fileSize = ftell(filePointer);
    int result = fwrite(data,sizeof(char),PAGE_SIZE,filePointer);
    if( result != PAGE_SIZE )
	return FAILURE;
    appendPageCounter++;
    numberOfPages++;
    return SUCCESS;
}


unsigned myFileHandle::getNumberOfPages()
{
    return appendPageCounter;
}


RC myFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return SUCCESS;
}

RC myFileHandle::initFilePointer(const string &fileName)
{
    filePointer = fopen(fileName.c_str(),"r+b");
    if(isOpen) return FAILURE;
    if( filePointer == NULL )
	return FAILURE;

    fseek(filePointer, 0, SEEK_END);
    int numOfBytes = ftell(filePointer);
    //dprintf("file length in byte in %s is %d \n\n",fileName.c_str(),numOfBytes);
    setNumberOfPages(numOfBytes/PAGE_SIZE);

    return SUCCESS;
}

RC myFileHandle::closeFilePointer()
{
  if( fclose( filePointer ) == 0 )
    return SUCCESS;
  return FAILURE;
}

void myFileHandle::setNumberOfPages(unsigned numOfPage)
{
    numberOfPages = numOfPage;
}

