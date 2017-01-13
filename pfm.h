#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#define INVALID_ITEM 5000
#include <string>
#include <fstream>
#include <climits>


using namespace std;

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                  // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                            // Create a new file
    RC destroyFile   (const string &fileName);                            // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle);    // Open a file
    RC closeFile     (FileHandle &fileHandle);                            // Close a file

protected:
    PagedFileManager();                                                   // Constructor
    ~PagedFileManager();                                                  // Destructor

private:
    static PagedFileManager *_pf_manager;

public:
    bool fileExists(const string& fileName);	//	check the file whether exists or not
};


class FileHandle
{
public:
	bool isAllocated;			//	mark this fileHandle whether is allocated to the specific file
	string specifiedFileName;	//	the specified file's name
	unsigned pagesCounter;		//	count the pages in the file currently
	fstream fstr;				//	the fstream object to write and read the file

public:
    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;

    FileHandle();                                                         // Default constructor
    ~FileHandle();                                                        // Destructor

    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page
    unsigned getNumberOfPages();                                          // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables
}; 




#include "../rbf/pfm.h"
//#define NDEBUG

#include <string>
#include <climits>
#include <assert.h>
#include <stdarg.h>
#include <cstring>


typedef int RC;
typedef char byte;
typedef unsigned PageNum;

// Directory Page
struct Directory{
    unsigned pagenum;
    short int freespace;
};
// Directory Page Descriptor
struct DirectoryDesc{
    unsigned nextDir;
    short int size;
};


#define PAGE_SIZE 4096
#define SUCCESS 0
#define FAILURE -1


using namespace std;

class myFileHandle;

class myPagedFileManager
{
public:
    static myPagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                         // Create a new file
    RC destroyFile   (const string &fileName);                         // Destroy a file
    RC openFile      (const string &fileName, myFileHandle &myFileHandle); // Open a file
    RC closeFile     (myFileHandle &myFileHandle);                         // Close a file

protected:
    myPagedFileManager();                                   // Constructor
    ~myPagedFileManager();                                  // Destructor

private:
    static myPagedFileManager *_pf_manager;

};


class myFileHandle
{
public:
    // variables to keep counter for each operation
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;
	bool isOpen;

    myFileHandle();                                                    // Default constructor
    ~myFileHandle();                                                   // Destructor

    // init filePointer
    RC initFilePointer(const string &fileName);
    RC closeFilePointer();

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    void setNumberOfPages(unsigned numOfPage);                                        // Set the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // put the current counter values into variables

private:
    FILE *filePointer;  // a file pointer to write Pages into disk
    unsigned numberOfPages;
};






#endif
