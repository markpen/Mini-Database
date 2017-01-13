#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <cstdio>

#include "../rbf/rbfm.h"
//#include "mypfm.h"

# define IX_EOF (-1)  // end of the index scan
# define SUCCESS 0
# define FAILURE -1
const int IXDirectorySize = PAGE_SIZE / sizeof(PageNum) ; // 1024
typedef unsigned short int PageSize;

typedef char NodeType;
const char Leaf = 1 , NonLeaf = 2;

const int maxvarchar = PAGE_SIZE;
const int THRESHOLD = PAGE_SIZE / 2;
const PageNum InvalidPage = 0;

typedef enum { OP_Split, OP_Merge , OP_Dist , OP_None , OP_Error }TreeOp;

typedef struct {
	NodeType type;
	PageSize size;
	PageNum next = InvalidPage;
	PageNum prev = InvalidPage;
}NodeDesc;

const int UpperThreshold = (PAGE_SIZE-sizeof(NodeDesc))*0.9;
const int LowerThreshold = (PAGE_SIZE-sizeof(NodeDesc))*0.5;




typedef struct {
	PageNum leftNode;
	PageNum rightNode;
	short int keySize;
	void *keyValue;
} KeyDesc;

typedef struct {
	PageNum overflow = InvalidPage;
	short int numOfRID;
	short int keySize;
	void *keyValue;
} DataEntryDesc;

const int DataEntryKeyOffset =  sizeof(DataEntryDesc) ;

class IX_ScanIterator;
class IXFileHandle;
class myfileHandle;

class IndexManager {
	friend class IX_ScanIterator;
	public:
	static IndexManager* instance();

	// Create an index file
	RC createFile(const string &fileName);

	// Delete an index file
	RC destroyFile(const string &fileName);

	// Open an index and return a file handle
	RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

	// Close a file handle for an index.
	RC closeFile(IXFileHandle &ixfileHandle);

	// Insert an entry into the given index that is indicated by the given ixfileHandle
	RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

	// Delete an entry from the given index that is indicated by the given fileHandle
	RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

	// Initialize and IX_ScanIterator to supports a range search
	RC scan(IXFileHandle &ixfileHandle,
			const Attribute &attribute,
			const void *lowKey,
			const void *highKey,
			bool lowKeyInclusive,
			bool highKeyInclusive,
			IX_ScanIterator &ix_ScanIterator);

	// Print the B+ tree JSON record in pre-order
	void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute);
	void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page, int depth, PageNum nodeNum);



	RC getRecordActualSize(const vector<Attribute> &recordDescriptor, const void* data, int &recordActualSize);

	  ///@brief		create a new page and store the record into it
	RC createNewPageAndInsertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void* data, const int &internalRecordLen, RID &rid);

	  ///@brief		transform the data given default into the internal record structure
	RC transRecordFromDefaultToInternal(const vector<Attribute> &recordDescriptor, char* internalRecord, int& internalRecordLen, const void* data);

	  ///@brief		transform the internal record into the format given default
	RC transRecordFromInternalToDefault(const vector<Attribute> &recordDescriptor, void* data, const char* internalRecord, const int &internalRecordLen);

	  ///@brief		insert the record into the appointed page
	RC insertRecordIntoAppointedPage(FileHandle &fileHandle, char* internalRecord, int recordLen, RID &rid, int pageNumber);

	  ///@brief		update the record in current page
	RC updateRecordInCurrentPage(FileHandle &fileHandle, char* page, char* record, int &recordStartOffset, int &recordLen, int &originalRecordLen, int &recordDataSize, int &slotIndex, int &slotQuantity, const RID &rid);



	protected:
	IndexManager();
	~IndexManager();

	private:
	static IndexManager *_index_manager;


	 RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

	 RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

	  // This method will be mainly used for debugging/testing.
	  // The format is as follows:
	  // field1-name: field1-value  field2-name: field2-value ... \n
	  // (e.g., age: 24  height: 6.1  salary: 9000
	  //        age: NULL  height: 7.5  salary: 7500)
	 RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

	/******************************************************************************************************************************************************************
	IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
	******************************************************************************************************************************************************************/
	 RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

	  // Assume the RID does not change after an update
	 RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

	 RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);




	void FindLastKey(void *page,KeyDesc &keyDesc);

	void checkKeyInt(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page);

	void checkPageInt(IXFileHandle &ixfileHandle, void *page, PageNum pageNum,bool p);


	RC FindOffset(void *page,int size,int &offset,bool IsGreater);

	TreeOp TraverseTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *page, PageNum pageNum, PageNum &returnpageNum);

	TreeOp TraverseTreeDelete(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc,int rightMost);

	TreeOp TraverseTreeInsert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc);


	TreeOp insertToLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc);


	TreeOp deleteFromLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page,

			PageNum pageNum, KeyDesc &keyDesc, int rightMost);


	int keyCompare(const Attribute &attribute, const void *keyA, const void* keyB);
	int getKeySize(const Attribute &attribute, const void *key);
	void printKey(const Attribute &attribute, const void *key);
	bool unsync; // deletion sync flag
	bool debug;


	private:
	int slotCountLen;				//	the size of slotCount
	int slotIdLen; 				//	the size of the slotId
	int recordOffsetLen;			//	the size of record offset
	int recordSizeLen;			//	the size of the record's length
	int slotLen; 					//	the size of slot
	int recordDataSizeLen;		//	the size of the length of all the internal records
	int slotTableSizeLen;			//	the size of the length of the slot table

	int isNullFieldLen;			//	the size of the flag about this field whether is null
	int internalRecordHeadSlotLen;//	the size of the internal record head slot
	int recordHeadSlotLen;		//	the size of the internal record head slot
	int isInCurrentPageLen;   	//	the size of the flag byte for the internal record. If it is 'y', the internal record is in current page. Else if it is 'n', the internal record is in another page.
	int versionNumberLen;			//  the size of the version number of the internal record
	int slotTableHeadLen;			//	the size of the slot table head
	int reservedBytesLen;			//	the size of reserved bytes in the record head
	int recordFixedHeadLen;		//	the fixed size of the record head
	int fieldOffsetLen;			//	the size of the field offset
};


class IXFileHandle {
	public:
		// Put the current counter values of associated PF FileHandles into variables
		RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

		IXFileHandle();  							// Constructor
		~IXFileHandle(); 							// Destructor

		unsigned ixReadPageCounter;
		unsigned ixWritePageCounter;
		unsigned ixAppendPageCounter;

		bool isOpen;
		// Initialize the file pointer for an index.
		RC initFilePointer(const string &fileName);

		// Close a file handle for an index.
		RC closeFilePointer();

		// PageNum from pfm is unsigned int
		RC readPage(PageNum pageNum, void *data);
		RC writePage(PageNum pageNum, const void *data);
		RC deletePage(PageNum pageNum);

		bool root_debug = false;
		PageNum findFreePage();
		PageNum findRootPage();
		RC updateRootPage(PageNum pageNum);
		RC isReadable();
	private:
		myFileHandle fileHandle;
		RC error;

};




class IX_ScanIterator {

	public:
		IX_ScanIterator();  							// Constructor
		~IX_ScanIterator(); 							// Destructor

		RC getNextEntry(RID &rid, void *key);			 		// Get next matching entry
		RC close();             						// Terminate index scan

		// Initialize and IX_ScanIterator to supports a range search
		RC init(IXFileHandle &ixfileHandle,
				const Attribute &attribute,
				const void *lowKey,
				const void *highKey,
				bool lowKeyInclusive,
				bool highKeyInclusive);

	private:
		IXFileHandle ixfileHandle;
		IndexManager *im;
		Attribute attribute;
		void *lowKey; bool lowKeyNull , highKeyNull;
		void *highKey;
		bool lowKeyInclusive, highKeyInclusive;
		void *page, *overflowPage;
		bool overflow;
		int offsetToKey, offsetToRID, overflowRID;
		PageNum currentOverflowPage, pageNum;

};

// print out the error message for a given return code
void IX_PrintError (RC rc);

#endif
