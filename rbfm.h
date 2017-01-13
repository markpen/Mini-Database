#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <climits>

#include "../rbf/pfm.h"

using namespace std;

class RecordBasedFileManager;

// Record ID
typedef struct
{
  unsigned pageNum;    // page number
  unsigned slotNum;    // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
    int position;		//	attibute position in the table
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// =
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;


/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
  RBFM_ScanIterator();
  ~RBFM_ScanIterator();

  // Never keep the results in the memory. When getNextRecord() is called,
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);
  RC close();

  ///@brief		check whether this record satisfies the filter
  bool isSatisfied(char* record, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value);

  ///@brief		output the projected fields with internal record
  void getProjectedRecord(char* record, int recordLen, const vector<Attribute> &recordDescriptor, vector<string> attributeNames, void* data);

  ///@brief		judge whether this attribute is the projected attribute
  bool isProjected(Attribute recordAttribute, vector<string> attributeNames);

  vector<Attribute> recordDescriptor;
  string conditionAttribute;
  CompOp compOp;
  const void* value;
  vector<string> attributeNames;
  int startPageId;		//	store the page number of the previous record
  int startSlotIndex;	//	store the index of the previous record
  FileHandle fileHandle;
  RecordBasedFileManager* rbfm;

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
  int fieldLen;					//	the size of the field
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first,
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
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

  // Scan returns an iterator to allow the caller to go through the results one by one.
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

public:
  ///@brief		get one record's actual length, including its null-fields indicator and actual data
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
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;

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

public:
  PagedFileManager* pfm;		//	use class PageFileManager to manage the file's create, delete, open and close.
};

#endif
