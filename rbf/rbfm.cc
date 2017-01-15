#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>
#include <math.h>
#include <algorithm>

#include "rbfm.h"

using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	this->pfm->instance();

	slotCountLen = sizeof(unsigned short);		//	the size of slotCount
	slotIdLen = sizeof(unsigned short);			//	the size of the slotId
	recordSizeLen = sizeof(unsigned short);		//	the size of the record's size
	recordOffsetLen = sizeof(unsigned short);	//	the size of record offset
	slotLen = slotIdLen + recordSizeLen + recordOffsetLen;		//	the size of slot
	isNullFieldLen = sizeof(char);				//	the size of the flag about this field whether is null
	internalRecordHeadSlotLen = sizeof(unsigned short) + isNullFieldLen;			//	the size of the internal record head entry
	isInCurrentPageLen = sizeof(char);			//	the size of the flag byte for the internal record. If it is 'y', the internal record is
												//	in current page. Else if it is 'n', the internal record is in another page.
	versionNumberLen = sizeof(unsigned short);	//  the size of the version number of the internal record

	recordDataSizeLen = sizeof(unsigned short);		//	the size of the length of all the internal records
	slotTableSizeLen = sizeof(unsigned short);		//	the size of the length of the slot table
	slotTableHeadLen = slotCountLen + recordDataSizeLen + slotTableSizeLen;					//	the size of the slot table head
	reservedBytesLen = sizeof(int);					//	the size of reserved bytes in the record head
	recordFixedHeadLen = isInCurrentPageLen + versionNumberLen + reservedBytesLen;			//	the fixed size of the record head
	fieldOffsetLen = sizeof(unsigned short);		//	the size of the field offset in the record head
	recordHeadSlotLen = internalRecordHeadSlotLen;  //	the size of the internal record head slot
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

///@brief	create the file named as fileName
///@param	fileName	the name of the file
///@return	if the file can be created successfully, return 0. Else return -1
RC RecordBasedFileManager::createFile(const string &fileName) {
	return this->pfm->createFile(fileName);
}

///@brief	destroy the file named as fileName
///@param	fileName	delete the file named as fileName
///@return	if this destroy operation can be executed successfully, return 0, else return -1
RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return this->pfm->destroyFile(fileName);
}

///@brief	open the file named as fileName
///@param	fileName	the name of the file
///@param	fileHandle	the handler of the file
///@return	if the file can be opened successfully, return 0, else return -1
RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return this->pfm->openFile(fileName, fileHandle);
}

///@brief	close the file named as fileName
///@param	fileHandle	the handler of the file
///@return	if the file closed successfully, return 0, else return -1
RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return this->pfm->closeFile(fileHandle);
}

///@brief	insert a record into the file
///@param	fileHandle			the handler of the file
///@param	recordDescriptor	the descriptor about the record's fields
///@param	data				the pointer to the record
///@param	rid					the series number of the record
///@return	if the record can be inserted correctly, return 0, else return -1
/* @note	1. set the slot directory at every page's end, which starts with 2 bytes to store the valid slots' amount, which means that even
 *             each insert/delete operatioin on the page will lead to the modification of slotCount. It equals to the records' quantity in
 *             the page. Then it follows 2 bytes to store the actual data length. Behind that, there are another 2 bytes to store the length
 *             of the slot table.
 * 			2. Each slot's size is 6 bytes. The first 2 bytes is the slot id, the second 2 bytes is the length of the internal record. The
 * 			   third 2 bytes are used to store the starting offset of the internal record. Each record has a corresponding slot. For the
 * 			   reason of each page's size is fixed to 4 KBytes, so the record's amount can't be over 4K. The integer in 2 bytes can cover it.
 * 			3. To satisfy the requirement of O(1) limit for visiting each fields in the record, the internal record's structure is organized
 * 			   as two parts.
 * 			4. The first part is the head, which has 4 parts. The first part is a 1 byte flag. It represents whether this record is really
 * 			   stored in the current page or it is just a tomb stone. The second part is a 2 bytes field to represent the version number of
 * 			   the record. It is reserved to implement the credit part of project 2. The third part is reserved bytes, which is used to
 * 			   guarantee that the least record is no less than 9 bytes. The fourth part are the fields offset, which includes each field's
 * 			   flag about whether it is null and its offset in the record. The slot in the head is represents by char and unsigned short
 * 			   type, 1 + 2 = 3 bytes. If there is a null field, the flag would be 'y', otherwise it is 'n'.
 * 			5. The second part is the actual record data. It's the same as the default record structure. For the integer and fload type,
 * 			   there's 4 bytes to store. For the varchar type, there is 4 + n bytes to store. Here 4 bytes is the varchar length and n bytes
 * 			   is the string's length. Before we insert it, we should transform it at first.
 * 			6. Before write the record into the page, we should judge whether there is enough free space for this record. The slot directory's
 * 			   size = slot amount * 6 bytes + 6 bytes. The last offset for the record area is stored in the head of the slot table. So the
 * 			   size of the free space in one page is equal to (the end of the slot directory - the tail of the record area).
 * 			7. If there is enough free space for the new record, we insert it and update the corresponding slot directory. Otherwise, we have
 * 			   to find a page which holds enough free space for it from file beginning. If there's none page satisfies this record's space,
 * 			   we append a new page to store it.
 * 			8. When we do some operation on the page of files, we should load it into memory, just one page. Rather than modifying directyly
 * 			   on the files through file streams.
 */
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	int recordLen = 0;							//	the length of the record
	int rc = 0;											//	the return code for the functions in this project
	char* internalRecord = new char[PAGE_SIZE];								//	the internal record format

	//	transform the record's format from default into internal
	rc = transRecordFromDefaultToInternal(recordDescriptor, internalRecord, recordLen, data);

	if (fileHandle.fstr.is_open()) {
		// if there is no page in the file

		if (fileHandle.pagesCounter == 0) {

			rc = createNewPageAndInsertRecord(fileHandle, recordDescriptor, internalRecord, recordLen, rid);

			assert(rc == 0 && "create a new page and insert the record failed.");

			return 0;
		}
		else {

			//	try to insert the record into the last page first
			rc = insertRecordIntoAppointedPage(fileHandle, internalRecord, recordLen, rid, fileHandle.pagesCounter - 1);
			if (rc == 0)	return 0;
			// if there isn't enough free space in the last page, search the pages from the beginning of the file
			else {
				for (int i = 0; i < fileHandle.pagesCounter - 1; i++)
					if (insertRecordIntoAppointedPage(fileHandle, internalRecord, recordLen, rid, i) == 0) {
						return 0;
					}
			}
		}
		//	if there is no page in the file has enough free space for the new record, then create a new page and insert it.
		rc = createNewPageAndInsertRecord(fileHandle, recordDescriptor, internalRecord, recordLen, rid);
		assert(rc == 0 && "create new page failed");
		return 0;
	}
	return -1;
}

///@brief	insert the record into the appointed page
///@param	fileHandle			the handler of the file
///@param	internalRecord		the pointer to the internal record
///@param	recordLen			the length of the internal record
///@param	rid					the series number of the record
///@param	pageNumber			the number of the appointed page
///@return	if the record is inserted successfully, return 0, else return -1
RC RecordBasedFileManager::insertRecordIntoAppointedPage(FileHandle &fileHandle, char* internalRecord, int recordLen, RID &rid, int pageNumber) {
	int freeSpaceSize = 0;								//	free space in one page
	int slotTableSize = 0;						//	the size of slot table
	int newSlotId = 0;							//	the latest record id
	int recordDataSize = 0;					//	the size of actual record data
	int slotCount = 0;							//	the counter of the slots in the slot table
	int rc = 0;											//	the return code for the functions in this project

	if (fileHandle.fstr.is_open()) {
		char* page = new char[PAGE_SIZE];
		memset(page, 0, PAGE_SIZE);

		//	read the data from the file with pageNumber
		int rc = fileHandle.readPage(pageNumber, page);
		assert(rc == 0 && "read page failed.");

		//	get the slot table size
		memcpy(&slotTableSize, page + PAGE_SIZE - slotTableHeadLen, slotTableSizeLen);

		//	get the actual records size
		memcpy(&recordDataSize, page + PAGE_SIZE - slotCountLen - recordDataSizeLen, recordDataSizeLen);

		freeSpaceSize = PAGE_SIZE - slotTableSize - recordDataSize;


		//if there is enough free space in the page, insert it
		if (recordLen <= freeSpaceSize) {
			//	get the slotCount and update it in the slot table head. slotCount is used to represent the valid slots in the slot table,
			//	excluding the invalid ones.
			memcpy(&slotCount, page + PAGE_SIZE - slotCountLen, slotCountLen);
			slotCount++;
			memcpy(page + PAGE_SIZE - slotCountLen, &slotCount, slotCountLen);

			//	update the slot table and the slot
			int slotQuantity = (slotTableSize - slotTableHeadLen) / slotLen;
			int slotId = 0;
			int slotIndex = 0;
			for (slotIndex = 0; slotIndex < slotQuantity; slotIndex++) {
				memcpy(&slotId, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen), slotIdLen);
				if (slotId == INVALID_ITEM) {
					//	update the new slot id in the slot
					newSlotId = slotIndex;
					memcpy(page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen), &newSlotId, slotIdLen);

					//	update the record length in the slot
					memcpy(page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen - slotIdLen), &recordLen, recordSizeLen);

					//	update the record start offset in the slot
					memcpy(page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen - slotIdLen - recordSizeLen), &recordDataSize, recordDataSizeLen);

					//	update the recordDataSize in the slot table head
					recordDataSize += recordLen;
					memcpy(page + PAGE_SIZE - slotCountLen - recordDataSizeLen, &recordDataSize, recordDataSizeLen);

					//	because we find a deserted slot in the slot table, not append a new slot, we don't need to change the slot table size
					break;
				}
			}
			if (slotIndex == slotQuantity) {
				if (recordLen + slotLen > freeSpaceSize)	return -1;
				//	update the slot table size in the slot table head
				slotTableSize += slotLen;
				memcpy(page + PAGE_SIZE - slotTableHeadLen, &slotTableSize, slotTableSizeLen);

				//	update the slot id in the slot
				newSlotId = slotIndex;
				memcpy(page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen), &newSlotId, slotIdLen);

				//	update the record length in the slot
				memcpy(page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen - slotIdLen), &recordLen, recordSizeLen);

				//	update the record start offset in the slot
				memcpy(page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen - slotIdLen - recordSizeLen), &recordDataSize, recordDataSizeLen);

				//	update the recordDataSize in the slot table head
				recordDataSize += recordLen;
				memcpy(page + PAGE_SIZE - slotCountLen - recordDataSizeLen, &recordDataSize, recordDataSizeLen);
			}

			//	update the actual record area
			memcpy(page + recordDataSize - recordLen, internalRecord, recordLen);

			//	write the page into disk
			fileHandle.writePage(pageNumber, page);

			//	update the rid
			rid.pageNum = pageNumber;
			rid.slotNum = newSlotId;
			return 0;
		}
		return -1;	//	if there is no enough free space in this page, return -1
	}
}

///@brief	read a record whose id is given
///@param	fileHandle			the handler of the file
///@param	recordDescriptor	the descriptor about the record's fields
///@param	rid					the record id
///@param	data				the buffer stores the record
///@return	if this record can be read successfully, return 0. Otherwise return -1
///@note	1. because after multiple times of records' deletion and insertion, the slots id are not in order. so the slot offset in the slot table
//			can't be traced in the slot table by O(1) time complexity. Meanwhile, we have to iterate all the slot one by one to find the
//			the slot matching the given id. Its time complexity is O(n).
//			2. if the rid in current page is a tomb stone, we should trace down to the valid record with this tomb stone and read it.
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	if (fileHandle.fstr.is_open()) {
		char* page = new char[PAGE_SIZE];
		memset(page, 0, PAGE_SIZE);

		//	read the data from the file with pageNum
		int rc = fileHandle.readPage(rid.pageNum, page);
		assert(rc == 0 && "read page failed.");

		int recordStartOffset = 0;
		int recordLen = 0;
		int slotId = 0;
		int slotQuantity = 0;
		int slotTableSize = 0;

		//	get the slot table size
		memcpy(&slotTableSize, page + PAGE_SIZE - slotTableHeadLen, slotTableSizeLen);
		// 	get the slot's quantity
		slotQuantity = (slotTableSize - slotTableHeadLen) / slotLen;

		int slotIndex = 0;
		for (slotIndex = 0; slotIndex < slotQuantity; slotIndex++) {
			memcpy(&slotId, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen), slotIdLen);
			if (slotId == rid.slotNum) {
				memcpy(&recordLen, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen, recordSizeLen);
				memcpy(&recordStartOffset, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen + recordSizeLen, recordOffsetLen);

				//	if the record is invalid, read will fail.
				if (recordLen == 0 || recordStartOffset == INVALID_ITEM)	return -1;

				char* internalRecord = new char[recordLen];
				memcpy(internalRecord, page + recordStartOffset, recordLen);

				char isInCurrentPage = 'z';
				memcpy(&isInCurrentPage, internalRecord, isInCurrentPageLen);
				//	if the record is not a tomb stone
				if (isInCurrentPage == 'y') {
					//	transform the record's format from the internal to default
					rc = transRecordFromInternalToDefault(recordDescriptor, data, internalRecord, recordLen);
					assert(rc == 0 && "transform failed.");
				}
				//	if the record is a tomb stone
				if (isInCurrentPage == 'n') {
					RID tombStone;
					tombStone.pageNum = 0;
					tombStone.slotNum = 0;
					memcpy(&tombStone, internalRecord + isInCurrentPageLen, sizeof(RID));
					rc = readRecord(fileHandle, recordDescriptor, tombStone, data);
					assert(rc == 0 && "read record failed.");
				}
				return 0;
			}
		}
	}
    return -1;
}

///@brief	print the record to the screen
///@param	recordDescriptor		the descriptor about the record fields
///@param	data					the record in defaul format, assume that there is only one record in the data
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	if ((!recordDescriptor.empty() && !data) || (recordDescriptor.empty() && data))	return -1;

	int fieldCount = recordDescriptor.size();									// 	fields counter
	int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);	//	the size of null fields indicator in bytes
	char* nullFieldsIndicator = new char[nullFieldsIndicatorActualSize];
	memcpy(nullFieldsIndicator, (char*)data, nullFieldsIndicatorActualSize);

	int offset = nullFieldsIndicatorActualSize;	//	the start of the actual value

	bool nullBit = false;
	for (int i = 0; i < fieldCount; i += CHAR_BIT) {	// process the null fields indicator byte by byte
		for (int j = i; j < i + CHAR_BIT && j < fieldCount; j++) {	// process the null fields byte bit by bit
			nullBit = nullFieldsIndicator[j / CHAR_BIT] & (1 << (7 - (j % CHAR_BIT)));
			cout << recordDescriptor[j].name << ": ";
			if (!nullBit) {
				if (recordDescriptor[j].type == TypeVarChar) {	//	process TypeVarChar
					//	get the charLength
					int charLength = 0;
					memcpy(&charLength, (char*)data + offset, sizeof(int));

					offset += sizeof(int);

					//	get the char content
					char* fieldValue = new char[charLength + 1];
					memcpy(fieldValue, (char*)data + offset, charLength);
					fieldValue[charLength] = '\0';
					offset += charLength;
					const char* cchar = (char*)fieldValue;
					string varChar(cchar);
					cout << varChar;

					delete []fieldValue;
				}
				if (recordDescriptor[j].type == TypeInt) {		//	process TypeInt
					int fieldValue = 0;
					memcpy(&fieldValue, (char*)data + offset, recordDescriptor[j].length);
					offset += recordDescriptor[j].length;
					cout << fieldValue;
				}
				if (recordDescriptor[j].type == TypeReal) {		//	process TypeReal
					float fieldValue = 0.0;
					memcpy(&fieldValue, (char*)data + offset, recordDescriptor[j].length);
					offset += recordDescriptor[j].length;
					cout << fieldValue;
				}

			}
			else	cout << "NULL";
			cout << "    ";
		}
	}
	cout << endl;
	return 0;
}

///@brief	get the length of one record in default format, including its null-fields indicator and actual data
///@param	recordDescriptor		the descriptor about the record fields
///@param	data					the record, assume that there is only one record in the data
///@param	recordActualSize		the actual length of the record in bytes
///@return 	if the record is valid, return 0; else return -1
RC RecordBasedFileManager::getRecordActualSize(const vector<Attribute> &recordDescriptor, const void* data, int& recordActualSize) {
	if ((!recordDescriptor.empty() && !data) || (recordDescriptor.empty() && data))	return -1;

	int fieldCount = recordDescriptor.size();		// fields counter
	int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);	//	the size of null fields indicator in bytes
	char* nullFieldsIndicator = new char[nullFieldsIndicatorActualSize];
	memcpy(nullFieldsIndicator, (char*)data, nullFieldsIndicatorActualSize);

	int offset = nullFieldsIndicatorActualSize;	//	the start of the actual value

	for (int i = 0; i < fieldCount; i += CHAR_BIT) {	// process the null fields indicator byte by byte
		bool nullBit = false;
		for (int j = i; j < i + CHAR_BIT && j < fieldCount; j++) {	// process the null fields byte bit by bit
			nullBit = nullFieldsIndicator[j / CHAR_BIT] & (1 << (7 - (j % CHAR_BIT)));
			if (!nullBit) {
				if (recordDescriptor[j].type == TypeVarChar) {
					int charLength = 0;
					memcpy(&charLength, (char*)data + offset, sizeof(int));
					offset += sizeof(int) + charLength;
				}
				if (recordDescriptor[j].type == TypeInt) {
					offset += sizeof(int);
				}
				if (recordDescriptor[j].type == TypeReal) {
					offset += sizeof(float);
				}
			}
		}
	}
	recordActualSize = offset;
	return 0;
}

///@brief	create a new page and store the record into it
///@param	fileHandle			the handler of the file
///@param	recordDescriptor	the descriptor about the record's fields
///@param	data				the pointer to the record
///@param	internalRecordLen	the length of the record
///@param	rid					the series number of the record
///@return	if the record can be inserted correctly, return 0, else return -1
RC RecordBasedFileManager::createNewPageAndInsertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const int &internalRecordLen, RID &rid) {
	if ((!recordDescriptor.empty() && !data) || (recordDescriptor.empty() && data))	return -1;

	char* newPage = new char[PAGE_SIZE];
	memset(newPage, 0, PAGE_SIZE);

	int slotCount = 1;
	int recordDataSize = internalRecordLen;
	int slotTableSize = slotCountLen + recordDataSizeLen + slotTableSizeLen + slotLen;
	int recordLen = internalRecordLen;
	int newSlotId =  0;
	int recordStartOffset = 0;

	//	update the slotCount in the slot table
	memcpy(newPage + PAGE_SIZE - slotCountLen, &slotCount, slotCountLen);

	//  update the recordDataLen in the slot table
	memcpy(newPage + PAGE_SIZE - slotCountLen - recordDataSizeLen, &recordDataSize, recordDataSizeLen);

	//	update the size of slot table
	memcpy(newPage + PAGE_SIZE - slotCountLen - recordDataSizeLen - slotTableSizeLen, &slotTableSize, slotTableSizeLen);

	//	update the slot id
	memcpy(newPage + PAGE_SIZE - slotTableSize, &newSlotId, slotIdLen);

	//	update the record length
	memcpy(newPage + PAGE_SIZE - slotTableSize + slotIdLen, &recordLen, recordSizeLen);

	//	update the start offset of the new record
	memcpy(newPage + PAGE_SIZE - slotTableSize + slotIdLen + recordSizeLen, &recordStartOffset, recordOffsetLen);

	//	update the record area
	char* memBlock = new char[recordLen];
	memcpy(memBlock, data, recordLen);
	memcpy(newPage, memBlock, recordLen);
	delete []memBlock;

	//	write the new page into the disk
	int rc = fileHandle.appendPage(newPage);
	if (rc == 0 && "append page failed.");
	delete []newPage;

	//	update the rid
	rid.pageNum = fileHandle.pagesCounter - 1;
	rid.slotNum = newSlotId;
	return 0;
}

///@brief		transform the data given default into the internal record structure
///@param		recordDescriptor	descriptor about the record fields
///@param		data				the record in default
///@param		internalRecord		the internal record format transformed from data
///@param		internalRecordLen	the length of the internal record
///@return		if the transformation successes, return 0. Else return -1
RC RecordBasedFileManager::transRecordFromDefaultToInternal(const vector<Attribute> &recordDescriptor, char* internalRecord, int& internalRecordLen, const void* data) {
	if ((!recordDescriptor.empty() && !data) || (recordDescriptor.empty() && data))	return -1;
	int fieldCount = recordDescriptor.size();
	int internalRecordHeadLen = recordFixedHeadLen + fieldCount * internalRecordHeadSlotLen;
	int internalRecordHeadSlotOffset = internalRecordHeadLen;				//	the offset used to fill the internalRecordHead
	char* internalRecordHead = new char[internalRecordHeadLen];
	memset(internalRecordHead, 0, internalRecordHeadLen);

	int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);	//	the size of null fields indicator in bytes
	char* nullFieldsIndicator = new char[nullFieldsIndicatorActualSize];
	memcpy(nullFieldsIndicator, (char*)data, nullFieldsIndicatorActualSize);

	int offset = nullFieldsIndicatorActualSize;	//	the start of the actual value in the default record



	char isInCurrentPage = 'y';
	int versionNumber = 0;
	int reservedBytes = 4;		//	reserved bytes are used to guarantee that the shortest record could be longer than the tomb stone
	memcpy(internalRecordHead, &isInCurrentPage, isInCurrentPageLen);
	memcpy(internalRecordHead + isInCurrentPageLen, &versionNumber, versionNumberLen);
	memcpy(internalRecordHead + isInCurrentPageLen + versionNumberLen, &reservedBytes, reservedBytesLen);
	char isNullField = 'z';		//	the flag to mark the field is whether null
	memcpy(internalRecordHead + recordFixedHeadLen + isNullFieldLen, &internalRecordHeadSlotOffset, fieldOffsetLen);	//	fill the first internal record head slot



	//	fill in the internal record head
	bool nullBit = false;
	for (int i = 0; i < fieldCount - 1; i += CHAR_BIT) {	// process the null fields indicator byte by byte
		for (int j = i; j < i + CHAR_BIT && j < fieldCount - 1; j++) {	// process the null fields byte bit by bit
			nullBit = nullFieldsIndicator[j / CHAR_BIT] & (1 << (7 - (j % CHAR_BIT)));
			if (!nullBit) {
				isNullField = 'n';
				if (recordDescriptor[j].type == TypeVarChar) {
					int charLength = 0;
					memcpy(&charLength, (char*)data + offset, sizeof(int));
					offset += sizeof(int) + charLength;
					internalRecordHeadSlotOffset += sizeof(int) + charLength;
				}
				if (recordDescriptor[j].type == TypeInt) {
					offset += sizeof(int);
					internalRecordHeadSlotOffset += sizeof(int);
				}
				if (recordDescriptor[j].type == TypeReal) {
					offset += sizeof(float);
					internalRecordHeadSlotOffset += sizeof(float);
				}
			}
			else {
				isNullField = 'y';
			}
			memcpy(internalRecordHead + recordFixedHeadLen + j * internalRecordHeadSlotLen, &isNullField, isNullFieldLen);
			memcpy(internalRecordHead + recordFixedHeadLen + (j + 1) * internalRecordHeadSlotLen + isNullFieldLen, &internalRecordHeadSlotOffset, fieldOffsetLen);
		}
	}

	/*
		the reason why process the last bit saparately is the head slot is used to store the field starting offset. In the previous loop,
		the last head slot has been filled, but the offset hasn't been updated. So we just update the offset here.
	*/

	//process the last bit in the record nullFieldsIndicator
	nullBit = nullFieldsIndicator[(fieldCount - 1) / CHAR_BIT] & (1 << (7 - ((fieldCount - 1) % CHAR_BIT)));
	if (!nullBit) {
		isNullField = 'n';
		if (recordDescriptor.back().type == TypeVarChar) {
			int charLength = 0;
			memcpy(&charLength, (char*)data + offset, sizeof(int));
			offset += sizeof(int) + charLength;
			internalRecordHeadSlotOffset += sizeof(int) + charLength;
		}
		if (recordDescriptor.back().type == TypeInt) {
			offset += sizeof(int);
			internalRecordHeadSlotOffset += sizeof(int);
		}
		if (recordDescriptor.back().type == TypeReal) {
			offset += sizeof(float);
			internalRecordHeadSlotOffset += sizeof(float);
		}
	}
	else {
		isNullField = 'y';
	}

	memcpy(internalRecordHead + recordFixedHeadLen + (fieldCount - 1) * internalRecordHeadSlotLen, &isNullField, isNullFieldLen);
	internalRecordLen = internalRecordHeadLen + (offset - nullFieldsIndicatorActualSize);

	char* internalRecordLocal = new char[internalRecordLen];
	memcpy(internalRecordLocal, internalRecordHead, internalRecordHeadLen);
	memcpy(internalRecordLocal + internalRecordHeadLen, data + nullFieldsIndicatorActualSize, offset - nullFieldsIndicatorActualSize);
	memcpy(internalRecord, internalRecordLocal, internalRecordLen);


	return 0;
}

///@brief		transform the internal record into the format given default
///@param		recordDescriptor	descriptor about the record fields
///@param		internalRecord		the internal record format transformed from data
///@param		internalRecordLen	the length of the internal record
///@param		data				the record in default
///@return		if the transformation successes, return 0. Else return -1
RC RecordBasedFileManager::transRecordFromInternalToDefault(const vector<Attribute> &recordDescriptor, void* data, const char* internalRecord, const int &internalRecordLen) {
	if ((!recordDescriptor.empty() && !internalRecord) || (recordDescriptor.empty() && internalRecord))	return -1;

	int fieldCount = recordDescriptor.size();
	int internalRecordHeadLen = recordFixedHeadLen + fieldCount * internalRecordHeadSlotLen;
	char isNullField = 'z';
	char* internalRecordHead = new char[internalRecordHeadLen];
	memcpy(internalRecordHead, internalRecord, internalRecordHeadLen);

	int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);	//	the size of null fields indicator in bytes
	char* nullFieldsIndicator = new char[nullFieldsIndicatorActualSize];
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);

	//	fill in the nullFieldsIndicator in the default record head
	bool nullBit = false;
	for (int i = 0; i < fieldCount; i += CHAR_BIT) {	// process the internal record head byte by byte
		for (int j = i; j < i + CHAR_BIT && j < fieldCount; j++) {	// process the null fields byte bit by bit
			memcpy(&isNullField, internalRecordHead + recordFixedHeadLen + j * internalRecordHeadSlotLen, isNullFieldLen);

			if (isNullField == 'y')	nullBit = true;
			if (isNullField == 'n')	nullBit = false;

			if (nullBit)
				nullFieldsIndicator[j / CHAR_BIT] |= 1 << (7 - (j % CHAR_BIT));
			else
				nullFieldsIndicator[j / CHAR_BIT] &= ~(1 << (7 - (j % CHAR_BIT)));
		}
	}
	memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	memcpy(data + nullFieldsIndicatorActualSize, internalRecord + internalRecordHeadLen, internalRecordLen - internalRecordHeadLen);
	delete []nullFieldsIndicator;
	delete []internalRecordHead;
	return 0;
}

///@brief	delete the record whose rid has been given
///@param	fileHandle			the handle to access the file
///@param	recordDescriptor	the description about the fields in the record
///@param	rid					the record id
///@return	if the record could be deleted successfully, return 0. Else return -1
///@note	1. the size of slot table only grows, never decrease
//			2. the slot table is designed like this
/*
 *------------------------------------------------------ Slot Table -------------------------------------------------
 *|                        Slots                           |                        Slot Table Head                 |
 *|--------------------------------------------------------|--------------------------------------------------------|
 *|  ....  | Slot Id | Record Length | Record Start Offset | Slot Table Size | Records Data Size | Valid Slot Count |
 *|--------|---------|---------------|---------------------|-----------------|-------------------|------------------|
 *|        | 2 Bytes |    2 Bytes    |      2 Bytes        |      2 Bytes    |       2 Bytes     |     2 Bytes      |
 *|--------|-----------------------------------------------|-----------------|-------------------|------------------|
 *|        |               6 Bytes                         |                         6 Bytes                        |
 *|--------------------------------------------------------|--------------------------------------------------------|
 *|           Slots Number * 6 Bytes                       |                         6 Bytes                        |
 *-------------------------------------------------------------------------------------------------------------------
 * 																											 Page End
 *
 */
//			3. the internal record is designed like this
/*-------------------------------------------------- Internal Record --------------------------------------------------------------------
 *|                                       Record Head                                  |                   Record Data                  |
 *|------------------------------------------------------------------------------------|------------------------------------------------|
 *|  FLAG: isInCurrentPage  |  Version Number | Reserved Bytes |isNull| Field i Offset | Integer |  Float  |            VarChar         |
 *|-------------------------|-----------------|----------------|------|----------------|---------|---------|----------------------------|
 *|       1 Byte            |    2 Bytes      |    4 Bytes     |1 Byte|    2 Bytes     | 4 Bytes | 4 Bytes | VarCharLength |  String    |
 *|------------------------------------------------------------------------------------|------------------------------------------------|
 *|                             7 Bytes + Fields Number * 3 Bytes                      | 4 Bytes | 4 Bytes |    4 Bytes    |  n Bytes   |
 *|--------------------------------------------------------------------------------------------------------------------------------------
 *Page Start
 */
//			4. if the record isn't stored in this page, the version number also doesn't exist. The record only stored the FLAG: isInCurrentPage
//			and follow that flag there is another rid.
//			5. if the record which is a tomb stone is deleted, the slot counter in the slot table head should be decrease. Or this record
//			   will be count twice or more.
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
	if (fileHandle.fstr.is_open()) {
		char* page = new char[PAGE_SIZE];
		fileHandle.readPage(rid.pageNum, page);

		int slotTableSize = 0;
		int recordDataSize = 0;
		int slotQuantity = 0;
		int slotIndex = 0;
		int slotId = 0;
		int recordLen = 0;
		int recordStartOffset = 0;
		int slotCount = 0;

		//	get the valid slot counter
		memcpy(&slotCount, page + PAGE_SIZE - slotCountLen, slotCountLen);

		//	get the actual record data size
		memcpy(&recordDataSize, page + PAGE_SIZE - slotTableHeadLen + slotCountLen, recordDataSizeLen);

		//	get the total slot number, include invalid ones
		memcpy(&slotTableSize, page + PAGE_SIZE - slotTableHeadLen, slotTableSizeLen);
		slotQuantity = (slotTableSize - slotTableHeadLen) / slotLen;
		for (slotIndex = 0; slotIndex < slotQuantity; slotIndex++) {
			memcpy(&slotId, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen), slotIdLen);
			memcpy(&recordLen, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen, recordSizeLen);
			memcpy(&recordStartOffset, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen + recordSizeLen, recordOffsetLen);

			if (slotId == rid.slotNum && recordLen != 0 && recordStartOffset != INVALID_ITEM) {
				//	get the record
				char* record = new char[recordLen];
				memcpy(record, page + recordStartOffset, recordLen);
				//	get the flag about the record whether in current page
				char isInCurrentPage = 'z';	//	unkown, initialize to 'z'
				memcpy(&isInCurrentPage, record, isInCurrentPageLen);


				//	if the record is only left a tomb stone in this page
				if (isInCurrentPage == 'n') {
					RID tombStone;
					tombStone.pageNum = 0;
					tombStone.slotNum = 0;
					memcpy(&tombStone, page + recordStartOffset + isInCurrentPageLen, sizeof(RID));
					int rc = 0;
					rc = deleteRecord(fileHandle, recordDescriptor, tombStone);
					assert(rc == 0 && "delete the record failed.");
				}
				//	delete the record, no matter it is a tomb stone or actual record.
				//	shift the records behind the record to the start end
				memcpy(page + recordStartOffset, page + recordStartOffset + recordLen, recordDataSize - (recordStartOffset + recordLen));
				//	reset the tail space to 0
				memset(page + recordDataSize - recordLen, 0, recordLen);

				//	update the slotCount in the slot table head, which represent the valid slot number.
				slotCount--;
				memcpy(page + PAGE_SIZE - slotCountLen, &slotCount, slotCountLen);

				//	update the record area size in the slot table head
				recordDataSize -= recordLen;
				memcpy(page + PAGE_SIZE - slotCountLen - recordDataSizeLen, &recordDataSize, recordDataSizeLen);

				//	update other affected slots
				for (int j = 0; j < slotQuantity; j++) {
					int affectedRecordStartOffset = 0;
					memcpy(&affectedRecordStartOffset, page + PAGE_SIZE - slotTableHeadLen - (j + 1) * slotLen + slotIdLen + recordSizeLen, recordOffsetLen);
					if (affectedRecordStartOffset > recordStartOffset) {
						affectedRecordStartOffset -= recordLen;
						memcpy(page + PAGE_SIZE - slotTableHeadLen - (j + 1) * slotLen + slotIdLen + recordSizeLen, &affectedRecordStartOffset, recordOffsetLen);
					}
				}

				//	update the record's length in the slot
				recordLen = 0;
				memcpy(page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen, &recordLen, recordSizeLen);

				//	update the record start offset in the slot
				recordStartOffset = INVALID_ITEM;
				memcpy(page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen + recordSizeLen, &recordStartOffset, recordOffsetLen);

				fileHandle.writePage(rid.pageNum, page);
				delete []page;
				return 0;
			}
		}
	}
	return -1;
}

///@brief	update the record
///@param	fileHandle			the handler of file
///@param	recordDescriptor	the description of the record
///@param	data				the record in the default format. So before write into disk, we should transform it first.
///@param	rid					the record slot number and page number
///@return	if the record can be updated correctly, return 0. Else return -1.
///@note	1. if the new incrementing length of the record over the free space, we should at first find a page has enough space to hold this
//			   record, then change the original record into a tomb stone.
//			2. according to the new version's length, the other records might be shift forward or backward.
//			3. the slots, including the other affected slots and the slot table header should be updated too.
//			4. the data given in the parameter is in the default format, so we should transform it into internal one.
//			5. if the record becomes a tomb stone, the slot counter in the slot table header should be decreased 1.
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
	int recordLen = 0;							//	the length of the record
	int rc = 0;											//	the return code for the functions in this project
	char* record = new char[PAGE_SIZE];			//	the internal record format

	//	transform the record's format from default into internal
	rc = transRecordFromDefaultToInternal(recordDescriptor, record, recordLen, data);
	assert(rc == 0 && "transform failed.");

	if (fileHandle.fstr.is_open()) {
		char* page = new char[PAGE_SIZE];
		rc = fileHandle.readPage(rid.pageNum, page);
		assert(rc == 0 && "read page failed.");

		int freeSpaceSize = 0;
		int recordDataSize = 0;
		int slotTableSize = 0;
		int recordStartOffset = 0;
		int slotCount = 0;
		int originalRecordLen = 0;
		int slotQuantity = 0;
		int slotId = 0;

		//	calculate the free space size
		memcpy(&slotTableSize, page + PAGE_SIZE - slotTableHeadLen, slotTableSizeLen);
		memcpy(&recordDataSize, page + PAGE_SIZE - slotCountLen - recordDataSizeLen, recordDataSizeLen);
		freeSpaceSize = PAGE_SIZE - recordDataSize - slotTableSize;



		//	get the slot quantities
		slotQuantity = (slotTableSize - slotTableHeadLen) / slotLen;

		//	find the slot and get the original record length and record start offset
		int slotIndex = 0;
		for (slotIndex = 0; slotIndex < slotQuantity; slotIndex++) {
			memcpy(&slotId, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen), slotIdLen);
			if (slotId == rid.slotNum) {
				memcpy(&originalRecordLen, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen, recordSizeLen);
				memcpy(&recordStartOffset, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen + recordSizeLen, recordOffsetLen);
				break;
			}
		}

		bool isShrink = recordLen <= originalRecordLen ? true : false;

		//	if the record shrinks, we can update the record in this page
		if (isShrink) {
			rc = updateRecordInCurrentPage(fileHandle, page, record, recordStartOffset, recordLen, originalRecordLen, recordDataSize, slotIndex, slotQuantity, rid);
			assert(rc == 0 && "update record failed.");
		}
		//	if the record extends, there are two situations. the first one is the free space in current page is enough, so we just update
		//	it. the second situation is we have to find another page with enough free space to store the new record. we should insert in
		//	another page first and then update the record in current page.
		else {
			if (freeSpaceSize + originalRecordLen >= recordLen) {
				rc = updateRecordInCurrentPage(fileHandle, page, record, recordStartOffset, recordLen, originalRecordLen, recordDataSize, slotIndex, slotQuantity, rid);
				assert(rc == 0 && "update record failed.");
			}
			else {
				RID tombStone;
				tombStone.pageNum = 0;
				tombStone.slotNum = 0;
				rc = insertRecord(fileHandle, recordDescriptor, data, tombStone);
				assert(rc == 0 && "insert record failed.");

				//	assume the tomb stone size(9 bytes) is always less than the valid record
				int tombStoneLen = isInCurrentPageLen + sizeof(RID);

				//	set the tomb stone
				char isInCurrentPage = 'n';
				char* tombStoneRecord = new char[tombStoneLen];
				memcpy(tombStoneRecord, &isInCurrentPage, isInCurrentPageLen);
				memcpy(tombStoneRecord + isInCurrentPageLen, &tombStone, sizeof(RID));

				rc = updateRecordInCurrentPage(fileHandle, page, tombStoneRecord, recordStartOffset, tombStoneLen, originalRecordLen, recordDataSize, slotIndex, slotQuantity, rid);
				assert(rc == 0 && "update record failed.");
				delete []tombStoneRecord;
			}
		}
		delete []page;
		return 0;
	}

	return -1;
}

///@brief	update the record in current page
///@param	fileHandle				file handler
///@param	record					the new version of record
///@param	recordStartOffset		the record start offset
///@param	recordLen				the new record's length
///@param	originalRecordLen		the original record's length
///@param	slotIndex				the slot index, which is in ascending order and is different with slot id
RC RecordBasedFileManager::updateRecordInCurrentPage(FileHandle &fileHandle,
		char* page,
		char* record,
		int &recordStartOffset,
		int &recordLen,
		int &originalRecordLen,
		int &recordDataSize,
		int &slotIndex,
		int &slotQuantity,
		const RID &rid)
{
	if (fileHandle.fstr.is_open()) {
		//	I am so stupid!!!!hahahahah!!!
		//	memcpy is operated on the same memory, of course it will be wrong!!!!! I'm a genius!!fuck!!
		//	shift the behind records
		//	memcpy(page + recordStartOffset + recordLen, page + recordStartOffset + originalRecordLen, recordDataSize - (recordStartOffset + originalRecordLen));

		//	shift the behind records
		char* tmpPage = new char[PAGE_SIZE];
		memcpy(tmpPage, page + recordStartOffset + originalRecordLen, recordDataSize - (recordStartOffset + originalRecordLen));
		memcpy(page + recordStartOffset + recordLen, tmpPage, recordDataSize - (recordStartOffset + originalRecordLen));
		delete []tmpPage;

		if (recordLen <= originalRecordLen)
			memset(page + recordDataSize - (originalRecordLen - recordLen), 0, originalRecordLen - recordLen);

		//	update the record
		memcpy(page + recordStartOffset, record, recordLen);

		//	update the current record length in the slot table
		memcpy(page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen, &recordLen, recordSizeLen);

		//	update all the other records start offset in the slot table, which are affected by the shift
		for (slotIndex = 0; slotIndex < slotQuantity; slotIndex++) {
			int affectedRecordStartOffset = 0;
			memcpy(&affectedRecordStartOffset, page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen + recordSizeLen, recordOffsetLen);
			if (affectedRecordStartOffset > recordStartOffset) {
				affectedRecordStartOffset += recordLen - originalRecordLen;
				memcpy(page + PAGE_SIZE - (slotTableHeadLen + (slotIndex + 1) * slotLen) + slotIdLen + recordSizeLen, &affectedRecordStartOffset, recordOffsetLen);
			}
		}
		//	update the record size in the slot table header
		recordDataSize += recordLen - originalRecordLen;
		memcpy(page + PAGE_SIZE - slotCountLen - recordDataSizeLen, &recordDataSize, recordDataSizeLen);
		fileHandle.writePage(rid.pageNum, page);
		return 0;
	}
	return -1;
}

///@brief	read a specific field in one record
///@param	fileHandle				file handler
///@param	recordDescriptor		the description of record
///@param	rid						the record id
///@param	attributeName			the name of attribute
///@param	data					the data of the field
///@return	if the operation can be executed successfully, then return 0.
//			if the field is null, then return -2.
//			if the file can't open, return -1
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
	if (fileHandle.fstr.is_open()) {
		int fieldNum = -1;
		for (int i = 0; i < recordDescriptor.size(); i++) {
			if (recordDescriptor[i].name == attributeName) {
				fieldNum = i;
				break;
			}
		}
		if (fieldNum == -1)	return -1;

		char* record = new char[PAGE_SIZE];
		char* defaultRecord = new char[PAGE_SIZE];
		int recordLen = 0;
		int rc = readRecord(fileHandle, recordDescriptor, rid, defaultRecord);
		assert(rc == 0 && "read record faild.");
		//	transform the record into internal mode
		rc = transRecordFromDefaultToInternal(recordDescriptor, record, recordLen, defaultRecord);

		int fieldOffset = 0;
		char isNullField = 'z';
		memcpy(&isNullField, record + recordFixedHeadLen + fieldNum * recordHeadSlotLen, isNullFieldLen);

		if (isNullField == 'y') {
			return -2;
		}
		if (isNullField == 'n') {
			int nullIndicator = 0;
			memcpy(data, &nullIndicator, 1);	//	there is only one field need to be returned
			memcpy(&fieldOffset, record + recordFixedHeadLen + fieldNum * recordHeadSlotLen + isNullFieldLen, fieldOffsetLen);
			if (recordDescriptor[fieldNum].type == TypeInt || recordDescriptor[fieldNum].type == TypeReal)
				memcpy(data + 1, record + fieldOffset, 4);
			else if (recordDescriptor[fieldNum].type == TypeVarChar) {
				int chLen = -1;
				memcpy(&chLen, record + fieldOffset, 4);
				memcpy(data + 1, record + fieldOffset, 4 + chLen);
			}
		}
		delete []record;
		return 0;
	}
	return -1;
}

///@brief		Scan returns an iterator to allow the caller to go through the results one by one.
///@param	fileHandle			file handler
///@param	recordDescriptor	the description for the record fields
///@param	conditionAttribute	the attribute to filter records
///@param	compOp				the operator for filter
///@param	value				the filter value
///@param	attributeNames		the fields left for the record filtered out
///@param	rbfm_ScanIterator	the output. It will be used to iterate the filtered record
///@return 	if it can output the iterator successfully, returns 0. Else return -1.
///@note
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute,
		const CompOp compOp,                  // comparision type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator)
{
	if (fileHandle.fstr.is_open()) {
		rbfm_ScanIterator.recordDescriptor = recordDescriptor;
		rbfm_ScanIterator.conditionAttribute = conditionAttribute;
		rbfm_ScanIterator.compOp = compOp;
		rbfm_ScanIterator.value = value;
		rbfm_ScanIterator.attributeNames = attributeNames;

		rbfm_ScanIterator.fileHandle.pagesCounter = fileHandle.pagesCounter;
		rbfm_ScanIterator.fileHandle.isAllocated = fileHandle.isAllocated;
		rbfm_ScanIterator.fileHandle.specifiedFileName = fileHandle.specifiedFileName;
		rbfm_ScanIterator.fileHandle.fstr.open(rbfm_ScanIterator.fileHandle.specifiedFileName, ios::in | ios::out | ios::binary);
		rbfm_ScanIterator.fileHandle.readPageCounter = fileHandle.readPageCounter;
		rbfm_ScanIterator.fileHandle.writePageCounter = fileHandle.writePageCounter;
		rbfm_ScanIterator.fileHandle.appendPageCounter = fileHandle.appendPageCounter;

		return 0;
	}
	return -1;
}

RBFM_ScanIterator::RBFM_ScanIterator() {
	  this->conditionAttribute = "";
	  this->compOp = NO_OP;
	  this->value = nullptr;
	  this->startPageId = 0;
	  this->startSlotIndex = 0;
	  this->rbfm->instance();

	  slotCountLen = sizeof(unsigned short);		//	the size of slotCount
	  slotIdLen = sizeof(unsigned short);			//	the size of the slotId
	  recordSizeLen = sizeof(unsigned short);		//	the size of the record's size
	  recordOffsetLen = sizeof(unsigned short);	//	the size of record offset
	  slotLen = slotIdLen + recordSizeLen + recordOffsetLen;		//	the size of slot
	  isNullFieldLen = sizeof(char);				//	the size of the flag about this field whether is null
	  internalRecordHeadSlotLen = sizeof(unsigned short) + isNullFieldLen;			//	the size of the internal record head entry
	  isInCurrentPageLen = sizeof(char);			//	the size of the flag byte for the internal record. If it is 'y', the internal record is
	  //	in current page. Else if it is 'n', the internal record is in another page.
	  versionNumberLen = sizeof(unsigned short);	//  the size of the version number of the internal record

	  recordDataSizeLen = sizeof(unsigned short);		//	the size of the length of all the internal records
	  slotTableSizeLen = sizeof(unsigned short);		//	the size of the length of the slot table
	  slotTableHeadLen = slotCountLen + recordDataSizeLen + slotTableSizeLen;					//	the size of the slot table head
	  reservedBytesLen = sizeof(int);					//	the size of reserved bytes in the record head
	  recordFixedHeadLen = isInCurrentPageLen + versionNumberLen + reservedBytesLen;			//	the fixed size of the record head
	  fieldOffsetLen = sizeof(unsigned short);		//	the size of the field offset in the record head
	  recordHeadSlotLen = internalRecordHeadSlotLen;  //	the size of the internal record head slot
	  fieldLen = isNullFieldLen + fieldOffsetLen;					//	the size of the field
};

RBFM_ScanIterator::~RBFM_ScanIterator() {

};

///@brief	get the next record filtered
///@param	rid			the filtered record id
///@param	data		the filter record, which must satisfies the attributeNames, the format is default.
///@return	if get the filtered record successfully, return 0; else return -1
///@note	1. don't consider the situation of tomb stone, just iterate the pages one by one. if a record is the tomb stone, don't output.
//			2. the iteration is as the order of slot offset from start to the end of the slot table, not as the order of slot id. Because
//			   they will change after deletion and insertion.
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
	int rc = 0;
	if (fileHandle.fstr.is_open()) {
		char* page = new char[PAGE_SIZE];
		int pageId = startPageId;

		while (pageId < fileHandle.pagesCounter) {

			rc = fileHandle.readPage(pageId, page);
			assert(rc == 0 && "read page should not fail.");

			int slotTableSize = 0;
			int slotQuantity = 0;
			int recordStartOffset = 0;
			int recordLen = 0;
			int slotId = 0;
			int slotIndex = startSlotIndex;

			memcpy(&slotTableSize, page + PAGE_SIZE - slotTableHeadLen, slotTableSizeLen);
			slotQuantity = (slotTableSize - slotTableHeadLen) / slotLen;
			for (int i = slotIndex; i < slotQuantity; i++) {

				memcpy(&slotId, page + PAGE_SIZE - slotTableHeadLen - (i + 1) * slotLen, slotIdLen);

				memcpy(&recordLen, page + PAGE_SIZE - slotTableHeadLen - (i + 1) * slotLen + slotIdLen, recordSizeLen);

				memcpy(&recordStartOffset, page + PAGE_SIZE - slotTableHeadLen - (i + 1) * slotLen + slotIdLen + recordSizeLen, recordOffsetLen);

				//	the record is invalid
				if (recordLen == 0 || recordStartOffset == INVALID_ITEM)	continue;
				//	the record is a tomb stone
				char isInCurrentPage = 'z';
				memcpy(&isInCurrentPage, page + recordStartOffset, isInCurrentPageLen);

				if (isInCurrentPage == 'n')	continue;

				//	normal records
				char* record = new char[recordLen];
				memcpy(record, page + recordStartOffset, recordLen);

				bool satf = isSatisfied(record, this->recordDescriptor, this->conditionAttribute, this->compOp, this->value);

				if (satf) {
					//	output the projected fields as data
					getProjectedRecord(record, recordLen, this->recordDescriptor, this->attributeNames, data);

					//	output the rid
					rid.pageNum = pageId;
					rid.slotNum = slotId;

					//	update the startPageId and startSlotIndex
					if (i == slotQuantity - 1) {
						this->startPageId = pageId + 1;
						this->startSlotIndex = 0;
					}
					else if (i < slotQuantity - 1) {
						this->startPageId = pageId;
						this->startSlotIndex = i + 1;
					}
					delete []page;
					delete []record;
					return 0;
				}
				delete []record;
			}
			pageId++;
		}
		delete []page;
	}
	return -1;
}

///@brief		check whether this record satisfies the filter
///@param		record				the record in the page
///@param		recordDescriptor	the description for the record fields
///@param		conditionAttribute	the attribute to filter records
///@param		compOp				the operator for filter
///@param		value				the filter value
///@return		if the record satisfies the filter, return true. Else return false.
bool RBFM_ScanIterator::isSatisfied(char* record, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value) {
	//	to store different types of void* value
	int intVal = 0, intRec = 0;
	float floatVal = 0, floatRec = 0;
	string strVal, strRec;
	int fieldOffset = 0;

	if (compOp == NO_OP)	return true;

	for (int i = 0; i < recordDescriptor.size(); i++) {
		if (recordDescriptor[i].name == conditionAttribute) {
			char isNull = 'z';
			memcpy(&isNull, record + recordFixedHeadLen + i * fieldLen, isNullFieldLen);

			if (isNull == 'y')	return false;

			memcpy(&fieldOffset, record + recordFixedHeadLen + i * fieldLen + isNullFieldLen, fieldOffsetLen);

			if (recordDescriptor[i].type == TypeInt) {
				memcpy(&intVal, value, sizeof(int));
				memcpy(&intRec, record + fieldOffset, sizeof(int));

				if ((compOp == EQ_OP && intRec == intVal) ||
				    (compOp == LT_OP && intRec < intVal)  ||
				    (compOp == LE_OP && intRec <= intVal) ||
				    (compOp == GT_OP && intRec > intVal)  ||
				    (compOp == GE_OP && intRec >= intVal) ||
				    (compOp == NE_OP && intRec != intVal)
				   )   	return true;
				else	return false;
			}
			else if (recordDescriptor[i].type == TypeReal) {
				memcpy(&floatVal, value, sizeof(float));
				memcpy(&floatRec, record + fieldOffset, sizeof(float));

				if ((compOp == EQ_OP && floatRec == floatVal) ||
					(compOp == LT_OP && floatRec < floatVal)  ||
					(compOp == LE_OP && floatRec <= floatVal) ||
					(compOp == GT_OP && floatRec > floatVal)  ||
					(compOp == GE_OP && floatRec >= floatVal) ||
					(compOp == NE_OP && floatRec != floatVal)
				)   	return true;
				else	return false;
			}
			else if (recordDescriptor[i].type == TypeVarChar) {

				string tmpVal((const char*)value);
				strVal = tmpVal;

				int charLen = 0;
				memcpy(&charLen, record + fieldOffset, sizeof(int));

				char* chRec = new char[charLen+1];
				memcpy(chRec, record + fieldOffset + sizeof(int), charLen);

				chRec[charLen] = '\0';
				string tmpRec(chRec);
				strRec = tmpRec;
				delete []chRec;

				if ((compOp == EQ_OP && strRec == strVal) ||
					(compOp == LT_OP && strRec < strVal)  ||
					(compOp == LE_OP && strRec <= strVal) ||
					(compOp == GT_OP && strRec > strVal)  ||
					(compOp == GE_OP && strRec >= strVal) ||
					(compOp == NE_OP && strRec != strVal)
				)   	return true;
				else	return false;
			}
		}
	}
	return false;
}


///@brief		output the projected fields with internal record
///@param		record				the internal record in the page
///@param		recordLen			the length of the record
///@param		recordDescriptor	the description of the record's field
///@param		attributeNames		the projected fields
///@param		data				the projected record in default format
void RBFM_ScanIterator::getProjectedRecord(char* record, int recordLen, const vector<Attribute> &recordDescriptor, vector<string> attributeNames, void* data) {
	int fieldCount = recordDescriptor.size();
	int internalRecordHeadLen = recordFixedHeadLen + fieldCount * internalRecordHeadSlotLen;
	char isNullField = 'z';
	char* internalRecordHead = new char[internalRecordHeadLen];
	memcpy(internalRecordHead, record, internalRecordHeadLen);

	//	the size of null fields indicator in bytes
	int nullFieldsIndicatorActualSize = ceil((double) attributeNames.size() / CHAR_BIT);
	char* nullFieldsIndicator = new char[nullFieldsIndicatorActualSize];
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);

	bool nullBit = false;
	char* filterData = new char[recordLen];
	int filterDataOffset = 0;
	int filterDataLen = 0;

	for (int i = 0; i < recordDescriptor.size(); i += CHAR_BIT) {
		for (int j = i; j < i + CHAR_BIT && j < recordDescriptor.size(); j++) {
			if (isProjected(recordDescriptor[j], attributeNames)) {
				memcpy(&isNullField, internalRecordHead + recordFixedHeadLen + j * internalRecordHeadSlotLen, isNullFieldLen);
				if (isNullField == 'y')	nullBit = true;
				if (isNullField == 'n')	nullBit = false;
				if (nullBit)	nullFieldsIndicator[j / CHAR_BIT] |= 1 << (7 - (j % CHAR_BIT));
				else			nullFieldsIndicator[j / CHAR_BIT] &= ~(1 << (7 - (j % CHAR_BIT)));

				int fieldOffset = 0;
				memcpy(&fieldOffset, internalRecordHead + recordFixedHeadLen + j * internalRecordHeadSlotLen + isNullFieldLen, fieldOffsetLen);

				if (nullBit) continue;
				else {
					if (recordDescriptor[j].type == TypeInt) {
						memcpy(filterData + filterDataOffset, record + fieldOffset, sizeof(int));
						filterDataLen += sizeof(int);
						filterDataOffset += sizeof(int);
					}
					else if (recordDescriptor[j].type == TypeReal) {
						memcpy(filterData + filterDataOffset, record + fieldOffset, sizeof(float));
						filterDataLen += sizeof(float);
						filterDataOffset += sizeof(float);
					}
					else if (recordDescriptor[j].type == TypeVarChar) {
						int charLen = 0;
						memcpy(&charLen, record + fieldOffset, sizeof(int));
						memcpy(filterData + filterDataOffset, record + fieldOffset, sizeof(int) + charLen);
						filterDataLen += sizeof(int) + charLen;
						filterDataOffset += sizeof(int) + charLen;
					}
				}
			}
		}
	}

	memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	memcpy(data + nullFieldsIndicatorActualSize, filterData, filterDataLen);

	delete []internalRecordHead;
	delete []nullFieldsIndicator;
	delete []filterData;
}

///@brief		judge whether this attribute is the projected attribute
///@param		recordDescriptor
///@return		if the record's attribute is in the attributeNames, return true; Else return false.
bool RBFM_ScanIterator::isProjected(Attribute recordAttribute, vector<string> attributeNames) {
	for (int i = 0; i < attributeNames.size(); i++) {
		if (attributeNames[i] == recordAttribute.name)
			return true;
	}
	return false;
}

///@brief	close the iterator
RC RBFM_ScanIterator::close() {
	this->rbfm->pfm->closeFile(this->fileHandle);
	if (this->fileHandle.fstr.is_open())	return -1;

	return 0;
}



















