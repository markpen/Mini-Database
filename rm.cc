
#include "rm.h"

#include <string>
#include <string.h>
#include <assert.h>
#include <iostream>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	tableSize = sizeof(int) + sizeof(char) * 50 + sizeof(char) * 50; //the size of tables in catalog
	columnSize = sizeof(int) + sizeof(char) * 50 + sizeof(int) + sizeof(int); //size of columns in catalog
	startTableId = 0;

	//	initialize the start table id. Iterate all the entries in the "Tables" to find the biggest table-id
    RM_ScanIterator rmsi;
    string attr = "table-id";
    vector<string> attributes;
    attributes.push_back(attr);
    int rc = this->scan("Tables", "", NO_OP, NULL, attributes, rmsi);

    RID rid;
    char* tableIds = new char[1 + sizeof(int)];

    while(rmsi.getNextTuple(rid, tableIds) != RM_EOF) {
    	memcpy(&startTableId, tableIds + 1, sizeof(int));
    }
    rmsi.close();
    delete []tableIds;
}

RelationManager::~RelationManager()
{
}

///@brief	create the "Tables" recordDescriptor
RC RelationManager::attributeTable(vector<Attribute> &recordDescriptor){
	Attribute attribute;
	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 1;
	recordDescriptor.push_back(attribute);
	attribute.name = "table-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attribute.position = 2;
	recordDescriptor.push_back(attribute);
	attribute.name = "file-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attribute.position = 3;
	recordDescriptor.push_back(attribute);
	attribute.name = "system-table";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 4;
	recordDescriptor.push_back(attribute);
	return 0;
}

///@brief	create "Columns" recordDescriptor
RC RelationManager::attributeColumn(vector<Attribute> &recordDescriptor){
	Attribute attribute;
	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = 4;
	attribute.position = 1;
	recordDescriptor.push_back(attribute);

	attribute.name = "column-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attribute.position = 2;
	recordDescriptor.push_back(attribute);

	attribute.name = "column-type";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 3;
	recordDescriptor.push_back(attribute);

	attribute.name = "column-length";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 4;
	recordDescriptor.push_back(attribute);

	attribute.name = "column-position";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 5;
	recordDescriptor.push_back(attribute);

	attribute.name = "null-flag";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 6;
	recordDescriptor.push_back(attribute);

	return 0;
}

///@brief	assemble a "Tables" record
RC RelationManager::getFormedRecordTable(int tableId, string tableName, string fileName, int systemTable, char* data) {
	int offset = 0;
	unsigned nullIndicator = 0;

	memcpy(data + offset, &nullIndicator, 1);
	offset += 1;

	memcpy(data + offset, &tableId, 4);
	offset += 4;

	int tblNameLen = tableName.length();
	memcpy(data + offset, &tblNameLen, 4);
	offset += 4;


	memcpy(data + offset, tableName.c_str(), tblNameLen);
	offset += tblNameLen;

	int filNameLen = fileName.length();
	memcpy(data + offset, &filNameLen, 4);
	offset += 4;

	memcpy(data + offset, fileName.c_str(), filNameLen);
	offset += filNameLen;

	memcpy(data + offset, &systemTable, 4);
	offset += 4;

	return 0;
}

///@brief	assemble a "Columns" record
RC RelationManager::getFormedRecordColumn(int tableId, Attribute columnAttribute, int nullFlag, char* data){
	char* tempData = new char[PAGE_SIZE];
	int recordLength = 0;
	int stringLength = 0;
	char nullindictor = 0;

	memcpy(tempData, &nullindictor, sizeof(int));
	recordLength += sizeof(char);

	memcpy(tempData + recordLength, &tableId, sizeof(int));
	recordLength += sizeof(int);

	stringLength = columnAttribute.name.length();
	memcpy(tempData + recordLength, &stringLength, sizeof(int));
	recordLength += sizeof(int);

	memcpy(tempData + recordLength, columnAttribute.name.c_str(), stringLength);
	recordLength += stringLength;
	memcpy(tempData + recordLength, &columnAttribute.type, sizeof(int));
	recordLength += sizeof(int);
	memcpy(tempData + recordLength, &columnAttribute.length, sizeof(int));
	recordLength += sizeof(int);
	memcpy(tempData + recordLength, &columnAttribute.position, sizeof(int));
	recordLength += sizeof(int);
	memcpy(tempData + recordLength, &nullFlag, sizeof(int));
	recordLength += sizeof(int);

	memcpy(data, tempData, recordLength);
	delete []tempData;
	return 0;
}

RC RelationManager::insertColumn(int tableId, vector<Attribute> columnAttribute){
	char* tempData = new char[PAGE_SIZE];
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle columnFileHandle;
	vector<Attribute> recordDescriptor;
	RID rid;
	int rc = 0;
	if(rbfm->openFile("Columns", columnFileHandle)!= 0){
		return -1;
	}
	this->attributeColumn(recordDescriptor);
	for(unsigned i = 0; i < columnAttribute.size(); ++i){

		this->getFormedRecordColumn(tableId, columnAttribute[i], 0, tempData);

		rc = rbfm->insertRecord(columnFileHandle, recordDescriptor, tempData, rid);
	}
	rbfm->closeFile(columnFileHandle);
	delete []tempData;
	return 0;
}

RC RelationManager::createCatalog()
{
	vector<Attribute> tableDescriptor;
	vector<Attribute> columnDescriptor;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle tableFileHandle;
	FileHandle columnFileHandle;
	RID rid;
	char* tempData = new char[PAGE_SIZE];
	//create tables table
	if(rbfm->createFile("Tables") != 0){
		return -1;
	}
	rbfm->openFile("Tables",tableFileHandle);
	this->attributeTable(tableDescriptor);
	this->getFormedRecordTable(1, "Tables", "Tables", 1, tempData);

	rbfm->insertRecord(tableFileHandle, tableDescriptor, tempData, rid);

	memset(tempData, 0, sizeof(char)*PAGE_SIZE);
	this->getFormedRecordTable(2, "Columns", "Columns", 1, tempData);

	rbfm->insertRecord(tableFileHandle, tableDescriptor, tempData, rid);

	memset(tempData, 0, sizeof(char)*PAGE_SIZE);
	startTableId = 2;
	//create columns table
	if(rbfm->createFile("Columns") != 0){
		return -1;
	}
	this->attributeColumn(columnDescriptor);

	this->insertColumn(1, tableDescriptor);

	this->insertColumn(2, columnDescriptor);

	delete []tempData;
	return 0;
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if(rbfm->destroyFile("Tables") != 0){
		return -1;
	}
	if(rbfm->destroyFile("Columns") != 0){
		return -1;
	}
	return 0;
}



RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs){
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	vector<Attribute> tableAttribute;
	vector<Attribute> columnAttribute;
	char* tempData = new char[PAGE_SIZE];
	int tableId = -1;
	RID rid;

	columnAttribute = attrs;
	for(unsigned i = 0; i < columnAttribute.size(); ++i){
		columnAttribute[i].position = i+1;
	}

	if(rbfm->createFile(tableName) != 0){
		delete []tempData;
		return -1;
	}

	if(rbfm->openFile("Tables", fileHandle) != 0){
		delete []tempData;
		return -1;
	}

	//update Tables table

	tableId = ++this->startTableId;

	this->attributeTable(tableAttribute);

	this->getFormedRecordTable(tableId, tableName, tableName, 0, tempData);

	rbfm->insertRecord(fileHandle, tableAttribute, tempData, rid);
	rbfm->closeFile(fileHandle);
	//update column table
	this->insertColumn(tableId, columnAttribute);

	delete []tempData;
	return 0;
}


int RelationManager::GetTableId(const string tableName) {
	RM_ScanIterator tableIterator;
	vector<string> tableAttribute;
	int tableId = -1;
	RID rid;
	char* tempData = new char[PAGE_SIZE];

	tableAttribute.push_back("table-id");

	int rc = 0;
	rc = this->scan("Tables", "table-name", EQ_OP, tableName.c_str(), tableAttribute, tableIterator);

	if (rc != 0) {
		delete []tempData;
		tableIterator.close();
		return -1;
	}

	while (tableIterator.getNextTuple(rid, tempData) != RM_EOF) {
		memcpy(&tableId, tempData + sizeof(char), sizeof(int));
	}

	tableIterator.close();
	delete []tempData;

	return tableId;
}


///@brief	delete the table named of tableName
///@param	tableName	the name of table
///@return	if the table can be deleted successfully, then return 0. Else return -1.
RC RelationManager::deleteTable(const string &tableName)
{
	int tableId = -1;
	RID rid;
	RM_ScanIterator tableIterator;
	RM_ScanIterator columnIterator;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle tableFileHandle;
	FileHandle columnFileHandle;
	char* tempData = new char[PAGE_SIZE];

	vector<string> attributeName;
	attributeName.push_back("table-id");
	vector<Attribute> tableDescriptor;
	this->attributeTable(tableDescriptor);
	vector<Attribute> columnDescriptor;
	this->attributeColumn(columnDescriptor);


	if(tableName.compare("Tables") == 0 || tableName.compare("Columns") == 0){
		delete []tempData;
		return -1;
	}

	if(rbfm->destroyFile(tableName) != 0){
		delete []tempData;
		return -1;
	}

	//delete records in Tables table
	tableId = this->GetTableId(tableName);

	RC rc;
	rc = rbfm->openFile("Tables", tableFileHandle);
	if (rc != 0)	return rc;

	if(this->scan("Tables", "table-id", EQ_OP, &tableId, attributeName, tableIterator) != 0) {
		delete []tempData;
		return -1;
	}

	while(tableIterator.getNextTuple(rid, tempData) != RM_EOF){

		rc = rbfm->deleteRecord(tableFileHandle, tableDescriptor, rid);
		if (rc != 0)	return rc;
	}

	rc = rbfm->closeFile(tableFileHandle);
	if (rc != 0)	return rc;

	//delete records in Columns table
	rc = rbfm->openFile("Columns", columnFileHandle);
	if (rc != 0)	return rc;
	if(this->scan("Columns", "table-id", EQ_OP, &tableId, attributeName, columnIterator)!=0){
		delete []tempData;
		return -1;
	}

	while(columnIterator.getNextTuple(rid, tempData) != RM_EOF) {
		rc = rbfm->deleteRecord(columnFileHandle, columnDescriptor, rid);
		if (rc != 0)	return rc;
	}

	rbfm->closeFile(columnFileHandle);

	delete []tempData;

    return 0;
}

///@brief	get table's attribute
///@param	tableName		table's name
///@param	attrs			the table's attributes
///@return	if get atttributes failed, return -1, else return 0
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
	RM_ScanIterator attribute_iterator;
	RID rid;
	int tableId = -1;
	char* tempData = new char[PAGE_SIZE];
	vector<string> attributeName;
	attributeName.push_back("column-name");
	attributeName.push_back("column-type");
	attributeName.push_back("column-length");
	attributeName.push_back("column-position");
	attributeName.push_back("null-flag");

	int nullFlag = 0;

	tableId = this->GetTableId(tableName);

	if(tableId == -1) {
		delete []tempData;
		return -1;
	}


	if(this->scan("Columns", "table-id", EQ_OP, &tableId, attributeName, attribute_iterator) != 0) {
		delete []tempData;
		return -1;
	}
	while(attribute_iterator.getNextTuple(rid, tempData) != RM_EOF) {
		int recordLength = 1;
		Attribute tempattribute;

		int charLen = 0;
		memcpy(&charLen, tempData + 1, sizeof(int));
		char* colName = new char[charLen + 1];
		memcpy(colName, tempData + 1 + sizeof(int), charLen);
		colName[charLen] = '\0';
		string tempstring(colName);
		delete []colName;

		tempattribute.name = tempstring;
		recordLength += sizeof(int);
		recordLength += tempstring.length();

		memcpy(&tempattribute.type, tempData + recordLength, sizeof(int));
		recordLength += sizeof(int);

		memcpy(&tempattribute.length, tempData + recordLength, sizeof(int));
		recordLength += sizeof(int);

		memcpy(&tempattribute.position, tempData + recordLength, sizeof(int));
		recordLength += sizeof(int);

		memcpy(&nullFlag, tempData + recordLength, sizeof(int));
		recordLength += sizeof(int);

		if(nullFlag == 1){
			tempattribute.length = 0;
		}
		attrs.push_back(tempattribute);
	}

	attribute_iterator.close();
	delete []tempData;
	return 0;
}

///@brief	insert the tuple
///@param	tableName	the name of table which is going to be inserted
///@param	data		the tuple
///@param	rid			corresponding record's id
///@return	if insert tuple successfully, then return 0, else return -1
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {
	int rc = 0;
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	if(tableName == "Tables" || tableName == "Columns"){ //assume that the user do not create a table named Tables or Columns
		return -1;
	}


	rc = this->getAttributes(tableName, recordDescriptor);
	if (rc != 0) return rc;


	rc = rbfm->openFile(tableName,fileHandle);
	if (rc != 0) return rc;


	rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
	if (rc != 0) return rc;


	rc = rbfm->closeFile(fileHandle);
	if (rc != 0) return rc;

    return 0;
}

///@brief	delete tuple
///@param	tableName 	the name of table in which the tuple stays
///@param	rid			the corresponding record's id
///@return	if the tuple is deleted successfully, then return 0. Else return -1
RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	int rc = 0;
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	vector<Attribute> recordDescriptor;

	if(tableName == "Tables" || tableName == "Columns") { //assume that the user do not create a table named Tables or Columns
		return -1;
	}
	rc = this->getAttributes(tableName, recordDescriptor);
	if (rc != 0) return rc;

	rc = rbfm->openFile(tableName,fileHandle);
	if (rc != 0) return rc;

	rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
	if (rc != 0) return rc;

	rc = rbfm->closeFile(fileHandle);
	if (rc != 0) return rc;

    return 0;
}

///@brief	update the tuple
///@param	tableName	the name of table in which the tuple stays
///@param	data		the new tuple
///@param	rid			the corresponding record's id
///@return	if the tuple can be successfully updated, then return 0. Else return -1.
RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid) {
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> recordDescriptor;

	if(tableName == "Tables" || tableName == "Columns") { //assume that the user do not create a table named Tables or Columns
		return -1;
	}
	int rc = 0;
	rc = this->getAttributes(tableName, recordDescriptor);
	if (rc != 0) return rc;

	rc = rbfm->openFile(tableName,fileHandle);
	if (rc != 0) return rc;

	rc = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
	if (rc != 0) return rc;

	rc = rbfm->closeFile(fileHandle);
	if (rc != 0) return rc;

    return 0;
}

///@brief	read the tuple
///@param	tableName		the name of the table in which the tuple stays
///@param	rid				the record's id which is corresponding to the tuple in the table
///@param	data			the output tuple
///@return	if the tuple can be fetched successfully, then return 0. Else return -1.
RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data) {
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> recordDescriptor;

	int rc = 0;

	if(tableName == "Tables" || tableName == "Columns") { //assume that the user do not create a table named Tables or Columns
		return -1;
	}

	rc = this->getAttributes(tableName, recordDescriptor);
	if (rc != 0) return rc;

	rc = rbfm->openFile(tableName,fileHandle);
	if (rc != 0) return rc;

	rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
	if (rc != 0) return rc;

	rc = rbfm->closeFile(fileHandle);
	if (rc != 0) return rc;

    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	rbfm->printRecord(attrs, data);
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> recordDescriptor;

	this->getAttributes(tableName, recordDescriptor);
	rbfm->openFile(tableName, fileHandle);
	rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
	rbfm->closeFile(fileHandle);

    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> recordDescriptor;

	int rc = 0;

	if(tableName == "Tables"){
		this->attributeTable(recordDescriptor);
	}
	else if(tableName == "Columns"){
		this->attributeColumn(recordDescriptor);
	}
	else{
		this->getAttributes(tableName, recordDescriptor);
	}

	rc = rbfm->openFile(tableName, fileHandle);

	if (rc != 0)	return rc;

	rc = rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);

	if (rc != 0)	return rc;

    return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


