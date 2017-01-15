#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 
#include <math.h>

#include "pfm.h"
#include "rbfm.h"

using namespace std;

const int success = 0;


// Check whether a file exists
bool FileExists(string &fileName)
{
    struct stat stFileInfo;

    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}


// Calculate actual bytes for nulls-indicator for the given field counts
int getActualByteForNullsIndicator(int fieldCount) {

    return ceil((double) fieldCount / CHAR_BIT);
}


// After createFile() check
int createFileShouldSucceed(string &fileName) 
{
    if(FileExists(fileName))
    {
        cout << "File " << fileName << " has been created properly." << endl << endl;
        return 0;
    }
    else
    {
        cout << "[Fail] Failed to create the file: " << fileName << endl;
        cout << "[Fail] Test Case Failed!" << endl << endl;
        return -1;
    }
}

// After destroyFile() check
int destroyFileShouldSucceed(string &fileName) 
{
    if(FileExists(fileName))
    {
        cout << "[Fail] Failed to destory the file: " << fileName << endl;
        cout << "[Fail] Test Case Failed!" << endl << endl;
        return -1;
    }
    else
    {
        cout << "File " << fileName << " has been destroyed properly." << endl << endl;
        return 0;
    }
}
    
// Function to prepare the data in the correct form to be inserted/read
void prepareRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int nameLength, const string &name, const int age, const float height, const int salary, void *buffer, int *recordSize)
{
    int offset = 0;

    // Null-indicators
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);

    // Null-indicator for the fields
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Beginning of the actual data    
    // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
    // e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]

    // Is the name field not-NULL?
    nullBit = nullFieldsIndicator[0] & (1 << 7);

    if (!nullBit) {
        memcpy((char *)buffer + offset, &nameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, name.c_str(), nameLength);
        offset += nameLength;
    }

    // Is the age field not-NULL?
    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &age, sizeof(int));
        offset += sizeof(int);
    }

    // Is the height field not-NULL?
    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &height, sizeof(float));
        offset += sizeof(float);
    }

    // Is the height field not-NULL?
    nullBit = nullFieldsIndicator[0] & (1 << 4);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &salary, sizeof(int));
        offset += sizeof(int);
    }

    *recordSize = offset;
}

void prepareLargeRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int index, void *buffer, int *size)
{
    int offset = 0;

    // compute the count
    int count = (index + 2) % 50 + 1;

    // compute the letter
    char text = (index + 2) % 26 + 97;

    // Null-indicators
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);

    // Null-indicators
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Actual data
    for(int i = 0; i < 10; i++)
    {
        memcpy((char *)buffer + offset, &count, sizeof(int));
        offset += sizeof(int);

        for(int j = 0; j < count; j++)
        {
            memcpy((char *)buffer + offset, &text, 1);
            offset += 1;
        }

        // compute the integer
        memcpy((char *)buffer + offset, &index, sizeof(int));
        offset += sizeof(int);

        // compute the floating number
        float real = (float)(index + 1);
        memcpy((char *)buffer + offset, &real, sizeof(float));
        offset += sizeof(float);
    }
    *size = offset;
}

void createRecordDescriptor(vector<Attribute> &recordDescriptor) {

    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    recordDescriptor.push_back(attr);

    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

}

void createLargeRecordDescriptor(vector<Attribute> &recordDescriptor)
{
    char *suffix = (char *)malloc(10);
    for(int i = 0; i < 10; i++)
    {
        Attribute attr;
        sprintf(suffix, "%d", i);
        attr.name = "Char";
        attr.name += suffix;
        attr.type = TypeVarChar;
        attr.length = (AttrLength)50;
        recordDescriptor.push_back(attr);

        sprintf(suffix, "%d", i);
        attr.name = "Int";
        attr.name += suffix;
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        recordDescriptor.push_back(attr);

        sprintf(suffix, "%d", i);
        attr.name = "Real";
        attr.name += suffix;
        attr.type = TypeReal;
        attr.length = (AttrLength)4;
        recordDescriptor.push_back(attr);
    }
    free(suffix);
}

void prepareLargeRecord2(int fieldCount, unsigned char *nullFieldsIndicator, const int index, void *buffer, int *size) {
    int offset = 0;

    // compute the count
    int count = (index + 2) % 60 + 1;

    // compute the letter
    char text = (index + 2) % 26 + 65;

    // Null-indicators
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);

    // Null-indicators
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    for (int i = 0; i < 10; i++) {
        // compute the integer
        memcpy((char *) buffer + offset, &index, sizeof(int));
        offset += sizeof(int);

        // compute the floating number
        float real = (float) (index + 1);
        memcpy((char *) buffer + offset, &real, sizeof(float));
        offset += sizeof(float);

        // compute the varchar field
        memcpy((char *) buffer + offset, &count, sizeof(int));
        offset += sizeof(int);

        for (int j = 0; j < count; j++) {
            memcpy((char *) buffer + offset, &text, 1);
            offset += 1;
        }

    }
    *size = offset;
}

void createLargeRecordDescriptor2(vector<Attribute> &recordDescriptor) {
    char *suffix = (char *) malloc(10);
    for (int i = 0; i < 10; i++) {
        Attribute attr;
        sprintf(suffix, "%d", i);
        attr.name = "Int";
        attr.name += suffix;
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        recordDescriptor.push_back(attr);

        sprintf(suffix, "%d", i);
        attr.name = "Real";
        attr.name += suffix;
        attr.type = TypeReal;
        attr.length = (AttrLength) 4;
        recordDescriptor.push_back(attr);

        sprintf(suffix, "%d", i);
        attr.name = "Char";
        attr.name += suffix;
        attr.type = TypeVarChar;
        attr.length = (AttrLength) 60;
        recordDescriptor.push_back(attr);

    }
    free(suffix);
}
