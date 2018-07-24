#include <cmath>
#include "ix.h"

#include <assert.h>
#include <string.h>
#include <iostream>
/*
 * I will re-construct the code
 */


IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
	if(!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager()
{

    unsync = false;

}

IndexManager::~IndexManager()
{
}


///@brief	create file named by fileName
///@param	fileName, the name of file
///@return  if create successfully, return 0, else return -1
RC IndexManager::createFile(const string &fileName)
{
	// if file doesn't exist
	if ( access( fileName.c_str(), F_OK ) == -1  ){

		FILE* file = fopen(fileName.c_str(),"w+b");
		if( file != NULL )
			fclose(file);
		else
			return FAILURE;

		return SUCCESS;
	}

	return FAILURE;
}

///@brief	destroy the file named by fileName
///@param	fileName, the name of the file
///@return 	if the file was destroied successfully, then return 0, else return -1
RC IndexManager::destroyFile(const string &fileName)
{
	if( remove( fileName.c_str() ) != 0 )
		return FAILURE;
	else
		return SUCCESS;
}

///@brief	open the file
///@param	fileName, the name of the file.	ixfileHandle, the filehandle of the file
///@return	if open the file successfully, return 0, else return -1
RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
	// file does exist, open it
	if ( access( fileName.c_str(), F_OK ) == 0  ){

		if( ixFileHandle.initFilePointer( fileName ) == FAILURE )
			return FAILURE;

		ixFileHandle.ixReadPageCounter = 0;
		ixFileHandle.ixWritePageCounter = 0;
		ixFileHandle.ixAppendPageCounter = 0;

		ixFileHandle.isOpen = true;

		return SUCCESS;
	}

	return FAILURE;
}


///@brief	close the file
///@param	ixfileHandle
///@return	if close the file successfully, then return 0. else return -1
RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	ixfileHandle.isOpen = false; //update the open mark
	return ixfileHandle.closeFilePointer();
}

///@brief	traverse the tree, find the position to insert entry, returning page is stored in the returnpageNum
///@param	ixfileHandle
///@param 	attribute	the attribute of the key
///@param	key			key value in the tree
///@param	page		current page
///@param	pagenum		current page number
///@param	returnPagenum	return page number
///@return	the operation towards the tree
TreeOp IndexManager::TraverseTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *page, PageNum pageNum, PageNum &returnpageNum)
{

	void *nextpage = malloc(PAGE_SIZE);
	NodeDesc nodeDesc;
	NodeDesc nextnodeDesc;
	memcpy(&nodeDesc,(char *)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));

	PageSize offset=0;
	KeyDesc currentkeyDesc;
	char *currentkeyValue = (char*)malloc(maxvarchar);
	PageNum currentpageNum=0;

	TreeOp treeop = OP_None;
	while(true){
		memcpy(&currentkeyDesc,(char *) page+offset,sizeof(KeyDesc));
		offset+=sizeof(KeyDesc);

		memcpy(currentkeyValue,(char *) page+offset,currentkeyDesc.keySize);
		offset+=currentkeyDesc.keySize;


		if(keyCompare(attribute,key,currentkeyValue)<0){
			//get the page pointer
			currentpageNum=currentkeyDesc.leftNode;
			offset -= sizeof(KeyDesc);//adjust the offset for inserting a  key entry,
			offset -= currentkeyDesc.keySize;



			break;
		}
		if(offset == nodeDesc.size){
			//last entry
			currentpageNum=currentkeyDesc.rightNode;



			break;
		}
	}
	currentkeyDesc.keyValue= currentkeyValue;

	ixfileHandle.readPage(currentpageNum,nextpage);
	memcpy(&nextnodeDesc,(char *)nextpage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	if(nextnodeDesc.type == Leaf){

		returnpageNum=currentpageNum;
		treeop=OP_None;
		free(nextpage);
		free(currentkeyValue);
		return treeop;
	}else if(nextnodeDesc.type == NonLeaf){

		TraverseTree(ixfileHandle, attribute, key, nextpage, currentpageNum, returnpageNum);
		treeop=OP_None;
		free(nextpage);
		free(currentkeyValue);
		return treeop;

	}else{
		assert(false &&"nextnodeDesc.type should be Leaf or Nonleaf");
	}

}

///@brief	traverse the tree and insert the entry
///@param	ixfileHandle
///@param	attribute, the attribute of key
///@param	key, the value of key
///@param	rid, the rid of the record in the heap
///@param	page, current page
///@param	pagenum, the page number of traverse tree
///@param	keyDesc, the descriptor of key, store the pushed up key
///@return	the operation toward the tree
TreeOp IndexManager::TraverseTreeInsert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc)
{
	void *bufferpage = malloc(PAGE_SIZE);
	void *nextpage = malloc(PAGE_SIZE);
	void *buffer = malloc(PAGE_SIZE);
	void *keyValue = keyDesc.keyValue;
	void *extrapage = malloc(PAGE_SIZE);

	int tempLengthofkeyValue = 0;
	assert( keyValue == keyDesc.keyValue && " begining keyValue should equal to keyDesc.keyValue \n");
	NodeDesc nodeDesc;
	NodeDesc nextnodeDesc;
	memcpy(&nodeDesc,(char *)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	NodeDesc extranodeDesc;
	PageSize offset=0;
	KeyDesc siblingkeyDesc;
	KeyDesc currentkeyDesc;
	char *currentkeyValue = (char *)malloc(maxvarchar);
	KeyDesc nextkeyDesc;
	nextkeyDesc.keyValue = malloc(maxvarchar);
	PageNum currentpageNum=0;

	RC rc = -1;
	NodeDesc prevDesc;
	NodeDesc myDesc;
	NodeDesc mytempDesc;

	if( pageNum < 0 ) cout << "here is an error." << endl;

	TreeOp treeop = OP_None;
	TreeOp nexttreeop = OP_None;


	TreeOp mytemptreeop = OP_None;
	TreeOp fordebugtreeop = OP_None;
	void * mytempbuffer = malloc(PAGE_SIZE);

	void * mytempkeyValue = malloc(PAGE_SIZE);


	vector <Attribute> myattribute;
	myattribute.push_back(attribute);


	rc = this->transRecordFromDefaultToInternal( myattribute, (char *)mytempbuffer, tempLengthofkeyValue, (char *)mytempkeyValue );

	//scan to find the desired pointer
	while(true){
		memcpy(&currentkeyDesc,(char *) page+offset,sizeof(KeyDesc));
		offset+=sizeof(KeyDesc);
		memcpy(currentkeyValue,(char *) page+offset,currentkeyDesc.keySize);
		offset+=currentkeyDesc.keySize;


		if(keyCompare(attribute,key,currentkeyValue)<0){

			//get the page pointer


			currentpageNum=currentkeyDesc.leftNode;
			offset -= sizeof(KeyDesc);//adjust the offset for inserting a  key entry,
			offset -= currentkeyDesc.keySize;
			break;
		}
		if(offset == nodeDesc.size){
			//last entry



			currentpageNum=currentkeyDesc.rightNode;
			break;
		}

		if( mytemptreeop == OP_Split ) {
			rc = this->transRecordFromDefaultToInternal( myattribute, (char *)mytempbuffer, tempLengthofkeyValue, (char *)mytempkeyValue );
		}
	}
	currentkeyDesc.keyValue = currentkeyValue;
	assert( currentpageNum != 0 && "Should find a pageNum\n");


	int splitoffset=0;
	NodeDesc tempnodeDesc;
	PageSize origsize=nodeDesc.size;
	KeyDesc tempkeyDesc;
	KeyDesc oldtempkeyDesc;
	int tempnext=nodeDesc.next;
	PageSize insertedKeysize = sizeof(KeyDesc) + getKeySize(attribute,key);


	if((nodeDesc.size + insertedKeysize) >= UpperThreshold){
		//split the page


		//find the position to split
		while(true){

			//use keyDesc
			memcpy(&tempkeyDesc,(char *) page+splitoffset,sizeof(KeyDesc));
			splitoffset += sizeof(KeyDesc);
			splitoffset += tempkeyDesc.keySize;


			if((splitoffset + insertedKeysize)>= LowerThreshold){

				break;
			}
			oldtempkeyDesc = tempkeyDesc;
			if( mytemptreeop == OP_Split ) {
				rc = this->transRecordFromDefaultToInternal( myattribute, (char *)mytempbuffer, tempLengthofkeyValue, (char *)mytempkeyValue );
			}
		}

		//create nodeDesc for original page

		nodeDesc.size = splitoffset;
		nodeDesc.next = ixfileHandle.findFreePage();


		//push up a key value
		memcpy(&tempkeyDesc,(char *) page+splitoffset,sizeof(KeyDesc));
		splitoffset += sizeof(KeyDesc);
		//updating keySize to returned key
		keyDesc.keySize = tempkeyDesc.keySize;

		memcpy(keyDesc.keyValue,(char *) page+splitoffset,keyDesc.keySize);
		splitoffset += keyDesc.keySize;
		keyDesc.leftNode = pageNum;
		keyDesc.rightNode = nodeDesc.next;

		//create nodeDesc for new page
		tempnodeDesc.size = origsize-splitoffset;
		tempnodeDesc.prev = pageNum;
		tempnodeDesc.next = tempnext;
		tempnodeDesc.type = NonLeaf;

		//move data to new page
		memcpy(bufferpage,(char *)page+splitoffset,origsize-splitoffset);
		//update nodeDesc for  two pages
		memcpy((char *)bufferpage+PAGE_SIZE-sizeof(NodeDesc),&tempnodeDesc,sizeof(NodeDesc));
		memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
		//write these pages to disk
		ixfileHandle.writePage(pageNum,page);
		ixfileHandle.writePage(nodeDesc.next,bufferpage);
		treeop = OP_Split;

		if( tempnodeDesc.next != InvalidPage ){
			//read new right sibling page
			ixfileHandle.readPage(tempnodeDesc.next,extrapage);
			//update nodeDesc in new right sibling
			memcpy(&extranodeDesc,(char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
			extranodeDesc.prev = nodeDesc.next;
			memcpy((char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),&extranodeDesc,sizeof(NodeDesc));
			//write page to disk
			ixfileHandle.writePage(tempnodeDesc.next,extrapage);

			if( mytemptreeop == OP_Split ) {
				rc = this->transRecordFromDefaultToInternal( myattribute, (char *)mytempbuffer, tempLengthofkeyValue, (char *)mytempkeyValue );
			}
		}


	}


	//recursively call TraverseTreeInsert

	//read the page pointed by currentpageNum to nextpage
	rc = ixfileHandle.readPage(currentpageNum,nextpage);
	assert(rc == 0 && "rc != 0 \n");
	memcpy(&nextnodeDesc,(char *)nextpage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	//if it is leaf page call insertToLeat, NonLeaf page recursively call TraverseTreeInsert

	if(nextnodeDesc.type == Leaf){

		nexttreeop = insertToLeaf(ixfileHandle, attribute, key, rid, nextpage, currentpageNum, nextkeyDesc);

		assert((nexttreeop == OP_Split || nexttreeop == OP_None) && "nexttreeop should be OP_split or OP_None");

	}else if(nextnodeDesc.type == NonLeaf){

		nexttreeop = TraverseTreeInsert(ixfileHandle, attribute, key, rid, nextpage, currentpageNum, nextkeyDesc);
		assert((nexttreeop == OP_Split || nexttreeop == OP_None) && "nexttreeop should be OP_split or OP_None");


	}else{
		assert("page type should be leaf or NonLeaf");
	}

	if( mytemptreeop == OP_Split ) {
					rc = this->transRecordFromDefaultToInternal( myattribute, (char *)mytempbuffer, tempLengthofkeyValue, (char *)mytempkeyValue );
				}
	assert( keyValue == keyDesc.keyValue && "after recursive call, keyValue should equal to keyDesc.keyValue \n");
	if(nexttreeop == OP_Split){



		if(treeop == OP_Split ){



			if(offset == nodeDesc.size){
				//When offset == nodeDesc.size ,the offset didn't include pushed up key
				//offset should be 0
				offset -= nodeDesc.size;


				//update sibling KeyDesc(the first key in new page)
				memcpy(&siblingkeyDesc,(char *)bufferpage+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
				memcpy((char *)bufferpage+offset,&siblingkeyDesc,sizeof(KeyDesc));

				//may cause problem,move data backward
				memcpy(buffer,(char *)bufferpage+offset,tempnodeDesc.size-offset);

				memcpy((char *)bufferpage+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,buffer,tempnodeDesc.size-offset);
				memcpy((char *)bufferpage+offset,&nextkeyDesc,sizeof(KeyDesc));
				memcpy((char *)bufferpage+offset+sizeof(KeyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);

				//update nodeDesc for new page, write page to disk
				tempnodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
				memcpy((char *)bufferpage+PAGE_SIZE-sizeof(NodeDesc),&tempnodeDesc,sizeof(NodeDesc));
				ixfileHandle.writePage(nodeDesc.next,bufferpage);



			}else if(offset > nodeDesc.size){
				//offset include the pushed up  key


				offset -= nodeDesc.size;
				offset -= sizeof(KeyDesc);
				offset -= keyDesc.keySize;

				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)bufferpage+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
				memcpy((char *)bufferpage+offset,&siblingkeyDesc,sizeof(KeyDesc));

				//may cause problem,move data backward
				memcpy(buffer,(char *)bufferpage+offset,tempnodeDesc.size-offset);


				memcpy((char *)bufferpage+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,(char *)buffer,tempnodeDesc.size-offset);
				//copy the inserted key to bufferpage
				memcpy((char *)bufferpage+offset,&nextkeyDesc,sizeof(KeyDesc));
				memcpy((char *)bufferpage+offset+sizeof(KeyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);
				//update nodeDesc for new page, write page to disk
				tempnodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
				memcpy((char *)bufferpage+PAGE_SIZE-sizeof(NodeDesc),&tempnodeDesc,sizeof(NodeDesc));
				ixfileHandle.writePage(nodeDesc.next,bufferpage);

			}else{

				assert(offset < nodeDesc.size && "offset < nodeDesc.size");
				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
				memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));

				//move data backward
				memcpy(buffer,(char *)page+offset,nodeDesc.size-offset);


				memcpy((char *)page+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,buffer,nodeDesc.size-offset);
				//copy the inserted key to page
				memcpy((char *)page+offset,&nextkeyDesc,sizeof(KeyDesc));
				memcpy((char *)page+offset+sizeof(KeyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);
				//update nodeDesc for new page, write page to disk
				nodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				ixfileHandle.writePage(pageNum,page);

			}

		}else if(treeop == OP_None){

			//update sibling KeyDesc

			memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
			siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
			memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));


			//move data backward
			memcpy(buffer,(char *)page+offset,nodeDesc.size-offset);
			memcpy((char *)page+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,buffer,nodeDesc.size-offset);
			//copy the inserted key to page

			memcpy((char *)page+offset,&nextkeyDesc,sizeof(KeyDesc));
			memcpy((char *)page+offset+sizeof(KeyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);
			//update nodeDesc for new page, write page to disk

			nodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
			memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
			ixfileHandle.writePage(pageNum,page);

		}else{
			assert("treeop should be OP_split or OP_None");
		}
	}

	free(extrapage);
	free(buffer);
	free(nextpage);
	free(bufferpage);
	free(currentkeyDesc.keyValue);
	free(nextkeyDesc.keyValue);
	if(nexttreeop == OP_Error){
		treeop = nexttreeop;
	}
	assert( keyValue == keyDesc.keyValue && "keyValue should equal to keyDesc.keyValue ");

	free ( mytempbuffer );
	free ( mytempkeyValue );

	return treeop;
}


///@brief	insertEntry
///@param	ixfileHandle, read, write pages
///@param	attribute, descriptor the key
///@param	key, the key value of the B tree
///@param	rid, the rid of record with key value
RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc;
	// find root first


	void * mytempbuffer = malloc(PAGE_SIZE);
	int tempLengthofkeyValue = 0;
	void * mytempkeyValue = malloc(PAGE_SIZE);
	vector <Attribute> myattribute;
	myattribute.push_back(attribute);


	rc = this->transRecordFromDefaultToInternal( myattribute, (char *)mytempbuffer, tempLengthofkeyValue, (char *)mytempkeyValue );



	PageNum root = ixfileHandle.findRootPage();
	void *page = malloc(PAGE_SIZE);

	rc = ixfileHandle.readPage(root,page);

	KeyDesc keyDesc;
	keyDesc.keyValue=malloc(maxvarchar);

	// check if root needs to be split
	NodeDesc nodeDesc;
	memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );

	NodeType type = nodeDesc.type;
	PageSize size = nodeDesc.size;

	// traverse tree
	if(type==Leaf){
		//root page is leaf page
		TreeOp treeop=insertToLeaf(ixfileHandle, attribute, key, rid, page, root, keyDesc);
		assert( ((treeop == OP_Split) || (treeop == OP_None)) && "treeop should be OP_Split or OP_None"  );
		if(treeop == OP_Split){
			PageNum newroot;
			newroot=ixfileHandle.findFreePage();

			ixfileHandle.updateRootPage(newroot);
			NodeDesc newnodeDesc;
			int keysize = getKeySize(attribute,keyDesc.keyValue);

			newnodeDesc.next=InvalidPage;
			newnodeDesc.type=NonLeaf;
			newnodeDesc.size=0;

			//reuse void * page, copy keyDesc to the page
			memcpy((char *)page+newnodeDesc.size,&keyDesc,sizeof(KeyDesc));
			newnodeDesc.size+=sizeof(KeyDesc);
			memcpy((char *)page+newnodeDesc.size,keyDesc.keyValue,keysize);
			newnodeDesc.size+=keysize;

			memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&newnodeDesc,sizeof(NodeDesc));

			rc = ixfileHandle.writePage(newroot,page);
			assert(rc == SUCCESS && "Fail to write root page as leaf page");

			rc = this->transRecordFromDefaultToInternal( myattribute, (char *)mytempbuffer, tempLengthofkeyValue, (char *)mytempkeyValue );
		}
		free(keyDesc.keyValue);
		free(page);

		free ( mytempbuffer );
		free ( mytempkeyValue );

		return 0;
	}else if(type == NonLeaf){
		//root page is NonLeaf

		TreeOp treeop=TraverseTreeInsert(ixfileHandle, attribute, key, rid, page, root, keyDesc);
		assert( ((treeop == OP_Split) || (treeop == OP_None)) && "treeop should be OP_Split or OP_None"  );
		if(treeop == OP_Split){
			PageNum newroot;
			newroot=ixfileHandle.findFreePage();

			ixfileHandle.updateRootPage(newroot);
			NodeDesc newnodeDesc;
			int keysize = getKeySize(attribute,keyDesc.keyValue);

			newnodeDesc.next=InvalidPage;
			newnodeDesc.type=NonLeaf;
			newnodeDesc.size=0;

			//reuse void * page, copy keyDesc to the page
			memcpy((char *)page+newnodeDesc.size,&keyDesc,sizeof(KeyDesc));
			newnodeDesc.size+=sizeof(KeyDesc);
			memcpy((char *)page+newnodeDesc.size,keyDesc.keyValue,keysize);
			newnodeDesc.size+=keysize;

			memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&newnodeDesc,sizeof(NodeDesc));

			rc = ixfileHandle.writePage(newroot,page);
			assert(rc == SUCCESS && "Fail to write root page as leaf page");

		}
		rc = this->transRecordFromDefaultToInternal( myattribute, (char *)mytempbuffer, tempLengthofkeyValue, (char *)mytempkeyValue );

		free(page);
		free(keyDesc.keyValue);

		free ( mytempbuffer );
		free ( mytempkeyValue );

		return 0;
	}else{
		assert("root page should be Leaf or NonLeaf");
	}
	//

	free ( mytempbuffer );
	free ( mytempkeyValue );

	return -1;
}

///@brief	insert the entry into the leaf node
///@param	ixfileHandle
///@param	attribute, the attribute of the key value
///@param	rid, the record's rid in the heap
///@param	page,
///@param	pageNum,
///@param	keyDesc, the descriptor of key

TreeOp IndexManager::insertToLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc)
{

//	checkPageInt(ixfileHandle, page, pageNum,false);
	TreeOp operation = OP_None;
	// retrieve node info
	NodeDesc nodeDesc;
	memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
	int offset = 0 ;
	bool insert = false;
	// potential split page buffer
	void *splitPage = malloc(PAGE_SIZE);

	// if current node size + new dataEntry size > threshold , do split first
	if( nodeDesc.size + sizeof(DataEntryDesc) + getKeySize(attribute,key) + sizeof(RID) > UpperThreshold ){
		offset = 0; // offset of entry which is going to split to right node
		operation = OP_Split;
		// add offset until it passes the half of size
		while( offset < nodeDesc.size / 2){
			DataEntryDesc ded;
			memcpy( &ded, (char*)page+offset, sizeof(DataEntryDesc) );
			offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
		}
		// allocate new page to insert splitted nodes
		PageNum freePageID = ixfileHandle.findFreePage();
		// form a new page, fill up the informatio

		memcpy(splitPage, (char*)page+offset, nodeDesc.size - offset );
		NodeDesc splitNodeDesc;
		splitNodeDesc.type = Leaf;
		splitNodeDesc.size = nodeDesc.size - offset;
		splitNodeDesc.next = nodeDesc.next;
		splitNodeDesc.prev = pageNum;
		memcpy( (char*)splitPage+PAGE_SIZE-sizeof(NodeDesc), &splitNodeDesc, sizeof(NodeDesc) );
		nodeDesc.size = offset;
		nodeDesc.next = freePageID;
		memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
		ixfileHandle.writePage(pageNum,page);

		// get First entry key value
		DataEntryDesc nDed;
		memcpy( &nDed, (char*)splitPage, sizeof(DataEntryDesc));
		keyDesc.rightNode = freePageID;
		keyDesc.leftNode = pageNum;
		keyDesc.keySize = nDed.keySize;
		memcpy( keyDesc.keyValue, (char*)splitPage+sizeof(DataEntryDesc), nDed.keySize);
		ixfileHandle.writePage(freePageID,splitPage);


		// if the insertion key is great than the split page's first key
		// insert it into split page
		if( keyCompare( attribute, key , keyDesc.keyValue ) >= 0 ){
		    // copy the spot page
		    memcpy(page, splitPage, PAGE_SIZE);
		    memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
		    pageNum = freePageID;
		}


		// update right leaf's left node to split page number
		// if it's not empty
		if( splitNodeDesc.next != InvalidPage ){
		    NodeDesc ntNodeDesc;
		    ixfileHandle.readPage( splitNodeDesc.next, splitPage);
		    memcpy( &ntNodeDesc, (char*)splitPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc));
		    ntNodeDesc.prev = freePageID;
		    memcpy( (char*)splitPage+PAGE_SIZE-sizeof(NodeDesc), &ntNodeDesc, sizeof(NodeDesc));
		    ixfileHandle.writePage( splitNodeDesc.next, splitPage);
		}

	}


	offset = 0;
	// insertion
	while( offset < nodeDesc.size ){

		DataEntryDesc ded;
		memcpy( &ded, (char*)page+offset, sizeof(DataEntryDesc) );

		ded.keyValue = malloc(ded.keySize);
		memcpy( ded.keyValue, (char*)page+offset+DataEntryKeyOffset, ded.keySize);

		// compare the key to find insertion point
		int result = keyCompare(attribute, ded.keyValue, key);


		// key value smaller than rest of the data
		// insert new key,rid pair right here
		if( result > 0 ){
			// use splitpage buffer as temp buffer, copy the rest of the key and rid lists
			int restDataSize = nodeDesc.size - offset;
			memcpy( splitPage , (char*)page+offset , restDataSize );
			// insert a new <key,rid> pair
			DataEntryDesc nDed;
			nDed.numOfRID = 1;
			nDed.overflow = false;
			nDed.keySize = getKeySize(attribute,key);
			memcpy( (char*)page+offset, &nDed, sizeof(DataEntryDesc));
			memcpy( (char*)page+offset+DataEntryKeyOffset, key , nDed.keySize ) ;
			memcpy( (char*)page+offset+DataEntryKeyOffset+nDed.keySize, &rid , sizeof(RID) );
			// update offset and copy rest of the data back
			offset += sizeof(DataEntryDesc) + nDed.keySize + nDed.numOfRID * sizeof(RID);
			memcpy( (char*)page+offset, splitPage, restDataSize );
			// update the node descriptor's size info
			nodeDesc.size += sizeof(DataEntryDesc) + nDed.keySize + nDed.numOfRID * sizeof(RID);
			memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );

			free(ded.keyValue);
			insert = true;
			break;
		}
		// if the keypair is overflowed, insert it to overflow page
		if( result == 0 && ded.overflow != InvalidPage ){
			RC rc;
			DataEntryDesc oDed;
			PageNum overflowPageNum = ded.overflow;
			rc = ixfileHandle.readPage( overflowPageNum , page );
			assert( rc == SUCCESS && "read overflow page failed");
			memcpy( &oDed, page, sizeof(DataEntryDesc));

			while( oDed.overflow != InvalidPage ){
			    rc = ixfileHandle.readPage( oDed.overflow , page );
			    assert( rc == SUCCESS && "read overflow page failed");
			    overflowPageNum = oDed.overflow;
			    memcpy( &oDed, page, sizeof(DataEntryDesc)); // copy new overflow page number
			}

			if( sizeof(DataEntryDesc)+oDed.keySize+oDed.numOfRID*sizeof(RID) > PAGE_SIZE *0.9 ){
			    // find over flow page and update the current ded's overflow page indicator
			    PageNum link = ixfileHandle.findFreePage();
			    oDed.overflow = link; // update overflow indicator
			    memcpy( page, &oDed, sizeof(DataEntryDesc));
			    rc = ixfileHandle.writePage( overflowPageNum, page );
			    assert( rc == SUCCESS && "write overflow page failed");
			    // new overflow page
			    overflowPageNum = link; oDed.numOfRID = 0; oDed.overflow = InvalidPage;
			}
			// write it to overflow page
			memcpy( (char*)page+sizeof(DataEntryDesc)+oDed.keySize+oDed.numOfRID*sizeof(RID), &rid, sizeof(RID) );
			oDed.numOfRID++;
			memcpy( page, &oDed, sizeof(DataEntryDesc));
			assert( sizeof(DataEntryDesc)+oDed.keySize+oDed.numOfRID*sizeof(RID) < PAGE_SIZE && "overflow page overflowed" );
			rc = ixfileHandle.writePage( overflowPageNum , page );
			assert( rc == SUCCESS && "write overflow page failed");
			free(ded.keyValue);
			free(splitPage);

			return operation;
		}
		// check if the RID list is too big so that we need to move it to a overflow page.
		// the condition depends on RID list's size bigger than LowerThreshold Bound

		if( result == 0 && ded.numOfRID*sizeof(RID) > LowerThreshold ){

			// find over flow page and update the current ded's overflow page indicator
			PageNum link = ixfileHandle.findFreePage();
			ded.overflow = link; // update overflow indicator

			memcpy( (char*)page+offset, &ded , sizeof(DataEntryDesc) ); // update ded to original page

			// put new key,rid pair into over flow page
			void *overflowPage = malloc(PAGE_SIZE); // create another overflow page
			ded.numOfRID = 1; ded.overflow = InvalidPage; // same ded but different size of RID list,

			// insert RID and ded to overflow page
			memcpy( overflowPage, &ded, sizeof(DataEntryDesc) );
			memcpy( (char*)overflowPage+sizeof(DataEntryDesc)+ded.keySize, &rid ,sizeof(RID));
			RC rc;
			rc = ixfileHandle.writePage( link , overflowPage );
			assert( rc == SUCCESS && "write overflow page failed");
			rc = ixfileHandle.writePage( pageNum , page );
			assert( rc == SUCCESS && "write overflow page failed");

			free(overflowPage);
			free(ded.keyValue);
			free(splitPage);
			return operation;
		}

		// same key value, append RID to the list
		if( result == 0 ){
			int pairSize = sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
			// use splitpage buffer as temp buffer, copy the rest of the key and rid lists
			memcpy( splitPage , (char*)page+offset+pairSize , nodeDesc.size - ( offset + pairSize ) );
			// add RID to the back
			memcpy( (char*)page+offset + sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID) , &rid , sizeof(RID));
			memcpy( (char*)page+offset + sizeof(DataEntryDesc) + ded.keySize + (ded.numOfRID+1)*sizeof(RID) , splitPage , nodeDesc.size - (offset + pairSize ) );
			// increase number of rid by 1 , write it back
			ded.numOfRID++;
			memcpy( (char*)page+offset, &ded , sizeof(DataEntryDesc) );
			// update page descriptor
			nodeDesc.size += sizeof(RID);
			memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc) , &nodeDesc , sizeof(NodeDesc) );

			free(ded.keyValue);
			insert = true;
			break;
		}

		free(ded.keyValue);
		offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
	} // end while of searching insertion point


	// if the key is the biggest in the page, append it
	if( !insert ){
		// insert a new <key,rid> pair
		DataEntryDesc nDed;
		nDed.numOfRID = 1;
		nDed.overflow = false;
		nDed.keySize = getKeySize(attribute,key);
		memcpy( (char*)page+offset, &nDed, sizeof(DataEntryDesc));
		memcpy( (char*)page+offset+DataEntryKeyOffset, key , nDed.keySize ) ;
		memcpy( (char*)page+offset+DataEntryKeyOffset+nDed.keySize, &rid , sizeof(RID) );

		// update the node descriptor's size info
		nodeDesc.size += sizeof(DataEntryDesc) + nDed.keySize + nDed.numOfRID * sizeof(RID);
		memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
	}
	// not necessarily be the pageNum pointed by non-leaf node
	// could be the split node num if the new key is inserted
	// into split page
	ixfileHandle.writePage(pageNum,page);
	free(splitPage);
	return operation;
}

///@brief	Find the last key, first get the size of the node, then scan the keys one by one.
///			if the offset = size of node, then it was the last key
///param	page, node page
///param	keyDesc, the descriptor of key vale
///return	the last key's descriptor will be returned
void IndexManager::FindLastKey(void *page,KeyDesc &keyDesc)
{
	NodeDesc nodeDesc;
	int offset = 0;

	// get the node descriptor
	memcpy(&nodeDesc, (char*)page + PAGE_SIZE - sizeof(NodeDesc), sizeof(NodeDesc));

	while(true){ // scan the keys one by one
		memcpy(&keyDesc, (char *) page + offset, sizeof(KeyDesc));
		offset += (sizeof(KeyDesc) + keyDesc.keySize);
		if(offset == nodeDesc.size){
			break;
		}
	}
}


///@brief	find the offset
///@param	page, current page
///@param	size,
///@param	offset
///@param 	isGreat
///@return 	return the offset
RC IndexManager::FindOffset(void *page, int size, int &offset, bool IsGreater)
{
	KeyDesc keyDesc;
	offset = 0;
	int oldoffset = -1;
	NodeDesc nodeDesc;

	// get the node descriptor
	memcpy(&nodeDesc, (char*)page + PAGE_SIZE - sizeof(NodeDesc), sizeof(NodeDesc));

	while(true){

		//scan key one by one
		memcpy(&keyDesc, (char *)page + offset, sizeof(keyDesc));

		offset += (sizeof(KeyDesc) + keyDesc.keySize);
		if(IsGreater){
			if(offset >= size){
				return 0;
			}else if(offset > nodeDesc.size){
				return -1;
			}
		}else{
			if(offset >= size){
				offset = oldoffset;
				return 0;
			}else if(offset > nodeDesc.size){
				return -1;
			}
		}
		oldoffset = offset;
	}
	assert("FindOffset has error!");
	return -1;
}


TreeOp IndexManager::TraverseTreeDelete(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc, int  rightMost )
{
	void *bufferpage = malloc(PAGE_SIZE);
	void *nextpage = malloc(PAGE_SIZE);
	void *leftsibling = malloc(PAGE_SIZE);
	void *rightsibling = malloc(PAGE_SIZE);
	void *extrapage = malloc(PAGE_SIZE);
	void *deletepage = malloc(PAGE_SIZE);
	NodeDesc nodeDesc;
	NodeDesc tempnodeDesc;
	NodeDesc nextnodeDesc;
	NodeDesc extranodeDesc;
	NodeDesc deletenodeDesc;
	memcpy(&nodeDesc,(char *)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	PageNum tempnext = nodeDesc.next;
	PageNum tempprev = nodeDesc.prev;
	int pushdownkeysize = keyDesc.keySize;
	int nextRightmost = 0;
	PageSize offset=0;


	PageSize oldoffset=0;
	KeyDesc siblingkeyDesc;
	KeyDesc currentkeyDesc;
	KeyDesc deletedkeyDesc;
	KeyDesc TobedeletekeyDesc;
	void * keyValue = keyDesc.keyValue;
	currentkeyDesc.keyValue = malloc(maxvarchar);
	void * currentkeyValue = currentkeyDesc.keyValue;
	KeyDesc nextkeyDesc;
	nextkeyDesc.keyValue = malloc(maxvarchar);
	void *nextkeyValue = nextkeyDesc.keyValue;
	PageNum currentpageNum=0;
	//initialize rightnode
	PageNum rightnode;
	KeyDesc checkkeyDesc;
	memcpy(&checkkeyDesc,page,sizeof(KeyDesc));
	rightnode = checkkeyDesc.leftNode;

	TreeOp treeop = OP_None;
	TreeOp nexttreeop = OP_None;
	//IsRightest to indicate whtether the currentpageNum is the rightNode
	bool IsRightest = false;
	bool DeleteNext = false;
	//store the leftNode and rightNode in currentkeyDesc
	PageNum temprightNode = 0;
	PageNum templeftNode =0;



	//scan to find the desired pointer
	while(true){
		memcpy(&currentkeyDesc,(char *) page+offset,sizeof(KeyDesc));
		offset+=sizeof(KeyDesc);
		memcpy(currentkeyValue,(char *) page+offset,currentkeyDesc.keySize);
		offset+=currentkeyDesc.keySize;
		if(keyCompare(attribute,key,currentkeyValue)<0){
					//get the page pointer

			currentpageNum=currentkeyDesc.leftNode;



			assert(rightnode == currentkeyDesc.leftNode && "compare < 0,currentpageNum=currentkeyDesc.leftNode");
			break;
		}

		if(offset == nodeDesc.size){
			//last entry
			currentpageNum=currentkeyDesc.rightNode;



			assert(rightnode == currentkeyDesc.leftNode && "In the end,currentpageNum=currentkeyDesc.leftNode");
			IsRightest = true;
			nextRightmost = 1;

			break;
		}

		rightnode = currentkeyDesc.rightNode;
	}
	oldoffset = offset - sizeof(KeyDesc) - currentkeyDesc.keySize;

	currentkeyDesc.keyValue = currentkeyValue;
	//store leftNode and rightNode in currentkeyDesc
	temprightNode = currentkeyDesc.rightNode;
	templeftNode = currentkeyDesc.leftNode;

	assert( currentpageNum != -1 && "Should find a pageNum");

	//assert(false);

	NodeDesc leftnodeDesc;
	NodeDesc rightnodeDesc;
	PageSize origsize=nodeDesc.size;
	PageSize leftsize=0;
	PageSize rightsize=0;
	KeyDesc lastkeyDesc;
	KeyDesc beginkeyDesc;
	int currentoffset = nodeDesc.size;
	bool Emptyflag = false;
	PageSize deletedkeySize = sizeof(KeyDesc) + getKeySize(attribute,key);


	if((nodeDesc.size - deletedkeySize)< LowerThreshold){
		//merege or redistribute the page


		if(nodeDesc.prev != InvalidPage || rightMost != 1){

			//not root page


			if(rightMost != 1){




				ixfileHandle.readPage(nodeDesc.next,rightsibling);
				memcpy(&rightnodeDesc,(char*)rightsibling+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));


				if( (rightnodeDesc.size + nodeDesc.size + deletedkeySize)> UpperThreshold){
					//redistribute data from rightsibling
					//checkKeyInt(ixfileHandle, attribute, page);


					int siblingoffset;
					//find the position in sibling page to move data
					FindOffset(rightsibling,LowerThreshold-nodeDesc.size,siblingoffset,true);

					//find the last key in original page and the first key in sibling page
					FindLastKey(page,lastkeyDesc);
					//assert(lastkeyDesc.rightNode != InvalidPage && "rightNode != InvalidPage in Redistribution");
					memcpy(&beginkeyDesc,rightsibling,sizeof(KeyDesc));
					//update the leftNode and rightNode for push down key
					keyDesc.leftNode = lastkeyDesc.rightNode;
					keyDesc.rightNode = beginkeyDesc.leftNode;

					//printKey(attribute,keyValue);
					//push down key
					memcpy((char *)page+currentoffset,&keyDesc,sizeof(KeyDesc));
					currentoffset += sizeof(KeyDesc);
					memcpy((char *)page+currentoffset,keyDesc.keyValue,keyDesc.keySize);
					currentoffset += keyDesc.keySize;
					//move the data from beginning to the splitoffset in  rightsibling to original page
					//redistribute data from right page
					memcpy((char *)page+currentoffset,(char *)rightsibling,siblingoffset);
					currentoffset += siblingoffset;

					//update nodeDesc
					nodeDesc.size = currentoffset;


					memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
					//checkKeyInt(ixfileHandle, attribute, page);

					//create returned keyDesc from the first key in sibling page
					memcpy(&keyDesc,(char*)rightsibling+siblingoffset,sizeof(KeyDesc));
					keyDesc.keyValue = keyValue;
					siblingoffset += sizeof(KeyDesc);
					memcpy(keyDesc.keyValue,(char*)rightsibling+siblingoffset,keyDesc.keySize);
					siblingoffset += keyDesc.keySize;
					//move entry in right sibling
					memcpy(bufferpage,(char*)rightsibling+siblingoffset,rightnodeDesc.size-siblingoffset);
					assert((rightnodeDesc.size-siblingoffset)>= 0 && "rightnodeDesc.size-siblingoffset >= 0");
					memcpy(rightsibling,(char*)bufferpage,rightnodeDesc.size-siblingoffset);
					//update rightnodeDesc
					rightnodeDesc.size = rightnodeDesc.size - siblingoffset;

					memcpy((char *)rightsibling+PAGE_SIZE-sizeof(NodeDesc),&rightnodeDesc,sizeof(NodeDesc));

					//update page and rigntsibling to disk
					ixfileHandle.writePage(pageNum,page);
					ixfileHandle.writePage(nodeDesc.next,rightsibling);
					treeop = OP_Dist;


				}else{
					//merge, move data from right sibling
					int tempnext = nodeDesc.next;
					//find the last key in original page and the first key in sibling page
					FindLastKey(page,lastkeyDesc);
					memcpy(&beginkeyDesc,rightsibling,sizeof(KeyDesc));
					//update the leftNode and rightNode for push down key
					keyDesc.leftNode = lastkeyDesc.rightNode;
					keyDesc.rightNode = beginkeyDesc.leftNode;
					//push down key
					memcpy((char *)page+nodeDesc.size,&keyDesc,sizeof(keyDesc));
					nodeDesc.size += sizeof(keyDesc);
					memcpy((char *)page+nodeDesc.size,keyDesc.keyValue,keyDesc.keySize);
					nodeDesc.size += keyDesc.keySize;
					//move data from rightsibling to original page
					memcpy((char *)page+nodeDesc.size,(char *)rightsibling,rightnodeDesc.size);
					nodeDesc.size += rightnodeDesc.size;
					nodeDesc.next =rightnodeDesc.next;
					//update nodeDesc
					memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));

					//write page to disk and delete the rightsibling in disk
					ixfileHandle.writePage(pageNum,page);
					assert( ixfileHandle.findRootPage() != tempnext && "shouldn't delete root page");
					ixfileHandle.deletePage(tempnext);
					treeop = OP_Merge;

					//update nodeDesc of new right sibling page
					if( nodeDesc.next != InvalidPage ){
						//read new right sibling page
						ixfileHandle.readPage(nodeDesc.next,extrapage);
						//update nodeDesc in new right sibling
						memcpy(&extranodeDesc,(char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
						extranodeDesc.prev = pageNum;
						memcpy((char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),&extranodeDesc,sizeof(NodeDesc));
						//write page to disk
						ixfileHandle.writePage(nodeDesc.next,extrapage);


					}


				}

			}else{
				//If current page is the rightest page, move data from left page
				int backwardoffset = 0;
				//get the leftnodeDesc from leftsibling
				ixfileHandle.readPage(nodeDesc.prev,leftsibling);
				memcpy(&leftnodeDesc,(char*)leftsibling+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));


				if( (leftnodeDesc.size + nodeDesc.size + deletedkeySize)> UpperThreshold){
					int siblingoffset;
					//find the begining point in left page to move data
					FindOffset(leftsibling,leftnodeDesc.size - (LowerThreshold-nodeDesc.size),siblingoffset,false);
					//assert((leftnodeDesc.size - (LowerThreshold-nodeDesc.size))>LowerThreshold && "(leftNode.size - (LowerThreshold-nodeDesc.size))>LowerThreshold");

					//find the last key in original page and the first key in sibling page
					FindLastKey(leftsibling,lastkeyDesc);
					assert(lastkeyDesc.rightNode != InvalidPage && "rightNode == InvalidPage in Redistribution");
					memcpy(&beginkeyDesc,page,sizeof(KeyDesc));
					keyDesc.leftNode = lastkeyDesc.rightNode;
					keyDesc.rightNode = beginkeyDesc.leftNode;

					//push down key
					backwardoffset = keyDesc.keySize+sizeof(KeyDesc);
					//move the data in page backward
					memcpy(bufferpage,page,currentoffset);
					memcpy((char *)page+backwardoffset,bufferpage,currentoffset);
					//copy push down key to page
					memcpy((char *)page,&keyDesc,sizeof(KeyDesc));
					currentoffset += sizeof(KeyDesc);
					oldoffset += backwardoffset;
					offset += backwardoffset;
					memcpy((char *)page+sizeof(KeyDesc),keyDesc.keyValue,keyDesc.keySize);
					currentoffset += keyDesc.keySize;


					assert(backwardoffset == (currentoffset - nodeDesc.size) &&"backwardoffset == (sizeof(KeyDesc) + keyDesc.keySize)");


					//move the data in page backward
					backwardoffset = leftnodeDesc.size - siblingoffset;
					memcpy(bufferpage,page,currentoffset);
					memcpy((char *)page+backwardoffset,bufferpage,currentoffset);
					//move data starting in siblingoffset from leftsibling to the page
					memcpy((char *)page,(char *)leftsibling+siblingoffset,backwardoffset);//redistribute data from right page
					currentoffset += backwardoffset;
					oldoffset += backwardoffset;
					offset += backwardoffset;

					//update nodeDesc
					nodeDesc.size = currentoffset;

					memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));

					//create returned keyDesc
					//modify leftnodeDesc.size for  FindLastKey to use
					leftnodeDesc.size = siblingoffset;
					memcpy((char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),&leftnodeDesc,sizeof(NodeDesc));
					FindLastKey(leftsibling,lastkeyDesc);
					//pointing to the start position for last key
					leftnodeDesc.size -= (sizeof(KeyDesc) + lastkeyDesc.keySize);
					//copy the keyDesc to returned key
					memcpy(&keyDesc,(char*)leftsibling+leftnodeDesc.size,sizeof(KeyDesc));
					keyDesc.keyValue = keyValue;
					memcpy(keyDesc.keyValue,(char*)leftsibling+leftnodeDesc.size+sizeof(KeyDesc),keyDesc.keySize);

					//update leftnodeDesc

					memcpy((char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),&leftnodeDesc,sizeof(NodeDesc));

					//update page and rigntsibling to disk
					ixfileHandle.writePage(pageNum,page);
					ixfileHandle.writePage(nodeDesc.prev,leftsibling);
					treeop = OP_Dist;


				}else{
					//merge, move data to left sibling

					//push down key
					FindLastKey(leftsibling,lastkeyDesc);
					memcpy(&beginkeyDesc,page,sizeof(KeyDesc));
					keyDesc.leftNode = lastkeyDesc.rightNode;
					keyDesc.rightNode = beginkeyDesc.leftNode;
					//copy push down key to lefsibling
					memcpy((char *)leftsibling+leftnodeDesc.size,&keyDesc,sizeof(keyDesc));
					leftnodeDesc.size += sizeof(keyDesc);
					memcpy((char *)leftsibling+leftnodeDesc.size,keyDesc.keyValue,keyDesc.keySize);
					leftnodeDesc.size += keyDesc.keySize;


					memcpy((char *)leftsibling+leftnodeDesc.size,(char *)page,nodeDesc.size);
					leftnodeDesc.size += nodeDesc.size;
					leftnodeDesc.next = nodeDesc.next;
					//update leftnodeDesc to leftsibling
					memcpy((char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),&leftnodeDesc,sizeof(NodeDesc));

					treeop = OP_Merge;
					ixfileHandle.deletePage(pageNum);
					//write page to disk
					ixfileHandle.writePage(nodeDesc.prev,leftsibling);


					//the distance to the ending of entries doesn't change
					oldoffset = leftnodeDesc.size - (nodeDesc.size - oldoffset);
					offset = leftnodeDesc.size - (nodeDesc.size - offset);

					//update nodeDesc of new right sibling page
					assert(tempnext == nodeDesc.next);
					assert(tempprev == nodeDesc.prev);
					if( tempnext != InvalidPage ){
						//read new right sibling page
						ixfileHandle.readPage(nodeDesc.next,extrapage);
						//update nodeDesc in new right sibling
						memcpy(&extranodeDesc,(char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
						extranodeDesc.prev = tempprev;
						memcpy((char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),&extranodeDesc,sizeof(NodeDesc));
						//write page to disk
						ixfileHandle.writePage(nodeDesc.next,extrapage);


					}

				}
			}
		}

	}


	//recursively call TraverseTreeInsert

	ixfileHandle.readPage(currentpageNum,nextpage);
	memcpy(&nextnodeDesc,(char *)nextpage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));

	DataEntryDesc trykeyDesc;
	void *trykey = malloc(PAGE_SIZE);


	if(nextnodeDesc.type == Leaf){
		memcpy(&trykeyDesc,nextpage,sizeof(DataEntryDesc));

		memcpy(trykey,(char *)nextpage+sizeof(DataEntryDesc),trykeyDesc.keySize);



		nexttreeop = deleteFromLeaf(ixfileHandle,attribute,key,rid,nextpage,currentpageNum, currentkeyDesc, nextRightmost);

		//assert((nexttreeop == OP_Merge || nexttreeop == OP_Dist || nexttreeop == OP_None) && "nexttreeop should be OP_Merge,OP_Dist or OP_None");

	}else if(nextnodeDesc.type == NonLeaf){


		nexttreeop = TraverseTreeDelete(ixfileHandle,attribute,key,rid,nextpage,currentpageNum, currentkeyDesc, nextRightmost);

		//assert((nexttreeop == OP_Merge || nexttreeop == OP_Dist || nexttreeop == OP_None) && "nexttreeop should be OP_Merge,OP_Dist or OP_None");



	}else{
		assert("page type should be leaf or NonLeaf\n");
	}




	if(nexttreeop == OP_Merge ){


		if(rightMost != 1){

				//if nodeDesc.next != InvalidPage, offset and oldoffset do not change
				//fetch the deleted key
				memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));


				//update sibling KeyDesc's leftnode to  keep link-list
				memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = deletedkeyDesc.leftNode;
				memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));
				//move data to delete the key
				memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

				//update page descriptor

				nodeDesc.size -= ((int)offset - (int)oldoffset);
				//assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");



				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				//write page to disk
				assert(nodeDesc.size != 0 && "nodeDesc.size != 0");
				ixfileHandle.writePage(pageNum,page);


		}else{



			if(treeop == OP_Dist){

				//fetch the deleted key
				memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));


				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = deletedkeyDesc.leftNode;
				memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));
				//move data to delete the key
				memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

				//update page descriptor
				nodeDesc.size -= ((int)offset - (int)oldoffset);
				assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");



				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				assert(nodeDesc.size != 0 && "nodeDesc.size != 0");

				//write page to disk
				ixfileHandle.writePage(pageNum,page);
			}else if(treeop == OP_Merge){

				//fetch the deleted key
				memcpy(&deletedkeyDesc,(char *)leftsibling+oldoffset,sizeof(KeyDesc));


				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)leftsibling+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = deletedkeyDesc.leftNode;
				memcpy((char *)leftsibling+offset,&siblingkeyDesc,sizeof(KeyDesc));
				//move data to delete the key
				memcpy(bufferpage,(char *)leftsibling+offset,leftnodeDesc.size-offset);
				memcpy((char *)leftsibling+oldoffset,bufferpage,leftnodeDesc.size-offset);

				//update page descriptor
				leftnodeDesc.size -= ((int)offset - (int)oldoffset);
				//assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");



				memcpy((char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),&leftnodeDesc,sizeof(NodeDesc));
				assert(nodeDesc.size != 0 && "nodeDesc.size != 0");

				//write page to disk
				ixfileHandle.writePage(nodeDesc.prev,leftsibling);
			}else if(treeop == OP_None){


				//if nodeDesc.next != InvalidPage, offset and oldoffset do not change
				//fetch the deleted key
				memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));


				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = deletedkeyDesc.leftNode;
				memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));
				//move data to delete the key
				memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

				//update page descriptor

				nodeDesc.size -= ((int)offset - (int)oldoffset);
				//assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");



				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				//write page to disk
				ixfileHandle.writePage(pageNum,page);

				//for root page,
				//if the page is empty, update the directory to set the new root page
				if(nodeDesc.size == 0){
					if(IsRightest){
						//currentpageNum is rightNode

						//update root page number

						ixfileHandle.updateRootPage(templeftNode);
						//delete this empty page
						ixfileHandle.deletePage(pageNum);


					}else{
						//currentpageNum is leftNode

						//update root page number

						ixfileHandle.updateRootPage(templeftNode);
						//delete this empty page
						ixfileHandle.deletePage(pageNum);


					}
				}
			}
		}

	}else if(nexttreeop == OP_Dist){


		if(rightMost != 1){


			//if nodeDesc.next != InvalidPage, offset and oldoffset do not change
			//fetch the to-be-modified key
			memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));


			//update to-be-modified KeyDesc.keySize
			deletedkeyDesc.keySize = currentkeyDesc.keySize;
			memcpy((char *)page+oldoffset,&deletedkeyDesc,sizeof(KeyDesc));
			oldoffset += sizeof(KeyDesc);
			//modified the key value
			memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
			memcpy((char*)page+oldoffset,currentkeyDesc.keyValue,currentkeyDesc.keySize);
			oldoffset += currentkeyDesc.keySize;
			memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

			//update page descriptor
			nodeDesc.size -= ((int)offset - (int)oldoffset);

			assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");



			memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
			//write page to disk
			ixfileHandle.writePage(pageNum,page);



		}else{

			if(treeop == OP_Dist){


				//fetch the to-be-modified key
				memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));


				//update to-be-modified KeyDesc.keySize
				deletedkeyDesc.keySize = currentkeyDesc.keySize;
				memcpy((char *)page+oldoffset,&deletedkeyDesc,sizeof(KeyDesc));
				oldoffset += sizeof(KeyDesc);
				//modified the key value
				memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char*)page+oldoffset,currentkeyDesc.keyValue,currentkeyDesc.keySize);
				oldoffset += currentkeyDesc.keySize;
				memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

				//update page descriptor
				nodeDesc.size -= ((int)offset - (int)oldoffset);

				assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");



				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				//write page to disk
				ixfileHandle.writePage(pageNum,page);
			}else if(treeop == OP_Merge){


				//fetch the to-be-modified key
				memcpy(&deletedkeyDesc,(char *)leftsibling+oldoffset,sizeof(KeyDesc));


				//update to-be-modified KeyDesc.keySize
				deletedkeyDesc.keySize = currentkeyDesc.keySize;
				memcpy((char *)leftsibling+oldoffset,&deletedkeyDesc,sizeof(KeyDesc));
				oldoffset += sizeof(KeyDesc);
				//modified the key value
				memcpy(bufferpage,(char *)leftsibling+offset,leftnodeDesc.size-offset);
				memcpy((char*)leftsibling+oldoffset,currentkeyDesc.keyValue,currentkeyDesc.keySize);
				oldoffset += currentkeyDesc.keySize;
				memcpy((char *)leftsibling+oldoffset,bufferpage,leftnodeDesc.size-offset);

				//update page descriptor
				leftnodeDesc.size -= ((int)offset - (int)oldoffset);

				assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");



				memcpy((char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),&leftnodeDesc,sizeof(NodeDesc));
				//write page to disk
				ixfileHandle.writePage(nodeDesc.prev,leftsibling);
			}else if(treeop == OP_None){


				//fetch the to-be-modified key
				memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));


				//update to-be-modified KeyDesc.keySize
				deletedkeyDesc.keySize = currentkeyDesc.keySize;
				memcpy((char *)page+oldoffset,&deletedkeyDesc,sizeof(KeyDesc));
				oldoffset += sizeof(KeyDesc);
				//modified the key value
				memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char*)page+oldoffset,currentkeyDesc.keyValue,currentkeyDesc.keySize);
				oldoffset += currentkeyDesc.keySize;
				memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

				//update page descriptor
				nodeDesc.size -= ((int)offset - (int)oldoffset);

				assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");


				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				//write page to disk
				ixfileHandle.writePage(pageNum,page);
			}
		}

	}



	assert( keyValue == keyDesc.keyValue && "keyValue should equal to keyDesc.keyValue ");
	//checkKeyInt(ixfileHandle, attribute, page);
	//if this page is merged, check integrity from leftsibling
	if(rightMost == 1 && treeop == OP_Merge){
		//checkPageInt(ixfileHandle, leftsibling, nodeDesc.prev,false);
	}else{
		//checkPageInt(ixfileHandle, page, pageNum,false);
	}
	if(nexttreeop == OP_Error){
		treeop = nexttreeop;
	}
	free(trykey);
	free(deletepage);
	free(extrapage);
	free(leftsibling);
	free(rightsibling);
	free(nextpage);
	free(bufferpage);
	free(currentkeyDesc.keyValue);
	free(nextkeyDesc.keyValue);

	return treeop;

}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc;
	// find root first

	int Rightmost = 1;

	PageNum root = ixfileHandle.findRootPage();
	void *page = malloc(PAGE_SIZE);

	rc = ixfileHandle.readPage(root,page);

	KeyDesc keyDesc;

	keyDesc.keyValue=malloc(maxvarchar);

	// check if root needs to be split
	NodeDesc nodeDesc;
	memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );

	NodeType type = nodeDesc.type;
	PageSize size = nodeDesc.size;

	// traverse tree
	if(type==Leaf){
		//root page is leaf page

		TreeOp treeop=deleteFromLeaf(ixfileHandle, attribute, key, rid, page, root, keyDesc, Rightmost);


		free(keyDesc.keyValue);
		free(page);
		if( treeop == OP_Error ) return FAILURE;


		return 0;

	}else if(type == NonLeaf){
		//root page is NonLeaf

		TreeOp treeop=TraverseTreeDelete(ixfileHandle, attribute, key, rid, page, root, keyDesc, Rightmost);

		free(keyDesc.keyValue);
		free(page);
		if( treeop == OP_Error ) return FAILURE;

		return 0;
	}else{
		assert("root page should be Leaf or NonLeaf");
	}
	//

	return -1;
}


TreeOp IndexManager::deleteFromLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page,

		PageNum pageNum, KeyDesc &keyDesc, int rightMost)

{
	//checkPageInt(ixfileHandle, page, pageNum);
	TreeOp operation = OP_None;
	// retrieve node info
	NodeDesc nodeDesc;
	memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
	int offset = 0 ;
	// potential split page buffer
	void *nextPage = malloc(PAGE_SIZE);

	bool found = false;
	while( offset < nodeDesc.size ){
		DataEntryDesc ded;
		memcpy( &ded, (char*)page+offset, sizeof(DataEntryDesc) );

		ded.keyValue = malloc(ded.keySize);
		memcpy( ded.keyValue, (char*)page+offset+DataEntryKeyOffset, ded.keySize);

		// compare the key to find the deleted record
		int result = keyCompare(attribute, ded.keyValue, key);


		// if it only contains 1 RID , remove whole entries
		if( result == 0 && ded.numOfRID == 1 && ded.overflow == InvalidPage){

			// use nextPage as temp buffer

			int entrySize = sizeof(DataEntryDesc) + ded.keySize + sizeof(RID);

			memcpy( nextPage, (char*)page+offset+entrySize , nodeDesc.size - ( offset + entrySize ) );
			memcpy( (char*)page+offset , nextPage, nodeDesc.size - ( offset + entrySize ) );

			nodeDesc.size -= entrySize;
			memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
			found = true;
			free(ded.keyValue);
			break;
		}

		else if( result == 0 ) {
			// if it has more than two RIDs, remove the one in the list
			//printf("RID List offset is %d rid.pageNum is %d rid.slotNum is %d numrid %d\n",offset,rid.pageNum,rid.slotNum,ded.numOfRID);

			unsync = true;
			for( int i=0; i<ded.numOfRID; i++){

				RID t_rid;
				memcpy( &t_rid, (char*)page+offset+sizeof(DataEntryDesc)+ded.keySize+sizeof(RID)*i, sizeof(RID) );
				if( rid.pageNum == t_rid.pageNum && rid.slotNum == t_rid.slotNum ){
					int entrySize = sizeof(DataEntryDesc) + ded.keySize + sizeof(RID)*ded.numOfRID;
					int restDataSize = (ded.numOfRID-(i+1))*sizeof(RID) + ( nodeDesc.size - offset - entrySize ) ;
					memcpy( nextPage, (char*)page+offset+sizeof(DataEntryDesc)+ded.keySize+sizeof(RID)*(i+1), restDataSize );
					memcpy( (char*)page+offset+sizeof(DataEntryDesc)+ded.keySize+sizeof(RID)*i, nextPage, restDataSize );
					ded.numOfRID -= 1;
					memcpy( (char*)page+offset, &ded, sizeof(DataEntryDesc) );
					nodeDesc.size -= sizeof(RID);
					memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
					found = true;
					RC rc;
					rc = ixfileHandle.writePage( pageNum, page );
					assert( rc == SUCCESS );
					break; // break for loop
				}

			}
			// Dealing over flow page deletion
			if( !found && ded.overflow != InvalidPage ){
//			    printf("deal overflow\n");
			    RC rc;
			    PageNum overflowPageNum = ded.overflow;
			    while( !found ){
				DataEntryDesc oDed;
				rc = ixfileHandle.readPage( overflowPageNum , nextPage );
				assert( rc == SUCCESS && "Error in reading overflow page in deletion");
				memcpy( &oDed, nextPage , sizeof(DataEntryDesc) );

//				printf("numrid %d\n",oDed.numOfRID);
				for( int i=0; i< oDed.numOfRID; i++){
				    RID t_rid;
				    memcpy( &t_rid, (char*)nextPage+sizeof(DataEntryDesc)+ded.keySize + sizeof(RID)*i, sizeof(RID) );

//				printf("numrid %d %d %d %d %d\n",oDed.numOfRID,rid.pageNum,rid.slotNum,t_rid.pageNum,t_rid.slotNum);

				    if( rid.pageNum == t_rid.pageNum && rid.slotNum == t_rid.slotNum ){
					int restSize = sizeof(RID) * ( oDed.numOfRID - (i+1) );
					void *temp = malloc(PAGE_SIZE);
					memcpy( temp, (char*)nextPage+sizeof(DataEntryDesc)+ded.keySize + sizeof(RID)*(i+1), restSize );
					memcpy( (char*)nextPage+sizeof(DataEntryDesc)+ded.keySize+sizeof(RID)*i, temp, restSize );
					oDed.numOfRID -= 1;
					free(temp);
					memcpy( nextPage, &oDed, sizeof(DataEntryDesc) );
					rc = ixfileHandle.writePage( overflowPageNum, nextPage );
					assert( rc == SUCCESS);
					found = true;
					break;
				    }
				}
				if( !found && oDed.overflow != InvalidPage ){
				    overflowPageNum = oDed.overflow;
				}
		//		printf("numrid %d %d %d\n",oDed.numOfRID, rid.pageNum,rid.slotNum);
				// end of the RID list and last overflow page
				// clear all page
				if( oDed.overflow == InvalidPage && oDed.numOfRID == 0){
				    overflowPageNum = ded.overflow;
				    while( overflowPageNum != InvalidPage ){
					assert( overflowPageNum != 0 );
					rc = ixfileHandle.readPage( overflowPageNum , nextPage );
					memcpy( &oDed, nextPage, sizeof(DataEntryDesc) );
					ixfileHandle.deletePage( overflowPageNum );
					overflowPageNum = oDed.overflow;
				    }
				    assert( ded.numOfRID == 0 && "at this point all rid are cleared");
				    int entrySize = sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);

				    memcpy( nextPage, (char*)page+offset+entrySize , nodeDesc.size - ( offset + entrySize ) );
				    memcpy( (char*)page+offset , nextPage, nodeDesc.size - ( offset + entrySize ) );
				    nodeDesc.size -= entrySize;
				    memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
				    ixfileHandle.writePage( pageNum, page) ;
				    free(ded.keyValue);
				    return operation;
				}
			    }
			}

			free(ded.keyValue);
			break; // break while loop
		}

		free(ded.keyValue);
		offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);

	}


	if( !found ) { return OP_Error; }

	// if this page is root page, dont apply merge / redistribution.
	if( nodeDesc.size < LowerThreshold && pageNum != ixfileHandle.findRootPage() ){
		unsync = true;

		NodeDesc nNodeDesc; // next node ( could be previous )
		// right most leaf case
		if( nodeDesc.next == InvalidPage || rightMost == 1){

			// read previous page since there is no next page
			ixfileHandle.readPage( nodeDesc.prev, nextPage );
			memcpy( &nNodeDesc, (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );

			// re-distribution case , else it needs to merge
			if( nodeDesc.size + nNodeDesc.size > UpperThreshold ){
				offset = 0;
				while( offset < nNodeDesc.size / 3 ){
					DataEntryDesc ded;
					memcpy( &ded, (char*)nextPage+offset, sizeof(DataEntryDesc) );
					offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
				}
				// move next page stuff to current page
				// and push current page stuff back
				void *temp = malloc(PAGE_SIZE);
				memcpy( temp, page, nodeDesc.size );
				memcpy( page, (char*)nextPage+offset, nNodeDesc.size - offset );
				memcpy( (char*)page+( nNodeDesc.size - offset), temp, nodeDesc.size );
				nodeDesc.size += ( nNodeDesc.size - offset );
				memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc));
				nNodeDesc.size = offset;
				memcpy( (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), &nNodeDesc, sizeof(NodeDesc));
				free(temp);


				DataEntryDesc newKeyEntry;
				memcpy( &newKeyEntry, page, sizeof(DataEntryDesc) );
				memcpy( keyDesc.keyValue, (char*)page+sizeof(DataEntryDesc), newKeyEntry.keySize);

				keyDesc.rightNode = pageNum;
				keyDesc.leftNode = nodeDesc.prev;
				keyDesc.keySize = newKeyEntry.keySize;
				ixfileHandle.writePage( pageNum, page );
				ixfileHandle.writePage( nodeDesc.prev, nextPage );
				operation = OP_Dist;
			}else{
				// merge case, nextPage is actually previous page
				memcpy( (char*)nextPage+nNodeDesc.size, page, nodeDesc.size);
				nNodeDesc.size += nodeDesc.size;

				// if it's not the real right most leaf, after merge and deletion
				// append next page's prev link to current page's left page
				if( nodeDesc.next != InvalidPage ){
				    void *nnPage = malloc(PAGE_SIZE);
				    ixfileHandle.readPage( nodeDesc.next, nnPage );
				    NodeDesc ntNodeDesc;
				    memcpy( &ntNodeDesc, (char*)nnPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
				    ntNodeDesc.prev = nodeDesc.prev;
				    memcpy( (char*)nnPage+PAGE_SIZE-sizeof(NodeDesc), &ntNodeDesc, sizeof(NodeDesc) );
				    ixfileHandle.writePage( nodeDesc.next, nnPage );
				    free(nnPage);
				}
				nNodeDesc.next = nodeDesc.next;
				ixfileHandle.deletePage( pageNum );
				memcpy( (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), &nNodeDesc, sizeof(NodeDesc));
				operation = OP_Merge;
				ixfileHandle.writePage( nodeDesc.prev , nextPage );

			}



		}else{


			// normal case
			ixfileHandle.readPage( nodeDesc.next, nextPage );
			memcpy( &nNodeDesc, (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
			assert( nodeDesc.next != InvalidPage && rightMost != 1 && "WTF");
			// re-distribution case , else it needs to merge
			if( nodeDesc.size + nNodeDesc.size > UpperThreshold ){


				offset = 0;
				while( offset < nNodeDesc.size / 3 ){
					DataEntryDesc ded;
					memcpy( &ded, (char*)nextPage+offset, sizeof(DataEntryDesc) );
					offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
				}
				if( offset == nNodeDesc.size ){
				    free(nextPage);
				    return operation;
				}
				memcpy( (char*)page+nodeDesc.size, nextPage, offset );
				void *temp = malloc(PAGE_SIZE);
				memcpy( temp, (char*)nextPage+offset, nNodeDesc.size - offset );
				memcpy( nextPage , temp , nNodeDesc.size - offset );

				nNodeDesc.size -= offset;
				nodeDesc.size += offset;
				free(temp);

				// write NodeDesc back
				memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc));
				memcpy( (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), &nNodeDesc, sizeof(NodeDesc));

				DataEntryDesc newKeyEntry;
				memcpy( &newKeyEntry, nextPage, sizeof(DataEntryDesc) );
				memcpy( keyDesc.keyValue, (char*)nextPage+sizeof(DataEntryDesc), newKeyEntry.keySize);
				keyDesc.leftNode = pageNum;
				keyDesc.rightNode = nodeDesc.next;
				keyDesc.keySize = newKeyEntry.keySize;

				ixfileHandle.writePage( pageNum, page );
				ixfileHandle.writePage( nodeDesc.next, nextPage );

				operation = OP_Dist;
			}else{
				//checkPageInt(ixfileHandle, page, pageNum,true);

				// merge case
				memcpy( (char*)page+nodeDesc.size, nextPage, nNodeDesc.size );
				// update info to the next two page
				if( nNodeDesc.next != InvalidPage ){
				    // overwrite nextPage with next 2 Page, since we've already merge it to our page
				    ixfileHandle.readPage(nNodeDesc.next, nextPage );
				    NodeDesc ntNodeDesc;
				    memcpy( &ntNodeDesc, (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc));
				//    printf("%d %d\n",ntNodeDesc.prev, nNodeDesc.next);
				    ntNodeDesc.prev = pageNum;
				    memcpy( (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), &ntNodeDesc, sizeof(NodeDesc));
				    ixfileHandle.writePage( nNodeDesc.next, nextPage );
				}

				// delete the next page
				ixfileHandle.deletePage( nodeDesc.next );

				// update the current page info
				// write page info back
				nodeDesc.size += nNodeDesc.size;
				nodeDesc.next = nNodeDesc.next;
				memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
				operation = OP_Merge;
				ixfileHandle.writePage( pageNum, page );
			}

		}

	}else{

		ixfileHandle.writePage( pageNum, page );
	}


	free(nextPage);
	return operation;
}

///@brief	Comparsion between two keys,
///			if KeyA greater than KeyB , return > 0
///			if KeyA smaller than KeyB , return < 0
///			if eauqlity return 0
///@param	attribute, indict the type of key
///@param	keyA, value of keyA
///@param	keyB, value of keyB

int IndexManager::keyCompare(const Attribute &attribute, const void *keyA, const void* keyB)
{
	AttrType type = attribute.type;

	int i_a , i_b;
	switch( type ){
		case TypeInt:
			memcpy( &i_a , keyA , sizeof(int));
			memcpy( &i_b , keyB , sizeof(int));
			return (i_a - i_b);
			break;

		case TypeReal:
			float f_a, f_b;
			memcpy( &f_a , keyA , sizeof(float));
			memcpy( &f_b , keyB , sizeof(float));
			f_a = (f_a-f_b) * 100000;
			return (int)f_a;
			break;

		case TypeVarChar:
			memcpy( &i_a , keyA , sizeof(int));
			memcpy( &i_b , keyB , sizeof(int));
			string sa ((char*)keyA + sizeof(int),i_a);
			string sb ((char*)keyB + sizeof(int),i_b);
			return sa.compare(sb);

	}
	assert("keycompare error!");
}

///@brief	get the size of key
///@param	attribute, indict the type of the key
///@param	key, the value of the key
int IndexManager::getKeySize(const Attribute &attribute, const void *key)
{
	AttrType type = attribute.type;
	int size = -1;
	switch( type ){
		case TypeInt:
			return sizeof(int);

		case TypeReal:
			return sizeof(float);

		case TypeVarChar:
			memcpy( &size, key , sizeof(int) );
			assert( size >= 0 && "something wrong with getting varchar key size\n");

			return size+sizeof(int);
	}

}

///@brief	print key to screen according to the attribute
///@param	attribute, the attribute of the key value
///@param	key, the value of the key
void IndexManager::printKey(const Attribute &attribute, const void *key)
{
	AttrType type = attribute.type;
	int size = -1;
	switch( type ){
		case TypeInt:
			printf("%d", *((int*)key));
			return;

		case TypeReal:
			printf("%f", *((float*)key));
			return;

		case TypeVarChar:
			memcpy( &size, key , sizeof(int) );
			assert( size >= 0 && "something wrong with getting varchar key size\n");
			//assert( size < 50 && "something wrong with getting varchar key size\n");
			string sa( (char*)key + 4 , size );
			printf("%s", sa.c_str());
			return;
	}

}

///@brief	initialize the scanning, call ix_ScanIterator.ini to realize it
///@param	ixfilehandle
///@param	attribute, the attribute of the key value
///@param	lowkey, the lower bounder of the key value
///@param	highkey, the upper bounder of the  key value
///@param	lowKeyInclusive, if it is true, then, we will include the lower bounder
///@param	highKeyInclsive, if it is true, then, we will include the upper bounder
///@param	ix_ScanIterator, the initial function is in the ix_ScanIterator
///@return	initialize the scanning
RC IndexManager::scan(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		const void      *lowKey,
		const void      *highKey,
		bool		lowKeyInclusive,
		bool        	highKeyInclusive,
		IX_ScanIterator &ix_ScanIterator)
{
	return ix_ScanIterator.init(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

///@brief	printBtree, it will call print B tree to do that
///@param	ixfileHandle
///@param	attribute, the attribute of the key
void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute)
{
	RC rc;
	void *page = malloc(PAGE_SIZE);
	PageNum rootPage = ixfileHandle.findRootPage();

	ixfileHandle.readPage(rootPage,page);
	printBtree(ixfileHandle, attribute, page, 0, rootPage);
	free(page);
}

///@breif	recursively call the printBtree to print B tree.
///			deal with two cases. one is non-leaf node, another is leaf node
///@param	ixfileHandle
///@param	attribute, the attribute of the key. it is used to printKey value
///@param	page, the entries are  stored in the page
///@param	depth, the depth of the subtree. it is starting at 0. When recursively call the printBtree, it will plus one.
///@param	nodeNum, the page number of node which will be printed
///@return

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page, int depth, PageNum nodeNum)
{
	RC rc;
	rc = ixfileHandle.readPage( nodeNum, page );
	assert( rc == SUCCESS && "Something wrong in read page in printBTree subTree");

	NodeDesc nodeDesc;
	memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
	// print indent based on depth
	for(int i=0; i < depth; i++) printf("\t");

	if( nodeDesc.type == NonLeaf ){

		// vector to store all links ( PageNum of sub-tree )
		vector<PageNum> links;
		int offset = 0;
		KeyDesc keyDesc;
		DataEntryDesc ded;
		printf("{\"keys\":[");

		// print key in non-leaf first
		while( offset < nodeDesc.size ){
			if( offset > 0 ) printf(",");
			memcpy( &keyDesc, (char*)page + offset, sizeof(KeyDesc));
			keyDesc.keyValue = malloc( keyDesc.keySize );
			memcpy( keyDesc.keyValue,(char *) page + offset+sizeof(KeyDesc), keyDesc.keySize);

			// print key
			printf("\""); printKey( attribute, keyDesc.keyValue ); printf("\"");

			// add links to vector ( without last one )
			links.push_back( keyDesc.leftNode );
			free( keyDesc.keyValue);
			offset += sizeof(KeyDesc) + keyDesc.keySize;
		}
		printf("],\n");

		// add the last link to vectory
		links.push_back( keyDesc.rightNode );

		// start to traverse all children
		for(int i = 0; i < depth; i++) printf("\t");
		printf("\"children\":[");

		for( int i = 0; i < links.size(); i ++){
			printBtree(ixfileHandle,attribute,page,depth+1,links[i]);
			if( i < links.size() - 1 ) printf(",\n");
		}
		printf("]}\n");
		return;
	}

	if( nodeDesc.type == Leaf ){
		int offset = 0;
		DataEntryDesc ded;
		printf("{\"keys\": [");
		while( offset < nodeDesc.size ){
			if( offset > 0 ) printf(",");
			memcpy( &ded, (char*)page + offset, sizeof(DataEntryDesc) );
			void *key = malloc( ded.keySize );
			memcpy( key , (char*)page + offset+sizeof(DataEntryDesc), ded.keySize);
			// print key
			printf("\""); printKey( attribute, key ); printf(":[");
			// print RIDs
			for(int i = 0; i < ded.numOfRID; i++){
				RID rid;
				memcpy( &rid, (char*)page + offset + sizeof(DataEntryDesc) + ded.keySize + i * sizeof(RID), sizeof(RID) );
				printf("(%d,%d)",rid.pageNum,rid.slotNum);
				if( i < ded.numOfRID-1 ) printf(",");
			}
			printf("]\"");
			free(key);
			offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID * sizeof(RID) ;
		}
		printf("]}\n");
		return;
	}

	assert( false && "It should return before this, error in printBtree!" );
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

///@breif	the main job of the function is to  find the page which contains the first satisfied entry
///			deal with two cases. one is non-leaf node, another is leaf node
///@param	ixfileHandle
///@param	attribute, the attribute of the key. it is used to printKey value
///@param	page, the entries are  stored in the page
///@param	depth, the depth of the subtree. it is starting at 0. When recursively call the printBtree, it will plus one.
///@param	nodeNum, the page number of node which will be printed
///@return

RC IX_ScanIterator::init(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		const void      *lowKey,
		const void      *highKey,
		bool		lowKeyInclusive,
		bool        	highKeyInclusive)
{
	if( ixfileHandle.isReadable() == -1 ) return FAILURE;
	this->overflow = false;

	this->ixfileHandle = ixfileHandle;
	this->attribute = attribute;
	this->lowKeyInclusive = lowKeyInclusive;
	this->highKeyInclusive = highKeyInclusive;

	this->page = malloc(PAGE_SIZE);
	this->overflowPage = malloc(PAGE_SIZE);
	this->lowKeyNull = false;
	this->highKeyNull = false;
	im = IndexManager::instance(); // to use the traverseTree
	im->unsync = false;
	float INF = INFINITY / 2, NINF = -INFINITY / 2;

	if( lowKey == NULL ){ //if it does not have lower bounder, then let the lower bounder be the negative infinity
		this->lowKeyNull = true;
		this->lowKey = malloc(sizeof(float));
		memcpy( this->lowKey , &NINF, sizeof(float));

	}else{ // it has the lower bounder
	    int keySize = im->getKeySize(this->attribute,lowKey);
	    this->lowKey = malloc( keySize );
	    memcpy( this->lowKey, lowKey , keySize );

	}
	if( highKey == NULL ){ // if it does not have upper bounder, then let the upper bounder be the positive infinity
		this->highKeyNull = true;
		this->highKey = malloc(sizeof(float));
		memcpy( this->highKey , &INF, sizeof(float));

	}else{

	    int keySize = im->getKeySize(this->attribute,highKey);
	    this->highKey = malloc( keySize );
	    memcpy( this->highKey, highKey , keySize );

	}

	RC rc;
	// find root first
	PageNum root = ixfileHandle.findRootPage(); // scanning starts from the root page
	rc = ixfileHandle.readPage(root, page);


	// Get Root Page Info
	NodeDesc nodeDesc;
	memcpy( &nodeDesc , (char*)page + PAGE_SIZE - sizeof(NodeDesc), sizeof(NodeDesc));


	// Start Tree Traversal if root is non-leaf
	PageNum returnPageNum = 0;
	if( nodeDesc.type == NonLeaf ){
		im->TraverseTree( ixfileHandle, attribute, this->lowKey, page, root, returnPageNum); //get the returnPageNum, scan will start at this page
		assert( root != returnPageNum && "root should not be leaf in this case" );
		assert( returnPageNum >=1 && "something went wrong when traversing tree in scan ");
		pageNum = returnPageNum;

		rc = ixfileHandle.readPage(returnPageNum, page);
	}


	offsetToKey = 0;
	offsetToRID = 0;
	while( true ){ // scan the entries in the page one by one, check if there is an entry satisfied. if there is no entry satisfied then there is no entry satisfied in the whole tree

		DataEntryDesc ded;
		memcpy( &ded, (char*)page + offsetToKey, sizeof(DataEntryDesc)); // get the entry descriptor

		// retrieve key value
		void *key = malloc( ded.keySize );
		memcpy( key, (char*)page + offsetToKey + sizeof(DataEntryDesc), ded.keySize );

		if( this->lowKeyNull ){
		    free(key);

		    return SUCCESS;
		}
		if( lowKeyInclusive ){

			if( im->keyCompare( attribute , key , this->lowKey ) >= 0 ){
				free(key);

				return SUCCESS;
			}
		}else{
			if( im->keyCompare( attribute , key , this->lowKey ) > 0 ){
				free(key);

				return SUCCESS;
			}
		}

		free(key);

		offsetToKey += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID * sizeof(RID);
	}


	return FAILURE;
}
///@brief	scan the entries one by one.
///@param	rid, return the rid of the entry which is satisfied
///@param	key, the key value of the satisfied entry

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	RC rc;

	// check if someone called deleteEntry in indexManager
	// if someone did, sync the location
	if( im->unsync ){

	    //assert(false);
	    // find root first
	    PageNum root = ixfileHandle.findRootPage();
	    rc = ixfileHandle.readPage(root,page);

	   // Start Tree Traversal if root is non-leaf
	    NodeDesc nodeDesc;
	    memcpy( &nodeDesc, (char*)page + PAGE_SIZE - sizeof(NodeDesc), sizeof(NodeDesc) );
	    PageNum returnPageNum = 0;

	    if( nodeDesc.type == NonLeaf ){
	    	im->TraverseTree( ixfileHandle, attribute, key,  page, root, returnPageNum);
	    	assert( root != returnPageNum && "root should not be leaf in this case" );
	    	assert( returnPageNum >=1 && "something went wrong when traversing tree in scan ");
	    	rc = ixfileHandle.readPage(returnPageNum, page);
	    }

	    int offset = 0 ;
	    while( offset < nodeDesc.size ){
	    	DataEntryDesc ded;
	    	memcpy( &ded, (char*)page+offset, sizeof(DataEntryDesc) );

	    	void *dataKey = malloc(ded.keySize);
	    	memcpy( dataKey, (char*)page+offset+sizeof(DataEntryDesc), ded.keySize);
	    	int compare = im->keyCompare( attribute, key , dataKey );
	    	free(dataKey);

	    	if( compare == 0 ){
	    		offsetToKey = offset;
	    		offsetToRID = 0;
	    		if( !overflow )
	    			im->unsync = false;
	    		break;
	    	}
	    	offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID) ;
	    }

	}



	// check if the offset exceeds the page size
	NodeDesc nodeDesc;
	memcpy( &nodeDesc, (char*)page + PAGE_SIZE - sizeof(NodeDesc), sizeof(NodeDesc) );

	if( overflow ){ //it has the overflow pages
	    DataEntryDesc oDed;
	    memcpy( &oDed, overflowPage, sizeof(DataEntryDesc));

	    if( im->unsync ){

		// unsync re-read page from disk
	    	rc = ixfileHandle.readPage( currentOverflowPage, overflowPage);
	    	memcpy( &oDed, overflowPage, sizeof(DataEntryDesc));

	    	if( overflowRID < oDed.numOfRID ){
	    		overflowRID = 0;
	    		memcpy( &rid, (char*)overflowPage + sizeof(DataEntryDesc) + oDed.keySize + overflowRID * sizeof(RID), sizeof(RID) );
	    		im->unsync=false;

	    		return SUCCESS;
	    	}else{
	    		overflowRID = 0;

	    		// if it still has a overflow chain behind it, read it and return new RID
	    		if( oDed.overflow != InvalidPage ){
	    			currentOverflowPage = oDed.overflow;
	    			ixfileHandle.readPage( oDed.overflow, overflowPage );
	    			memcpy( &oDed, overflowPage, sizeof(DataEntryDesc));
	    			memcpy( &rid, (char*)overflowPage + sizeof(DataEntryDesc) + oDed.keySize + overflowRID * sizeof(RID), sizeof(RID) );

	    			im->unsync=false;
	    			return SUCCESS;
	    		}else{
	    			overflow = false;
	    		}
	    	}
	    	im->unsync=false;
	    }else{

	    	if( overflowRID < oDed.numOfRID ){
	    		memcpy( &rid, (char*)overflowPage + sizeof(DataEntryDesc) + oDed.keySize + overflowRID * sizeof(RID), sizeof(RID) );
	    		overflowRID ++;
	    		return SUCCESS;

	    	}else{
	    		overflowRID = 0;

	    		// if it still has a overflow chain behind it, read it and return new RID
	    		if( oDed.overflow != InvalidPage ){
	    			ixfileHandle.readPage( oDed.overflow, overflowPage );
	    			memcpy( &oDed, overflowPage, sizeof(DataEntryDesc));
	    			memcpy( &rid, (char*)overflowPage + sizeof(DataEntryDesc) + oDed.keySize + overflowRID * sizeof(RID), sizeof(RID) );
	    			overflowRID ++;
	    			return SUCCESS;
	    		}else{

	    			overflow = false;
	    		}
	    	}

	    }
	}


	if( offsetToKey >= nodeDesc.size ){
		if( nodeDesc.next == InvalidPage ) return IX_EOF;

		pageNum = nodeDesc.next; // get next page
		rc = ixfileHandle.readPage( nodeDesc.next, page );
		assert( rc == SUCCESS && "something wrong in readpage in getNextEntry" );
		// Reset all offets for new pages;
		offsetToKey = 0;
		offsetToRID = 0;

		memcpy( &nodeDesc, (char*)page + PAGE_SIZE - sizeof(NodeDesc), sizeof(NodeDesc) );

	}

	DataEntryDesc ded;
	memcpy( &ded, (char*)page + offsetToKey, sizeof(DataEntryDesc) );

	// Read key and compare
	memcpy( key, (char*)page + offsetToKey + sizeof(DataEntryDesc), ded.keySize);

	int result = im->keyCompare( attribute, key , highKey );
	if( highKeyInclusive ){

		if( result > 0 ) return IX_EOF;
	}else{

		if( result >= 0 ) return IX_EOF;
	}

	// Read rid and return
	memcpy( &rid, (char*)page + offsetToKey + sizeof(DataEntryDesc) + ded.keySize + offsetToRID * sizeof(RID), sizeof(RID) );


	offsetToRID++;

	if( offsetToRID == ded.numOfRID ){

		// overflow case
		if( ded.overflow != InvalidPage ){
		    overflow = true;
		    currentOverflowPage = ded.overflow;
		    ixfileHandle.readPage( ded.overflow, overflowPage );
		    overflowRID = 0;
		}

		// still increase the regular page/rid offset
		offsetToKey += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID) ;
		offsetToRID = 0;
	}


	return SUCCESS;
}

RC IX_ScanIterator::close()
{
	free(page);
	free(overflowPage);
	free(lowKey);
	free(highKey);
	return SUCCESS;
}

IXFileHandle::IXFileHandle()
{
	isOpen = false;
	error = -1;
}


IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::isReadable()
{
	return error;
}
RC IXFileHandle::initFilePointer(const string &fileName)
{
	if(isOpen) return FAILURE;
	error = fileHandle.initFilePointer( fileName );
	return error;
}

RC IXFileHandle::closeFilePointer()
{
	fileHandle.isOpen = false;
	RC rc = fileHandle.closeFilePointer();
	error = -1;
	return rc;
}

///@breif	 PageNum ( unsigned int ) is 4 byte
///	 		 A directory can have (4096 / 4) - 1 = 1023 cells to describe pages ( leaf & non-leaf );
///			 Each directory start with index 1, where use 0 or 1 to indicate empty or used
///			 The first directory is a specical case where 1st entry stores root node's pagenum ( positive integer )

PageNum IXFileHandle::findFreePage()
{
	// PageNum ( unsigned int ) is 4 byte
	// A directory can have (4096 / 4) - 1 = 1023 cells to describe pages ( leaf & non-leaf );
	// Each directory start with index 1, where use 0 or 1 to indicate empty or used
	// The first directory is a specical case where 1st entry stores root node's pagenum ( positive integer )
	void *page = malloc(PAGE_SIZE);

	PageNum dirPageNum = 0 ; // Read first directory

	while( true ){
		// search from directory, if read directroy fail, means need to allocate new directory in file
		if( readPage(dirPageNum,page) == FAILURE ) {
			for(int i=1; i< IXDirectorySize ; i++ ){
				unsigned int empty = 0;
				memcpy( (char*)page+i*sizeof(PageNum) , &empty, sizeof(PageNum) );
			}
		}

		// lookup free page in directory
		for(int i=1; i<IXDirectorySize; i++){
			PageNum pageNum;
			memcpy( &pageNum, (char*)page+sizeof(PageNum)*i , sizeof(PageNum) );
			// find fresh page, update it to directory and return the page ID
			if( pageNum == 0 ){
				pageNum = 1;
				memcpy( (char*)page+i*sizeof(PageNum) , &pageNum, sizeof(PageNum) );
				writePage(dirPageNum,page);
				free(page);
				this-> ixAppendPageCounter ++;
				return (PageNum)(i+dirPageNum);
			}
		}
		dirPageNum += IXDirectorySize; // find next directory;
	}

	// un-reachable code
	free(page);
	assert(false && "should be unreachable");
}

///@brief	update the root page
///@param	pagenum, the page of root
///@return	successful, 0, else -1
RC IXFileHandle::updateRootPage(PageNum pageNum)
{
	assert ( pageNum > 0 && "root can not be zero\n");
	void *page = malloc(PAGE_SIZE);
	// read root directory
	if( readPage(0, page) == FAILURE ) {
		assert( false && "Read root director should not fail" );
		return FAILURE;
	}
	memcpy( (char*)page+sizeof(PageNum), &pageNum, sizeof(PageNum) );
	RC rc;
	rc = writePage(0,page);
	assert( rc == SUCCESS && "something wrong in udpate rootpage");
	free(page);
	return SUCCESS;
}

///@brief	PageNum ( unsigned int ) is 4 byte
///			A directory can have (4096 / 4) - 1 = 1023 cells to describe pages ( leaf & non-leaf );
///			Each directory start with index 1, where use 0 or 1 to indicate empty or full
///			The first directory is a specical case where 1st entry stores root node's pagenum ( positive integer )
///@return	successful, 0, else -1
PageNum IXFileHandle::findRootPage()
{

	int directorySize = PAGE_SIZE / sizeof( PageNum );
	void *page = malloc(PAGE_SIZE);
	PageNum root = 1; // assume root page is 1 if the whole structure hasn't been initialized
	if( readPage(0, page) == FAILURE ) {

		for(int i=1; i< directorySize; i++ ){
			PageNum empty = 0;
			memcpy( (char*)page+i*sizeof(PageNum) , &empty, sizeof(PageNum) );
		}
		memcpy( (char*)page+sizeof(PageNum), &root, sizeof(PageNum) ); // write root to 1st directory
		RC rc;

		this->ixAppendPageCounter ++;
		rc = writePage(0, page);
		assert( rc == SUCCESS && "write root page failed" );

		NodeDesc nodeDesc;
		nodeDesc.next=InvalidPage;
		nodeDesc.type=Leaf;
		nodeDesc.size=0;
		memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));

		this->ixAppendPageCounter ++;
		rc = writePage(1,page);
		assert(rc == SUCCESS && "Fail to write root page as leaf page");

	}else{
		// read 1st directory's 1st entry
		memcpy( &root , (char*)page+sizeof(PageNum) , sizeof(PageNum) );
	}
	assert( root > 0 && "Root can not be page zero" );
	free(page);
	return root;
}

///@brief	read the page by given pagenum, put the data on the page to the *data
///@param	pagenum, given page number
///@param	data, contain the data on the page
///@return	call the readpage in filehanlde to realize this
RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
	this->ixReadPageCounter++;
	return fileHandle.readPage(pageNum,data);
}
///@brief	write the data on the page by given page number
///@param	pagenum, given page number
///@param	data, contains the data which is needed to be written on the page
///@return	call the writePage in the fileHandle to realize this
RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
	this->ixWritePageCounter++;
	return fileHandle.writePage(pageNum,data);
}

RC IXFileHandle::deletePage(PageNum pageNum)
{
	PageNum dir = pageNum / IXDirectorySize;
//	printf("deletePage Num %u\n",pageNum );
//	assert( dir % IXDirectorySize != 0 && "Not valid directory index\n" );
	dir = IXDirectorySize * dir;
	assert( dir % IXDirectorySize == 0 && "Directory Page should be multiple of IXDirectorySize");
	int pageIndex = pageNum % IXDirectorySize;
	assert( pageIndex >= 1 && pageIndex < IXDirectorySize && "Not valid page index \n");
	assert( findRootPage() != pageNum && "WTF, Root can not be deleted\n");
	RC rc = -1;


	void *page = malloc(PAGE_SIZE);
	rc = fileHandle.readPage( dir, page );

	if ( rc == -1 ) cout << "cannot readPage " << endl;
	// mark the slot as empty
	PageNum empty = 0;
	memcpy( (char*)page+pageIndex*sizeof(PageNum) , &empty , sizeof(PageNum) );
	fileHandle.writePage( dir, page );
	free(page);
	return SUCCESS;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = this->ixReadPageCounter;
	writePageCount = this->ixWritePageCounter;
	appendPageCount = this->ixAppendPageCounter;
	return SUCCESS;
}

void IX_PrintError (RC rc)
{
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
RC IndexManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
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
RC IndexManager::insertRecordIntoAppointedPage(FileHandle &fileHandle, char* internalRecord, int recordLen, RID &rid, int pageNumber) {
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
RC IndexManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
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
RC IndexManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
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
RC IndexManager::getRecordActualSize(const vector<Attribute> &recordDescriptor, const void* data, int& recordActualSize) {
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
RC IndexManager::createNewPageAndInsertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const int &internalRecordLen, RID &rid) {
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
RC IndexManager::transRecordFromDefaultToInternal(const vector<Attribute> &recordDescriptor, char* internalRecord, int& internalRecordLen, const void* data) {
	if(internalRecordLen == 0) return 0;

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
RC IndexManager::transRecordFromInternalToDefault(const vector<Attribute> &recordDescriptor, void* data, const char* internalRecord, const int &internalRecordLen) {
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
RC IndexManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
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
RC IndexManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
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
RC IndexManager::updateRecordInCurrentPage(FileHandle &fileHandle,
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
RC IndexManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
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


