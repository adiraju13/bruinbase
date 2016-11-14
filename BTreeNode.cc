#include "BTreeNode.h"
#include <cstring>
#include <stdlib.h>
#include <iostream>

using namespace std;

BTLeafNode::BTLeafNode()
{
	//define some constants that will be used to run through the leaf
	size_of_element = sizeof(int) + sizeof(RecordId);
	size_of_pageID = sizeof(PageId);
	size_of_page = PageFile::PAGE_SIZE;
	numKeys = 0;

	//set buffer to 0s
	memset(buffer, 0, size_of_page);
}

void BTLeafNode::setNumKeys(int nKeys)
{
	numKeys = nKeys;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	//using PageFile API to read the page into the buffer
	return pf.read(pid, buffer); 
}
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */

RC BTLeafNode::write(PageId pid, PageFile& pf)
{	
	//using the PageFile to write into the page from the buffer 
	return pf.write(pid, buffer); 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
	return numKeys;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid){
	//check if there is space for the node?
	if (size_of_page - size_of_pageID - (numKeys * size_of_element) < size_of_element){
		return RC_NODE_FULL;
	}

	int byteOffset = 0;
	for (int i = 0; i < numKeys; i++){
		int currKeyValue;
		memcpy(&currKeyValue, buffer + byteOffset, sizeof(int));
		if (currKeyValue > key){
			break;
		}
		byteOffset += size_of_element;
	}

	//create temp set of memory for the new buffer
	char temp_buffer[size_of_page];

	//add stuff from buffer that stays the same
	memcpy(temp_buffer, buffer, byteOffset);
	memcpy(temp_buffer + byteOffset, &key, sizeof(int));
	memcpy(temp_buffer + byteOffset + sizeof(int), &rid, sizeof(RecordId));
	memcpy(temp_buffer + byteOffset + size_of_element, buffer + byteOffset, size_of_page-byteOffset);

	//now recopy the temp buffer back into the buffer:
	memcpy(buffer, temp_buffer, size_of_page);

	numKeys++;
	return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	//sibling has stuff in it
	if (sibling.getKeyCount() != 0)
		return RC_INVALID_ATTRIBUTE;

	//space to put the node
	if (size_of_page - size_of_pageID - (numKeys * size_of_element) >= size_of_element){
		return RC_INVALID_ATTRIBUTE;
	}

	PageId currNextPage = getNextNodePtr();

	int index;
	locate(key, index);
	int numKeysInFirst;
	int numKeysInSecond;
	bool insertFirstHalf = false;

	if (index >= numKeys/2){
		insertFirstHalf = false;
		numKeysInFirst = index + 1;
	}else{
		insertFirstHalf = true;
		numKeysInFirst = index;
	}
	numKeysInSecond = numKeys - numKeysInFirst;

	//zero out sibling buffer:
	memset(sibling.buffer, 0, size_of_page);

	//split the current buffer
	memcpy(sibling.buffer, buffer + (numKeysInFirst * size_of_element), numKeysInSecond * size_of_element);
	memset(buffer + (numKeysInFirst * size_of_element), 0, (numKeysInSecond * size_of_element) - sizeof(PageId));

	setNumKeys(numKeysInFirst);
	sibling.setNumKeys(numKeysInSecond);

	sibling.setNextNodePtr(currNextPage);

	if (insertFirstHalf){
		insert(key, rid);
	}
	else{
		sibling.insert(key, rid);
	}

	return 0; 
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{	
	if (numKeys == 0){
		eid = 0;
		return RC_NO_SUCH_RECORD;
	} 
	int leafKey = 0;
	for (int i = 0; i < numKeys; i++){
		memcpy(&leafKey, buffer + (i*size_of_element), sizeof(int));
		if (leafKey == searchKey){
			eid = i;
			return 0;
		}
		else if (leafKey > searchKey){
			eid = i;
			return RC_NO_SUCH_RECORD;
		}
	}
	eid = numKeys;
	return RC_NO_SUCH_RECORD; 
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
	if (eid < 0 || eid >= numKeys){
		return RC_NO_SUCH_RECORD;
	}
	memcpy(&key, buffer + (eid * size_of_element), sizeof(int));
	memcpy(&rid, buffer+ (eid * size_of_element) + sizeof(int), sizeof(RecordId));
	return 0; 
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	PageId pid;
	memcpy (&pid, buffer + (size_of_page - size_of_pageID), size_of_pageID);
	return pid; 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{  
	if (pid < 0)
		return RC_INVALID_PID;

	memcpy(buffer + size_of_page - size_of_pageID, &pid, size_of_pageID);
	return 0;
}

/*************************************************************************
**************************************************************************
**************************************************************************
**************************************************************************
**************************************************************************
***************************************************************************/

BTNonLeafNode::BTNonLeafNode() {
	numKeys=0;
}


/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf){
	return pf.read(pid,buffer); //Using PageFile function to read from specific page
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf){
	return pf.write(pid,buffer); //Using PageFile function to write to specific page
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount() {
	return numKeys;
}

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid){
	//Each pair has a certain size
	//Will be represented by the variable size
	//Want to see how many of these pairs can fit in buffer
	int size = sizeof(PageId) + sizeof(int);
	int pageSize = PageFile::PAGE_SIZE;
	//max number of pairs that can fit 
	int max = (pageSize-sizeof(PageId))/size;
	int currTotal = getKeyCount();

	//cout << "The value of currTotal - " << currTotal << endl;

	if(currTotal+1 > max) {return RC_NODE_FULL;}

	char* pointer = buffer+8;

	int count=8;
	int currKey;
	for (int i = 0; i < getKeyCount(); i++){
		memcpy(&currKey,pointer,sizeof(int));
		cout <<"curryKey: " << currKey << endl;
		if(currKey==0 || currKey>key) {break;}
		pointer = pointer + size; //pointer moves smh 
		count += size;
	}

	//After this loop
	//We have found the specific place to insert
	//Now we will be manipulating by copying data between the buffer and new buffer
	//The new buffer will contain the inserted value

	//char* newBuffer[pageSize]; //Represents the new buffer
	char* newBuffer = (char*)malloc(pageSize);

	memcpy(newBuffer,buffer,count); //Will copy all values from buffer to new buffer till count
	memcpy(newBuffer+count,&key,sizeof(int));
	memcpy(newBuffer+count+sizeof(int),&pid,sizeof(PageId));

	//The new value has been inserted
	//Now we need to copy the rest of the buffer to newBuffer

	//Non leaf nodes need to take in account for the 8 extra bytes

	int extra = getKeyCount()*size - count + 8;

	memcpy(newBuffer+count+size,buffer+count,extra);

	//Insertion has completed and now we need to replace data of buffer w/new buffer

	memcpy(buffer,newBuffer,pageSize);
	free(newBuffer);

	//cout << "The value of buffer is - " << buffer << endl;

	numKeys++;

	//cout << "Insertion successful" << endl;

	RC retValue = 0;
	return retValue;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey){
	int size = sizeof(PageId) + sizeof(int);
	int pageSize = PageFile::PAGE_SIZE;
	//max number of pairs that can fit 
	int max = (pageSize-sizeof(PageId))/size;

	if(!(getKeyCount()>=max)){return RC_INVALID_ATTRIBUTE;}
	if(sibling.getKeyCount()!=0) {return RC_INVALID_ATTRIBUTE;}

	//Need to split the current node 
	//So we can compare shit, and figure out where the
	//Marker node should be and accordingly, update the structure of the tree

	int half = ((int)((getKeyCount()+1)/2)); //Not this represents the keys for the first half

	//Now that we have the half, we need to find the SPECIFIC index value

	int indexToSplit = half*size + 8;

	//Next, is to find where we will be exactly splitting from
	//We'll compare the last key of the first half, and first key of the second half

	int lastKeyFirstHalf = 0;
	int firstKeySecondHalf = 0;

	//Get the values for the keys so the comparison shit can start

	memcpy(&lastKeyFirstHalf,buffer+indexToSplit-8,sizeof(int));
	memcpy(&firstKeySecondHalf,buffer+indexToSplit,sizeof(int));

	if(key < lastKeyFirstHalf) { //Get all to the right of half and store into the new sibling buffer

		//memcpy(sibling.buffer+8,buffer+indexToSplit,pageSize-half);
		int keyToInsert = 0;
		PageId pidToInsert = 0;
		for (int i = 0; i < getKeyCount() - half; i++){
			memcpy(&keyToInsert, buffer + indexToSplit + (i*size), sizeof(int));
			memcpy(&pidToInsert, buffer + indexToSplit + (i*size) + sizeof(int), sizeof(PageId));
			sibling.insert(keyToInsert, pidToInsert);

		}
		//sibling.numKeys = getKeyCount()-half; //Will update the # of keys
		memcpy(&midKey,buffer+indexToSplit-8,sizeof(int));
		memcpy(sibling.buffer,buffer+indexToSplit-4,sizeof(PageId));


		//Now that we,ve moved things we need to clear the second half of the 
		//Current buffer

		std::fill(buffer+indexToSplit-8,buffer+pageSize,0);
		numKeys = half-1;
		insert(key,pid);
	}

	else if(key > firstKeySecondHalf) {
		//memcpy(sibling.buffer+8,buffer+indexToSplit+8,pageSize-indexToSplit-8);
		//sibling.numKeys = getKeyCount()-half-1;

		int keyToInsert = 0;
		PageId pidToInsert = 0;
		for (int i = 0; i < getKeyCount() - half - 1; i++){
			memcpy(&keyToInsert, buffer+indexToSplit+8+(i*size), sizeof(int));
			memcpy(&pidToInsert, buffer+indexToSplit+8+(i*size) + sizeof(int), sizeof(PageId));
			sibling.insert(keyToInsert, pidToInsert);
		}

		memcpy(&midKey,buffer+indexToSplit,sizeof(int));
		memcpy(sibling.buffer,buffer+indexToSplit+4,sizeof(PageId));

		std::fill(buffer+indexToSplit,buffer+pageSize,0);
		numKeys=half;
		sibling.insert(key,pid);
	}

	else {
		//memcpy(sibling.buffer+8,buffer+indexToSplit,pageSize-indexToSplit);
		//sibling.numKeys=getKeyCount()-half;
		
		int keyToInsert = 0;
		PageId pidToInsert = 0;
		for (int i = 0; i < getKeyCount() - half; i++){
			memcpy(&keyToInsert, buffer + indexToSplit + (i*size), sizeof(int));
			memcpy(&pidToInsert, buffer + indexToSplit + (i*size) + sizeof(int), sizeof(PageId));
			sibling.insert(keyToInsert, pidToInsert);
		}

		std::fill(buffer+indexToSplit,buffer+pageSize,0);
		numKeys=half;
		midKey=key;
		memcpy(sibling.buffer,&pid,sizeof(PageId));
	}

	return 0;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid){
	int size = sizeof(PageId) + sizeof(int);

	char* pointer = buffer + 8;

	//What we will be doing is looping until we find the 
	//first child noe pointer to follow and output 
	//it in the pid

	int total = getKeyCount()*size+8;
	int count = 8;
	int curr_key;

	while(count < total) {
		memcpy(&curr_key,pointer,sizeof(int));
		if(curr_key > searchKey && count==8) {
			memcpy(&pid,buffer,sizeof(PageId));
			return 0;
		}
		else if(curr_key > searchKey) {
			memcpy(&pid,pointer - 4,sizeof(PageId));
			return 0;
		}
		count = count + 8;
	}
	//If that doesn't work, then we know that the search key 
	//is larger than the values of curr_key

	memcpy(&pid,pointer-4,sizeof(PageId));
	return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2){
	//What we want to do is insert the root node
	//Inserting the first pair into the B+Tree

	char* pointer = buffer;
	memcpy(pointer,&pid1,sizeof(PageId));

	RC retValue = insert(key,pid2);

	//cout << "The retValue of insertion is : " << retValue << endl;

	if(retValue!=0) {return retValue;}

	return 0;
}