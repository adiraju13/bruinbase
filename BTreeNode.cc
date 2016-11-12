#include "BTreeNode.h"
#include <cstring>
#include <stdlib.h>

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

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ return 0; }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ return 0; }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount() {
	//Each pair has a certain size
	//Will be represented by the variable size
	//Want to see how many of these pairs can fit in buffer
	int size = sizeof(PageId) + sizeof(int);

	//Need to find the number of keys in this buffer

	int numOfKeys = 0;
	char* pointer = buffer + 8;

	int count = 8;
	int currKey;
	while (count < 1016) {
		memcpy(&currKey,pointer,sizeof(int)); 
		if(currKey==0){
			break; 
			//Means this is where it ends 
			//We will return numOfKeys
		}
		numOfKeys = numOfKeys+1;
		count = count + size;
	}

	return numOfKeys;
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

	if(getKeyCount()+1 > max) {return RC_NODE_FULL;}

	char* pointer = buffer+8;

	int count=8;
	int currKey;
	while (count<1016) {
		memcpy(&currKey,pointer,sizeof(int));
		if(currKey>key || currKey==0) {break;}
		count = count + size;
	}

	//After this loop
	//We have found the specific place to insert
	//Now we will be manipulating by copying data between the buffer and new buffer
	//The new buffer will contain the inserted value

	char* newBuffer[PageFile::PAGE_SIZE]; //Represents the new buffer

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
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ return 0; }
