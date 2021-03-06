/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"

#include <iostream>       // std::cout
#include <queue>          // std::queue

#include <stdlib.h>
#include <cstring>


using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
    treeHeight = 0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode){
	RC val = pf.open(indexname,mode); //opens a file in read or write mode

	if(val!=0)  { //Check to see if the file did not open successfully
		rootPid = RC_INVALID_PID;
		return val;
	}

	//if the file openned successfully, we set the rootPid to zero

	rootPid = 0; 
 	
 	//Write an empty root node (which is a leaf node)
	if(pf.endPid() <= 0) { 
		BTLeafNode leaf;
		int t = leaf.write(rootPid,pf);
	}

	return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close(){
	rootPid = RC_INVALID_PID;
	return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	// if nothing has been added
	if (treeHeight == 0)
	{
		RC err;
		BTLeafNode firstAdd;

		err = firstAdd.insert(key, rid);

		if (err != 0){
			return err;
		}

		rootPid = pf.endPid();

		treeHeight++;
		return firstAdd.write(rootPid, pf);
	}

	int keyLocator = -100;
	PageId pageLocator = -100;

    return insertHelper(key, rid, 1, rootPid, keyLocator, pageLocator);
}
RC BTreeIndex::insertHelper(int key, const RecordId& rid, int level, PageId currPage, int& keyLocator, PageId &pageLocator){
	if (level == treeHeight)
	{
		BTLeafNode leafToInsert;
		RC err;
		err = leafToInsert.read(currPage, pf);
		if (err != 0) return err;

		//no split necessary
		err = leafToInsert.insert(key, rid);
		if (err == 0){
			leafToInsert.write(currPage, pf);
			return 0;
		}

		//split necessary
		BTLeafNode neighbor;
		int nextKey;
		err = leafToInsert.insertAndSplit(key, rid, neighbor, nextKey);
		if (err != 0) return err;

		PageId pidPointer = pf.endPid();

		err = leafToInsert.setNextNodePtr(pidPointer);
		if (err != 0) return err;

		keyLocator = nextKey;
		pageLocator = pidPointer;

		err = leafToInsert.write(currPage, pf);
		if (err != 0) return err;

		err = neighbor.write(pidPointer, pf);
		if (err != 0) return err;

		if (level == 1){
			BTNonLeafNode root;

			int err;
			cout << currPage << endl;
			cout << pidPointer << endl;
			err = root.initializeRoot(currPage, nextKey, pidPointer);
			if (err != 0)return err;

			rootPid = pf.endPid();
			err = root.write(rootPid, pf);
			if (err != 0)return err;

			treeHeight++;

		}
		return 0;
	}
	else{

		BTNonLeafNode nodeToSearch;
		RC err;
		err = nodeToSearch.read(currPage, pf);
		if (err != 0){
			return err;
		}

		PageId childId;
		err = nodeToSearch.locateChildPtr(key, childId);
		if (err != 0)
			return err;

		err = insertHelper(key, rid, level + 1, childId, keyLocator, pageLocator);
		// need to insert into the non leaf
		if (err == 0 && (keyLocator != -100 && pageLocator != -100)){
			err = nodeToSearch.insert(keyLocator, pageLocator);
			if (err == 0){
				nodeToSearch.write(currPage, pf);
				return 0;
			}

			BTNonLeafNode second;
			int midKey;

			err = nodeToSearch.insertAndSplit(key, currPage, second, midKey);
			if (err != 0)return err;

			int pidPointer = pf.endPid();
			err = nodeToSearch.write(currPage, pf);
			if (err != 0)return err;

			err = second.write(pidPointer, pf);
			if (err != 0) return err;

			if (level == 1){

				BTNonLeafNode root;
				err = root.initializeRoot(currPage, midKey, pidPointer);

				if (err != 0)return err;

				rootPid = pf.endPid();
				root.write(rootPid, pf);
				treeHeight++;

			}

		}

	}
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	RC err;
	if (treeHeight == 0){
		return RC_NO_SUCH_RECORD;
	}
	else{
		//follow root pid to the root and go from there -> recursive function
		err = locateHelper(searchKey, cursor, 1, rootPid);
	}
    return err;
}
RC BTreeIndex::locateHelper(int searchKey, IndexCursor &cursor, int level, PageId pid_looper)
{
	if (level == treeHeight){
		BTLeafNode searchNode;

		int err;
		err = searchNode.read(pid_looper, pf);
		if (err != 0) return err;

		int eid_found;
		err = searchNode.locate(searchKey, eid_found);

		if (err != 0) return err;

		cursor.pid = pid_looper;
		cursor.eid = eid_found;
		return 0;
	}
	else{
		BTNonLeafNode searchNode;
		int err;
		err = searchNode.read(pid_looper, pf);
		if (err != 0) return err;

		int new_pid;
		err = searchNode.locateChildPtr(searchKey, new_pid);
		if (err != 0) return err;

		return locateHelper(searchKey, cursor, level + 1, new_pid);
	}
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid){
	//First let's get the necessary details from the
	//cursor that's provided to us
	//Then we will load the leaf from the cursor

	PageId cursorPID = cursor.pid;

	if (cursorPID<0) {return RC_INVALID_CURSOR;}
	int cursorEID = cursor.eid;

	RC retVal;

	BTLeafNode leaf;
	retVal = leaf.read(cursorPID,pf);

	if(retVal!=0) {return retVal;}
	
	//What we need to do is to reconfirm that we are able
	//To read the entry 

	retVal = leaf.readEntry(cursorEID,key,rid);
	if(retVal!=0) {return retVal;}

	//Need to increment the cursorEID
	//Need to check that its not 
	//greater than the key count

	int max = leaf.getKeyCount();

	if(cursorEID+1>=max){
		cursorEID = 0;
		cursorPID = leaf.getNextNodePtr();
	}
	else {cursorEID++;}

	cursor.pid = cursorPID;
	cursor.eid = cursorEID;
	return 0;

}

void BTreeIndex::printTree()
{
	if (treeHeight <= 0) return;
	
	if (treeHeight == 1){
		BTLeafNode node;
		node.read(rootPid, pf);
		node.printLeaf();
		return;
	}
	else{

		BTNonLeafNode root;
		root.read(rootPid, pf);
		int level = 1;

		cout << "Level "<< level << endl;
		root.printNonLeafNode();

		queue<PageId>current;
		queue<PageId>children;
		PageId first;
		memcpy(&first, root.buffer, sizeof(PageId));
		children.push(first);

		for (int i = 0; i < root.getKeyCount(); i++){
			PageId add;
			memcpy(&add, root.buffer + 8 + (i*(sizeof(int) + sizeof(PageId))) + sizeof(int), sizeof(PageId));
			children.push(add);
		}

		current = queue<PageId>(children);
		children = queue<PageId>();

		level++;
		
		while(level <= treeHeight){
			cout << "Level " << level  << endl;
			if (level == treeHeight){
				while (!current.empty()){
					PageId pid = current.front();
					BTLeafNode n;
					n.read(pid, pf);
					n.printLeaf();
					current.pop();
				}
			}
			else{
				while(!current.empty())
				{
					PageId pid = current.front();
					BTNonLeafNode root; 
					root.read(pid, pf);
					root.printNonLeafNode();

					PageId first;
					memcpy(&first, root.buffer, sizeof(PageId));
					children.push(first);

					for (int i = 0; i < root.getKeyCount(); i++){
						PageId add;
						memcpy(&add, root.buffer + 8 + (i*(sizeof(int) + sizeof(PageId))) + sizeof(int), sizeof(PageId));
						children.push(add);
					}
					current.pop();
				}
			}
			level++;
			current = queue<PageId>(children);
			children = queue<PageId>();
		}
		
	}
}
