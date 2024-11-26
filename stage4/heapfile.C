/**
 * Krishna Ramesh - 908 469 4752 - kramesh5@wisc.edu
 * Scott Huddleston - 908 229 1163 - schuddleston@wisc.edu
 * Manoj Arulmurugan - <student ID here> - arulmurugan@wisc.edu
 * 
 * *** TO DO ***
 */

#include "heapfile.h"
#include "error.h"

/**
 * Creates an (almost) empty heap file, with a header page and first page.
 * @param fileName the name of the file being created
 * @return Status OK if creation successful, else other statuses if failed in a specific step.
 */
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // Tries to open the file. This should return an error...
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // Creates this new file, opens it, and allocates 
        // an empty header page in the buffer pool for it.
	    status = db.createFile(fileName); 
        if (status != OK) {return status;} // Returns error msg if failed
        status = db.openFile(fileName, file);
        if (status != OK) {return status;} // Returns error msg if failed
        status = bufMgr->allocPage(file, hdrPageNo, newPage); 
        if (status != OK) {db.closeFile(file); return status;} // Returns error msg & closes file if failed
		
        // Cast new page pointer to FileHdrPage.
	    hdrPage = (FileHdrPage*)newPage;

        // Converts 'fileName' string parameter into a char
        // pointer array, then initializes the header page's
        // 'fileName' variable as such.
        strcpy(hdrPage->fileName, fileName.c_str());

        // Retrieves pointer to the first page of the file,
        // and initializes this new page's contents.
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) {db.closeFile(file); return status;} // Returns error msg & closes file if failed
        newPage->init(newPageNo);

        // Assign the header page's values accordingly.
        hdrPage->pageCnt = 1;
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->recCnt = 0;
        
        // Unpins and marks dirty both the header page & its first page.
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK) {db.closeFile(file); return status;} // Returns error msg & closes file if failed
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK) {db.closeFile(file); return status;} // Returns error msg & closes file if failed
        
        status = db.closeFile(file);

        return status;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
/**
 * TO DO
 */
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        int tempPageNo;

        // Gets the page number of the header page of the file
        status = filePtr->getFirstPage(tempPageNo);
        if (status != OK) {returnStatus = status; return;} // Returns error msg if failed.

        // Reads & pins the header page of the file in the buffer pool
        status = bufMgr->readPage(filePtr, tempPageNo, pagePtr);
        if (status != OK) {returnStatus = status; return;} // Returns error msg if failed

        // Initializes data members associated with header page
        headerPage = (FileHdrPage*)pagePtr;
        headerPageNo = tempPageNo;
        hdrDirtyFlag = false;

        // Reads & pins the first data page of the file in the buffer pool
        status = bufMgr->readPage(filePtr, headerPage->firstPage, pagePtr);
        if (status != OK) {returnStatus = status; return;} // Returns error msg if failed

        // Initializes data members associated with the first data page
        curPage = pagePtr;
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;
        curRec = NULLRID;

        returnStatus = status;
        return;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    if (curPage == NULL)
    {
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK){return status;}

        //"Book-keeping: Set the fields curPage, curPageNo, curDirtyFlag, and curRec of the HeapFile object appropriately:"
        status = curPage->getRecord(rid, rec);
        if (status != OK){return status;}
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        curRec = rid;

        return status;
    }
    else
    {
        if (rid.pageNo != curPageNo)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK){return status;}

            status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
            if (status != OK){return status;}

            curPageNo = rid.pageNo;
            curRec = rid;
            curDirtyFlag = false;
            status = curPage->getRecord(rid, rec);
            if (status != OK){return status;}
            else{ return OK;}
        }
        
            status = curPage->getRecord(rid, rec);
            if (status != OK){ return status; }
            curRec = rid;
            return OK;
    }
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    
    nextPageNo = curPageNo; //Getting the firstPage
    
    while(true){
        //Get the next page
        if (nextPageNo == -1) {return FILEEOF;}

        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag); // if there is a nextPage, unpin the current page
        if (status != OK){ return status; }

        //Read in the new page and reset variables associated with it
        status = bufMgr->readPage(filePtr, nextPageNo, curPage); 
        if (status != OK) return status;
        
        curPageNo = nextPageNo;
        curDirtyFlag = false;

        //Iterate over all records
        if (curRec.pageNo == NULLRID.pageNo && curRec.slotNo == NULLRID.slotNo) {
            //Getting the first record
            status = curPage->firstRecord(nextRid);
        } 
        else {status = OK;}

        if (status != OK && status != NORECORDS) {
            //Records don't exist
            return status;
        } 
        else if(status == OK){ 
            //Records do exist
            if (curRec.pageNo != NULLRID.pageNo && curRec.slotNo != NULLRID.slotNo) {
                status = curPage->nextRecord(curRec, nextRid);
                tmpRid = nextRid;
            }

            if (status != OK && status != ENDOFPAGE) {
                return status;
            } 

            else if (status == OK) { 
                //If it is the final record, skip it
                while (true) {
                    //Try to match record
                    status = curPage->getRecord(nextRid, rec);
                    if (status != OK){ return status; }
                    curRec = nextRid;
                    if (matchRec(rec)){
                        outRid = nextRid;
                        return OK;
                    }
                    tmpRid = nextRid;

                    //Continue getting next record (No match)
                    status = curPage->nextRecord(tmpRid, nextRid);
                    if (status == ENDOFPAGE) {
                        curRec = NULLRID;
                        break; //Finished scanning, but no records
                    }
                }
            } 
            else {
                curRec = NULLRID;
            }
        }
        status = curPage->getNextPage(nextPageNo);
        if (status != OK){ return status; }
    }
    return OK;
	
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*    newPage;
    int        newPageNo;
    Status    status, unpinstatus;
    RID        rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if(curPage == NULL) {
        bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        curPageNo = headerPage->lastPage;
    }

    // try to insert record into curPage
    status = curPage->insertRecord(rec, outRid);
    if(status == OK) { // inserting into curPage was successful
        // bookkeeping
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
    }
    else { // could not insert into curPage
        status = bufMgr->allocPage(filePtr, newPageNo, newPage); // create a new page
        if(status != OK) return status;
        newPage->init(newPageNo); // initialize the new page

        // modify header page contents
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag); // unpin curPage

        // link the new page
        curPage->setNextPage(newPageNo);
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = true;

        // try to insert record into curPage
        status = curPage->insertRecord(rec, outRid);
        if(status == OK) {
            // bookkeeping
            headerPage->recCnt++;
            hdrDirtyFlag = true;
            curDirtyFlag = true;
        }
    }
    return status;
}