/**
 * Krishna Ramesh - 908 469 4752 - kramesh5@wisc.edu
 * Scott Huddleston - 908 229 1163 - schuddleston@wisc.edu
 * Manoj Arulmurugan - arulmurugan@wisc.edu
 * 
 * This file is the implementation of the BufMgr class that which contains the 4 methods that we implemented in order to allocate frames and buffers as required by our database.
 */

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/**
 * method used by allocPage and readPage to allocate a frame in the buffer. A free frame is found
 * using the clock algorithm and it clears the free frame, or returns with an error.
 * @param frame - referance to a frame integer that is used to return the frame number of the free frame back to the caller
 * @return Status OK if free frame found, BUFFEREXCEEDED if all pages are pinned, UNIXERR if writing a dirty page back to disk failed
 */
const Status BufMgr::allocBuf(int & frame) 
{
    int pinnedFrames[numBufs] = {0};
    int numbPinned = 0;
    unsigned int startHand = clockHand;
    while(numbPinned != numBufs) {
        advanceClock();
        
        // check if frame has already been checked and it is pinned => skip frame
        if(pinnedFrames[clockHand] == 1) continue;

        BufDesc *bufFrame = &bufTable[clockHand];
        if(bufFrame->valid) { // valid is set
            if(bufFrame->refbit) { // refBit is set
                bufFrame->refbit = false; // clear refBit
                continue; // restart
            }
            else { // refBit is not set
                if(bufFrame->pinCnt != 0) { // page is pinned
                    pinnedFrames[clockHand] = 1;
                    numbPinned++;
                    continue; // restart
                }
                else { // page is not pinned
                    if(bufFrame->dirty) { // dirty is set
                        if(bufFrame->file->writePage(bufFrame->pageNo, &bufPool[bufFrame->frameNo]) != OK) return UNIXERR;
                    }
                    hashTable->remove(bufFrame->file, bufFrame->pageNo); // remove page from hash table
                    bufFrame->Clear(); // clear frame
                    frame = bufFrame->frameNo; // set return val
                    return OK; // success
                }
            }
        }
        else { // valid is not set
            hashTable->remove(bufFrame->file, bufFrame->pageNo); // remove page from hash table
            bufFrame->Clear(); // clear frame
            frame = bufFrame->frameNo; // set return val
            return OK; // success
        }
    }
    return BUFFEREXCEEDED; // all buffer frames are pinned
}

/**
 * method used to read a page from bufferPool if it exists
 * @param file - a file pointer in which the page exists
 * @param PageNo - the page number corresponding to the page the caller wants to read
 * @param page - reference to a Page pointer that is used to return the pointer to the page the caller wants to read
 * @return Status OK if reading was successful, or intermediate status error from calling allocBuf(), readPage(), or insert()
 */
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, PageNo, frameNo); // Looks for page in hashtable

    if (status == OK) // If page is found in hashtable
    {
        BufDesc *frame; 
        frame = &bufTable[frameNo]; // Gets frame containing the page
        frame->refbit = true; // Sets frame's refbit
        frame->pinCnt += 1; // Increments frame's pin count
        page = &bufPool[frameNo]; // Returns page via pointer
    } 
    else // If page is NOT found in hashtable
    { 
        status = allocBuf(frameNo); // Allocates a frame for the page
        if (status != OK) {return status;} // Returns error msg if failed

        status = file->readPage(PageNo, &bufPool[frameNo]); // Reads page from disk into frame
        if (status != OK) {return status;} // Returns error msg if failed

        status = hashTable->insert(file, PageNo, frameNo); // Inserts page into hashtable
        if (status != OK) {return status;} // Returns error msg if failed

        BufDesc *frame;
        frame = &bufTable[frameNo]; // Gets frame now containing the page
        frame->Set(file, PageNo); // Invokes Set() on frame for proper setup
        page = &bufPool[frameNo]; // Returns page via pointer
    }

    return OK; // Success
}

/**
 * method used to unpin a certain page by decrementing the pinCnt integer var in the frame
 * @param file - a file pointer in which the page exists
 * @param PageNo - the page number corresponding to the page the caller wants to read
 * @param dirty - a boolean that tells the method to set the dirty bit in the frame to true or false
 * @return Status OK if unpinning was successful, PAGENOTPINNED if page was never pinned, or HASHNOTFOUND if page is not in bufferPool
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, 
                   const bool dirty) 
{
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, PageNo, frameNo); // Looks for page in hashtable
    
    if (status == OK) // If page is found in hashtable
    {
        BufDesc *frame = &bufTable[frameNo]; // Gets frame containing this page

        if (dirty) { // If 'dirty' param is true
            frame->dirty = true; // Set frame's dirty bit as true
        }

        if (frame->pinCnt > 0) { // If frame's pin count is not 0, decrement it
            frame->pinCnt -= 1;
        } else { // If pin count is 0, return error msg
            return PAGENOTPINNED;
        }
    } else { // If page NOT found in hashtable
        return HASHNOTFOUND; // Returns error msg
    }

    return OK; // Success
}

/**
 * method used to allocate a page by using allocatePage(), allocBuf(), insert(), and Set()
 * @param file - a file pointer in which the page exists
 * @param PageNo - referance to an integer pageNo that is used to return the page number of the page that is allocated
 *  * @param page - reference to a Page pointer that is used to return the pointer to the page that is allocated
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    if(file->allocatePage(pageNo) != OK) return UNIXERR; // error occurred in allocatePage

    int frame = 0;
    Status status = allocBuf(frame);
    if(status != OK) return status; // error occurred in allocBuf
    
    if(hashTable->insert(file, pageNo, frame) != OK) return HASHTBLERROR; // error occurred in insert
    
    BufDesc *bufFrame = &bufTable[frame];
    bufFrame->Set(file, pageNo);
    
    page = &bufPool[frame]; // set return val
    return OK; // success
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


