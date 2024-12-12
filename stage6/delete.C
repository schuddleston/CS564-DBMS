#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6

	Status status = OK;
	HeapFileScan scanner(relation, status);
	if(status != OK) return status; // error from creating scanner object instance

	// start scanning for appropriate entires in the table that match parameters
	status = scanner.startScan(0, attrName.length(), type, attrValue, op);
	if(status != OK) return status; // error from calling startScan()

	RID rid;
	while(scanner.scanNext(rid) == OK) {
		status = scanner.deleteRecord(); // delete the appropriate record
		if(status != OK) return status; // error from calling deleteRecord()
	}

	return OK; // success
}


