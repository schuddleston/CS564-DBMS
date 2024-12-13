#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
/**
 * Deletes records from a specified relation based on a predicate.
 * 
 * @param relation Table to delete record from
 * @param attrName Attribute name in predicate
 * @param op Operator in predicate
 * @param type Type of attribute in predicate
 * @param attrValue Actual value in predicate
 * @return Status OK if no errors occurred, otherwise the first error that occurred
 */
const Status QU_Delete(const string & relation, 
		       const string & attrName,
		       const Operator op,
		       const Datatype type,
		       const char *attrValue)
{
// part 6
	cout << "Doing QU_Delete " << endl;

	AttrDesc attrSchema; // Attributes of relation
	Status status = OK;
	HeapFileScan scanner(relation, status);
	if(status != OK) return status; // error from creating scanner object instance

	int offset, length;	

	// check if we need to remove all entries in relation
	if(attrName.length() == 0) {
		offset = 0;
		length = 0;
	}
	else {
		status = attrCat->getInfo(relation, attrName, attrSchema);
		if(status != OK) return status; // error from getInfo()

		offset = attrSchema.attrOffset;
		length = attrSchema.attrLen;
	}

	int new_int;
	float new_float;
	if(type == INTEGER) { // type is int
		new_int = atoi(attrValue);
		attrValue = (const char *)&new_int;
	}
	else if(type == FLOAT) { // type is float
		new_float = atof(attrValue);
		attrValue = (const char *)&new_float;
	}

	// start scanning for appropriate entires in the table that match parameters
	status = scanner.startScan(offset, length, type, attrValue, op);
	if(status != OK) return status; // error from calling startScan()

	RID rid;
	while(scanner.scanNext(rid) == OK) {
		status = scanner.deleteRecord(); // delete the appropriate record
		if(status != OK) return status; // error from calling deleteRecord()
	}

	return OK; // success
}


