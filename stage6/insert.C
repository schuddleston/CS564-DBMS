#include "catalog.h"
#include "query.h"

/**
 * Inserts a record into the specified relation.
 * 
 * @param relation Table to store tuple/record in
 * @param attrCnt Number of attributes
 * @param attrList Given attributes to insert
 * @return Status OK if no errors occurred, otherwise the first error that occurred
 */
const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// Borrows code from joinHTC.C and load.C

Status status; // Status to be returned
AttrDesc* attrSchema; // Attributes of relation
Record record; // Record to be inserted
RID rid; // ID of record to be inserted
int tmpAttrCnt = 0; // Gets attribute count
int width = 0; // Total length of tuple

// Gets info about the relation (attribute schema, count)
status = attrCat->getRelInfo(relation, tmpAttrCnt, attrSchema);
if (status != OK) {return status;}

// Determine total length of tuple
for (int i = 0; i < attrCnt; i++) {
	width += attrSchema[i].attrLen;
}

// Creates instance of InsertFileScan to later insert record
InsertFileScan* iFile = new InsertFileScan(relation, status);
if (!iFile) {return INSUFMEM;}
if (status != OK) {return status;}

char *recData; // Holds values in tuple to be added
if (!(recData = new char[width])) {return INSUFMEM;}

int iVal; // Used for integer attributes
float fVal; // Used for float attributes
char *sVal; // Holds value of any attribute

// Iterates through relation attributes
for (int i = 0; i < attrCnt; i++) {
	// Iterates through record's attributes
	for (int j = 0; j < attrCnt; j++) {
		// If any attribute contains a NULL value, return error
		if (attrList[j].attrValue == NULL) {return ATTRNOTFOUND;}

		// If relation & record attribute are aligned
		if (strcmp(attrSchema[i].attrName, attrList[j].attrName) == 0) {

			// Gets value of this attribute
			sVal = (char *)attrList[j].attrValue;

			switch (attrList[j].attrType) {
				// If attribute type is integer
				case INTEGER:	
					iVal = atoi(sVal); // converts value to integer
					memcpy(recData + attrSchema[i].attrOffset, 
							(void *)&iVal, attrSchema[i].attrLen);
					break;
				// If attribute type is a float
				case FLOAT:
					fVal = atof(sVal); // converts value to float
					memcpy(recData + attrSchema[i].attrOffset, 
							(void *)&fVal, attrSchema[i].attrLen);
					break;
				// If attribute type is a string
				case STRING:
					memcpy(recData + attrSchema[i].attrOffset, 
							(void *)sVal, attrSchema[i].attrLen);
					break;
				// If none of the above attribute types
				default:
					printf("illegal type in QU insert\n");
					break;
    		}
		}
	}
}

// Loads data into record
record.data = recData;
record.length = width;

// Inserts full record into relation
status = iFile->insertRecord(record, rid);

return status; // OK if insertion successful
}

