#include "catalog.h"
#include "query.h"

// forward declaration
const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen);

const Status QU_Select(const string &result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue)
{
    cout << "Doing QU_Select " << endl;

    Status status = OK;
    AttrDesc projNamesScan[projCnt];

    int reclen = 0; // init variable that tells us the string length of all the attributes (size of record in bytes)
    for(int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projNamesScan[i]); // init projNamesScan[i]
        reclen += projNamesScan[i].attrLen;
        if(status != OK) return status; // error from calling getInfo()
    }

    AttrDesc attrSchema; // Attributes of relation

    if(attr == NULL) { // ptr is NULL
		// init attrSchema
        strcpy(attrSchema.relName, projNames[0].relName);
        attrSchema.attrOffset = 0;
        attrSchema.attrLen = 0;
    }
    else {
        status = attrCat->getInfo(attr->relName, attr->attrName, attrSchema); // init attrSchema
		if(status != OK) return status; // error from calling getInfo()
		int new_int;
		float new_float;
		int type = attr->attrType;
		if((Datatype)type == INTEGER) { // type is int
			new_int = atoi(attrValue);
			attrValue = (const char *)&new_int;
		}
		else if((Datatype)type == FLOAT) { // type is float
			new_float = atof(attrValue);
			attrValue = (const char *)&new_float;
		}
    }

    status = ScanSelect(result, projCnt, projNamesScan, &attrSchema, op, attrValue, reclen); // call ScanSelect() to perform mem copying
	if(status != OK) return status; // error from calling ScanSelect()
	return OK;
}

const Status ScanSelect(const string & result,
        const int projCnt,
        const AttrDesc projNames[],
        const AttrDesc *attrDesc,
        const Operator op,
        const char *filter,
        const int reclen)
{

	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
    Status status = OK;
	InsertFileScan scannedTable(result, status);
	if(status != OK) return status; // error from creating InsertFileScan object instance

	HeapFileScan scanner(attrDesc->relName, status);
	if(status != OK) return status; // error from creating HeapFileScan object instance

	status = scanner.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
	if(status != OK) return status; // error from calling startScan()	

	RID rid, scanned_rid;
	Record record, scanned_record;
	while(scanner.scanNext(rid) == OK) {
		status = scanner.getRecord(record); // get the appropriate record
		if(status != OK) return status; // error from calling getRecord()

		int idx = 0;
		char scanned_data[reclen]; // mem buffer to copy data
		char *record_data = (char *)record.data; // pointer to array that contains data we want

		// loop through every attribute and copy data to scanned record memory
		for(int i = 0; i < projCnt; i++) {
			memcpy(&scanned_data[idx], &record_data[projNames[i].attrOffset], projNames[i].attrLen); // copy from record to data buffer array
			idx += projNames[i].attrLen;
		}

		// set scanned_record struct
		scanned_record.data = scanned_data;
		scanned_record.length = reclen;

		status = scannedTable.insertRecord(scanned_record, scanned_rid); // insert record into result table
		if(status != OK) return status; // error from calling insertRecord()
	}
	return OK;
}