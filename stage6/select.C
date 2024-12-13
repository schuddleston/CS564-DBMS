#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"

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
    AttrDesc projNamesDesc[projCnt];

    int reclen = 0;
    for(int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projNamesDesc[i]);
        reclen += projNamesDesc[i].attrLen;
        if(status != OK) return status;
    }

    AttrDesc attrDesc;

    if(attr == NULL) {
        strcpy(attrDesc.relName, projNames[0].relName);
        // strcpy(attrDesc.attrName, projNames[0].attrName);
        attrDesc.attrOffset = 0;
        attrDesc.attrLen = 0;
    }
    else {
        status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
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

    status = ScanSelect(result, projCnt, projNamesDesc, &attrDesc, op, attrValue, reclen);
	if(status != OK) return status;
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
	if(status != OK) return status;

	HeapFileScan scanner(attrDesc->relName, status);
	if(status != OK) return status; // error from creating scanner object instance

	status = scanner.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, filter, op);
	if(status != OK) return status; // error from calling startScan()	

	RID rid, scanned_rid;
	Record record, scanned_record;
	while(scanner.scanNext(rid) == OK) {
		int total_offset = 0;
		char data[reclen];

		status = scanner.getRecord(record); // get the appropriate record
		if(status != OK) return status; // error from calling deleteRecord()
		for(int i = 0; i < projCnt; i++) {
			memcpy(data + total_offset, record.data + projNames[i].attrOffset, projNames[i].attrLen);
			total_offset += projNames[i].attrLen;
		}

		scanned_record.data = data;
		scanned_record.length = reclen;

		status = scannedTable.insertRecord(scanned_record, scanned_rid);
		if(status != OK) return status;
	}
	return OK;
}