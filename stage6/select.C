#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

/*Status fetchRelationMetadata(const string &relName, AttrDesc *&attrDesc, int &attrCnt) {
    return attrCat->getRelInfo(relName, attrCnt, attrDesc);
}*/

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

	Status status;
    std::vector<AttrDesc> attrDescList(projCnt);
    std::unique_ptr<AttrDesc> filterAttrDesc;
    int recordLength = 0;

	//Checking for valid record or not?
	/*if ( (attr != NULL)
        && (attr->attrType <0 || attr->attrType>2) )
        return BADCATPARM;*/
	/*
	// Projection descriptors
    for (int i = 0; i < projCnt; ++i) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrDescArray[i]);
        if (status != OK) {
            delete[] attrDescArray; // Avoid memory leak
            return status;
        }
        reclen += attrDescArray[i].attrLen;
    }*/

	 //Projection attribute descriptors:
	for (int i = 0; i < projCnt; ++i) 
	{
        AttrDesc attrDesc;
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrDesc);
        if (status != OK) { return status;}
        attrDescList[i] = attrDesc;
        recordLength += attrDesc.attrLen;
    }

	// Retrieving metadata:
	if (attr != nullptr) 
	{
        filterAttrDesc = std::make_unique<AttrDesc>();
        status = attrCat->getInfo(attr->relName, attr->attrName, *filterAttrDesc);
        if (status != OK) {return status;}
    }

	return ScanSelect(result, projCnt, attrDescList.data(), 
					filterAttrDesc ? filterAttrDesc.get() : nullptr, op, 
					attrValue, 
					recordLength);

}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;


	Status status;
    Record outputRecord, inputRecord;
    RID rid;

	InsertFileScan resultRelation(result, status);
    if (status != OK) {return status;}

	outputRecord.length = reclen;
    outputRecord.data = new char[reclen];

	// Input scan:
	HeapFileScan inputScan(projNames[0].relName, status);
    if (status != OK) {return status;}

	/*
	if (attrDesc == nullptr) {
        status = scan.startScan(0, 0, STRING, nullptr, EQ);
    } else if (attrDesc->attrType == STRING) {
        status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, op);
    } else {
        cout << "Only string supported" << endl;
        //return "Some Error Type??";
    }*/

	if (attrDesc == nullptr) 
	{
        status = inputScan.startScan(0, 0, STRING, nullptr, EQ);
    } else {
        switch (attrDesc->attrType) {
            case STRING:
			{
                status = inputScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, op);
                break;
			}
            case FLOAT: 
			{
                float filterValue = atof(filter);
                status = inputScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, reinterpret_cast<char *>(&filterValue), op);
                break;
            }
            case INTEGER: 
			{
                int filterValue = atoi(filter);
                status = inputScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, reinterpret_cast<char *>(&filterValue), op);
                break;
            }
            default:
			{
                //return "Some Error Type??";
				cout<<"Error: Invalid";
			}
        }
    }

	if (status != OK) {return status;}

    // Scan and projection:
    while (inputScan.scanNext(rid) == OK) 
	{
        status = inputScan.getRecord(inputRecord);
        if (status != OK) {return status;}

        // Output record:
        int outputOffset = 0;
        for (int i = 0; i < projCnt; ++i) 
		{
            const AttrDesc &projAttr = projNames[i];
            memcpy(static_cast<char *>(outputRecord.data) + outputOffset, 
                   static_cast<const char *>(inputRecord.data) + projAttr.attrOffset, 
                   projAttr.attrLen);
            outputOffset += projAttr.attrLen;
        }

        // Insert the output record into the result relation
        RID resultRID;
        status = resultRelation.insertRecord(outputRecord, resultRID);
        if (status != OK) 
		{
            delete[] static_cast<char *>(outputRecord.data);
            return status;
        }
    }

	if (status != FILEEOF) {
        delete[] static_cast<char *>(outputRecord.data);
        return status;
    }

	status = inputScan.endScan();
    delete[] static_cast<char *>(outputRecord.data);

	//return OK;
	return status;
}
