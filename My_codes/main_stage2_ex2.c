#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include<iostream>
#include<cstring>
#include<string.h>


int main(int argc, char *argv[]) {
  /* Initialize the Run Copy of Disk */
  Disk disk_run;
  
  // create objects for the relation catalog and attribute catalog
  RecBuffer relCatBuffer(RELCAT_BLOCK);  //4
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);  //5

  HeadInfo relCatHeader;
  HeadInfo attrCatHeader;

  // load the headers of both the blocks into relCatHeader and attrCatHeader.
  // (we will implement these functions later)
  relCatBuffer.getHeader(&relCatHeader);
  attrCatBuffer.getHeader(&attrCatHeader);
  
  unsigned char buffer[BLOCK_SIZE];
  Disk :: readBlock(buffer, 5);

  for (int i =2; i<relCatHeader.numEntries; i++) {

    Attribute relCatRecord[RELCAT_NO_ATTRS]; // will store the record from the relation catalog

    relCatBuffer.getRecord(relCatRecord, i);

    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

    for (int j=0; j<attrCatHeader.numEntries; j++) {
    
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      attrCatBuffer.getRecord(attrCatRecord, j);
      // declare attrCatRecord and load the attribute catalog entry into it
      
      if(strcmp(relCatRecord[0].sVal,"Students")==0){
        if(strcmp(attrCatRecord[1].sVal,"Class")==0){
          strcpy(attrCatRecord[1].sVal,"Batch");
          int k = 68 + j*96; //32 + 20 + j*96 + 16;
          //unsigned char bass[6];
          //memcpy(bass, buffer+k, 6);
          //printf("%s\n",bass);
          //strcpy(bass,"class");
          memcpy(buffer+k,"Batch",6);
          Disk::writeBlock(buffer, 5);
        }
      }

      if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relCatRecord[RELCAT_REL_NAME_INDEX].sVal)==0) {
        //const char *attrName = attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].nVal == NUMBER ? "NUM" : "STR";
        const char *attrName = attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal;
        const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
        printf("  %s: %s\n", attrName, attrType);
      }
    }
    printf("\n");
  }
  
  /*unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, 7000);
  char message[] = "puppy";
  memcpy(buffer + 20, message, 6);
  Disk::writeBlock(buffer, 7000);

  unsigned char buffer2[BLOCK_SIZE];
  char message2[6];
  Disk::readBlock(buffer2, 7000);
  memcpy(message2, buffer2 + 20, 6);
  std::cout << message2;*/
  return 0;
  // StaticBuffer buffer;
  // OpenRelTable cache;

  //return FrontendInterface::handleFrontend(argc, argv); (commented out during stage 1)
}