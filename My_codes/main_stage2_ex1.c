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
  

  for (int i =0; i<relCatHeader.numEntries; i++) {
    int z = 5;

    Attribute relCatRecord[RELCAT_NO_ATTRS]; // will store the record from the relation catalog

    relCatBuffer.getRecord(relCatRecord, i);

    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);
    
    while(1){
      
      if(z==-1) break;
      RecBuffer n_attrCatBuffer(z);
      HeadInfo n_attrCatHeader;
      n_attrCatBuffer.getHeader(&n_attrCatHeader);

     for (int j=0; j<n_attrCatHeader.numEntries; j++) {
    
       Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      
       n_attrCatBuffer.getRecord(attrCatRecord, j);

       // declare attrCatRecord and load the attribute catalog entry into it

       if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relCatRecord[RELCAT_REL_NAME_INDEX].sVal)==0) {
         //const char *attrName = attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].nVal == NUMBER ? "NUM" : "STR";
         const char *attrName = attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal;
         const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
         printf("  %s: %s\n", attrName, attrType);
       }
     }
      z = n_attrCatHeader.rblock;
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