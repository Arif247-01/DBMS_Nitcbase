#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>

OpenRelTable::OpenRelTable() {

  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
  }

  /************ Setting up Relation Cache entries ************/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Relation Cache Table****/
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  /**** setting up Attribute Catalog relation in the Relation Cache Table ****/

  // set up the relation cache entry for the attribute catalog similarly
  // from the record at RELCAT_SLOTNUM_FOR_ATTRCAT

  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
  
  struct RelCacheEntry relCacheEntry2;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry2.relCatEntry);
  relCacheEntry2.recId.block = RELCAT_BLOCK;
  relCacheEntry2.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

  // set the value at RelCacheTable::relCache[ATTRCAT_RELID]
  
  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry2;
  
  // 3rd one
  relCatBlock.getRecord(relCatRecord, 2);
  
  struct RelCacheEntry relCacheEntry3;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry3.relCatEntry);
  relCacheEntry3.recId.block = RELCAT_BLOCK;
  relCacheEntry3.recId.slot = 2;
  
  RelCacheTable::relCache[2] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[2]) = relCacheEntry3;






  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);

  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  // iterate through all the attributes of the relation catalog and create a linked
  // list of AttrCacheEntry (slots 0 to 5)
  // for each of the entries, set
  //    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
  //    attrCacheEntry.recId.slot = i   (0 to 5)
  //    and attrCacheEntry.next appropriately
  // NOTE: allocate each entry dynamically using malloc

  AttrCacheEntry * head;
  AttrCacheEntry *x = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    attrCatBlock.getRecord(attrCatRecord, 0);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &x->attrCatEntry);
    x->recId.block = ATTRCAT_BLOCK;
    x->recId.slot = 0;
  head = x;
  for(int i=1; i<6; i++){
    AttrCacheEntry * y = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    attrCatBlock.getRecord(attrCatRecord, i);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &y->attrCatEntry);
    y->recId.block = ATTRCAT_BLOCK;
    y->recId.slot = i;
    x->next = y;
    x = x->next;
  }
  x->next = nullptr;

  // set the next field in the last entry to nullptr
  
  AttrCacheTable::attrCache[RELCAT_RELID] = head/* head of the linked list */;

  /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/
  AttrCacheEntry * head2;
  AttrCacheEntry *z = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    attrCatBlock.getRecord(attrCatRecord, 6);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &z->attrCatEntry);
    z->recId.block = ATTRCAT_BLOCK;
    z->recId.slot = 6;
  head2 = z;
  for(int i=7; i<12; i++){
    AttrCacheEntry * y = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    attrCatBlock.getRecord(attrCatRecord, i);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &y->attrCatEntry);
    y->recId.block = ATTRCAT_BLOCK;
    y->recId.slot = i;
    z->next = y;
    z = z->next;
  }
  z->next = nullptr;

  // set up the attributes of the attribute cache similarly.
  // read slots 6-11 from attrCatBlock and initialise recId appropriately

  // set the value at AttrCacheTable::attrCache[ATTRCAT_RELID]
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head2;

/*OpenRelTable::~OpenRelTable() {
  for (int i = 0; i < MAX_OPEN; ++i) {
    delete RelCacheTable::relCache[i];
    delete AttrCacheTable::attrCache[i];
  }
  // free all the memory that you allocated in the constructor
}*/

//3rd one

AttrCacheEntry * head3;
  AttrCacheEntry *z2 = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    attrCatBlock.getRecord(attrCatRecord, 12);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &z2->attrCatEntry);
    z2->recId.block = ATTRCAT_BLOCK;
    z2->recId.slot = 12;
  head3 = z2;
  for(int i=12; i<16; i++){
    AttrCacheEntry * y = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    attrCatBlock.getRecord(attrCatRecord, i);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &y->attrCatEntry);
    y->recId.block = ATTRCAT_BLOCK;
    y->recId.slot = i;
    z2->next = y;
    z2 = z2->next;
  }
  z2->next = nullptr;
  
  AttrCacheTable::attrCache[2] = head3;
  
  }



OpenRelTable::~OpenRelTable() {
  for (int i = 0; i < MAX_OPEN; ++i) {
    free(RelCacheTable::relCache[i]);  // Use free instead of delete
    
    // Free memory for the linked lists in the attribute cache
    AttrCacheEntry* current = AttrCacheTable::attrCache[i];
    while (current) {
      AttrCacheEntry* next = current->next;
      free(current);  // Use free instead of delete
      current = next;
    }
  }
}



/* This function will open a relation having name `relName`.
Since we are currently only working with the relation and attribute catalog, we
will just hardcode it. In subsequent stages, we will loop through all the relations
and open the appropriate one.
*/
int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  // if relname is RELCAT_RELNAME, return RELCAT_RELID
  if(strcmp(relName,RELCAT_RELNAME)==0) return RELCAT_RELID;
  // if relname is ATTRCAT_RELNAME, return ATTRCAT_RELID
  if(strcmp(relName,ATTRCAT_RELNAME)==0) return ATTRCAT_RELID;
  
  if(strcmp(relName,"Students")==0) return 2;

  return E_RELNOTOPEN;
}
