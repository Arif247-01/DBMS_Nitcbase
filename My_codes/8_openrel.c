#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>
#include<stdio.h>


OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {

  // initialise all values in relCache and attrCache to be nullptr and all entries
  // in tableMetaInfo to be free
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    OpenRelTable::tableMetaInfo[i].free = true;
  }

  // load the relation and attribute catalog into the relation cache (we did this already)
  // load the relation and attribute catalog into the attribute cache (we did this already)
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



  /************ Setting up tableMetaInfo entries ************/

  // in the tableMetaInfo array
  //   set free = false for RELCAT_RELID and ATTRCAT_RELID
  //   set relname for RELCAT_RELID and ATTRCAT_RELID-----what is this?
  OpenRelTable::tableMetaInfo[RELCAT_RELID].free = false;
  OpenRelTable::tableMetaInfo[ATTRCAT_RELID].free = false;
  strcpy(OpenRelTable::tableMetaInfo[RELCAT_RELID].relName,RELCAT_RELNAME);
  strcpy(OpenRelTable::tableMetaInfo[ATTRCAT_RELID].relName,ATTRCAT_RELNAME);
  
}


OpenRelTable::~OpenRelTable() {

  for(int i=2; i<MAX_OPEN; i++)
  {
    if(tableMetaInfo[i].free == false)
    {
      // close the relation using openRelTable::closeRel().
      closeRel(i);
    }
  }

  /**** Closing the catalog relations in the relation cache ****/

  //releasing the relation cache entry of the attribute catalog

  if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty == true/* RelCatEntry of the ATTRCAT_RELID-th RelCacheEntry has been modified */) {

      /* Get the Relation Catalog entry from RelCacheTable::relCache
      Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
      RelCatEntry relCatEntry;
      RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
      RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;
      Attribute rec[6];
      RelCacheTable::relCatEntryToRecord(&relCatEntry, rec);

      // declaring an object of RecBuffer class to write back to the buffer
      RecBuffer relCatBlock(recId.block);

      // Write back to the buffer using relCatBlock.setRecord() with recId.slot
      relCatBlock.setRecord(rec, recId.slot);
  }
  // free the memory dynamically allocated to this RelCacheEntry
  free(RelCacheTable::relCache[ATTRCAT_RELID]);


  //releasing the relation cache entry of the relation catalog

  if(RelCacheTable::relCache[RELCAT_RELID]->dirty == true/* RelCatEntry of the RELCAT_RELID-th RelCacheEntry has been modified */) {

      /* Get the Relation Catalog entry from RelCacheTable::relCache
      Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
      RelCatEntry relCatEntry;
      RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
      RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;
      Attribute rec[6];
      RelCacheTable::relCatEntryToRecord(&relCatEntry, rec);


      // declaring an object of RecBuffer class to write back to the buffer
      RecBuffer relCatBlock(recId.block);

      // Write back to the buffer using relCatBlock.setRecord() with recId.slot
      relCatBlock.setRecord(rec, recId.slot);
  }
  // free the memory dynamically allocated for this RelCacheEntry
  free(RelCacheTable::relCache[RELCAT_RELID]);


  // free the memory allocated for the attribute cache entries of the
  // relation catalog and the attribute catalog
  for (int i = 0; i < 2; ++i) {
    
    // Free memory for the linked lists in the attribute cache
    AttrCacheEntry* current = AttrCacheTable::attrCache[i];
    while (current) {
      AttrCacheEntry* next = current->next;
      free(current);  // Use free instead of delete
      current = next;
    }
  }
}


int OpenRelTable::getFreeOpenRelTableEntry() {

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/
    for(int i=0; i<MAX_OPEN; i++){
      if(OpenRelTable::tableMetaInfo[i].free==true) return i;
    }

  // if found return the relation id, else return E_CACHEFULL.
  return E_CACHEFULL;
}

int OpenRelTable::getRelId (char relName[ATTR_SIZE]) {

  /* traverse through the tableMetaInfo array,
    find the entry in the Open Relation Table corresponding to relName.*/
  for(int i=0; i<MAX_OPEN; i++)
  {
    if(OpenRelTable::tableMetaInfo[i].free==false && strcmp(relName,OpenRelTable::tableMetaInfo[i].relName)==0)
    {
      return i;
    }
  }

  // if found return the relation id, else indicate that the relation do not
  // have an entry in the Open Relation Table.
  //printf("%d\n",E_RELNOTOPEN);
  return E_RELNOTOPEN;
}


// int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
//   for (int i = 0; i < MAX_OPEN; i++) {
//         if (!strcmp(tableMetaInfo[i].relName, (char*)relName)) {
//             return i; // Return the relation id if an entry corresponding to relName is found
//         }
//     }
//     return E_RELNOTOPEN; // Return E_RELNOTOPEN if no such entry is found

// }

int OpenRelTable::openRel( char relName[ATTR_SIZE]) {

  if(OpenRelTable::getRelId(relName)!=E_RELNOTOPEN/* the relation `relName` already has an entry in the Open Relation Table */){
    // (checked using OpenRelTable::getRelId())

    // return that relation id;
    return OpenRelTable::getRelId(relName);
  }

  /* find a free slot in the Open Relation Table
     using OpenRelTable::getFreeOpenRelTableEntry(). */
  int f = OpenRelTable::getFreeOpenRelTableEntry();
  if (f==E_CACHEFULL/* free slot not available */){
    return E_CACHEFULL;
  }

  // let relId be used to store the free slot.
  int relId;
  relId = f;

  /****** Setting up Relation Cache entry for the relation ******/

  /* search for the entry with relation name, relName, in the Relation Catalog using
      BlockAccess::linearSearch().
      Care should be taken to reset the searchIndex of the relation RELCAT_RELID
      before calling linearSearch().*/
  RelCacheTable::resetSearchIndex(RELCAT_RELID);

  // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
  RecId relcatRecId;
  Attribute Val1;
  strcpy(Val1.sVal, relName);
  relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char *)"RelName", Val1, EQ);

  if (relcatRecId.block == -1 && relcatRecId.slot == -1) {
    // (the relation is not found in the Relation Catalog.)
    return E_RELNOTEXIST;
  }

  /* read the record entry corresponding to relcatRecId and create a relCacheEntry
      on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
      update the recId field of this Relation Cache entry to relcatRecId.
      use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
    NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
  */
  RecBuffer recBuffer(relcatRecId.block);
  Attribute rec[6];
  recBuffer.getRecord(rec, relcatRecId.slot);
  //RelCatEntry relCatEntry;
  //RelCacheTable::recordToRelCatEntry(rec, relCatEntry);
  //RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  //RelCacheTable::relCache[relId].recId = relcatRecId;
  //RelCacheTable::relCache[relId].relCatEntry = relCatEntry;
  
  
  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(rec, &relCacheEntry.relCatEntry);
  relCacheEntry.recId = relcatRecId;

  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[relId]) = relCacheEntry;
  

  /****** Setting up Attribute Cache entry for the relation ******/

  // let listHead be used to hold the head of the linked list of attrCache entries.
  AttrCacheEntry* listHead;

  /*iterate over all the entries in the Attribute Catalog corresponding to each
  attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
  care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
  corresponding to Attribute Catalog before the first call to linearSearch().*/
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  
  RecId attrcatRecId;
  attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char *)"RelName", Val1, EQ);
  
  AttrCacheEntry *x = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
  
  RecBuffer rec1Buffer(attrcatRecId.block);
  rec1Buffer.getRecord(rec, attrcatRecId.slot);
  AttrCacheTable::recordToAttrCatEntry(rec, &x->attrCatEntry);
  x->recId = attrcatRecId;
  listHead = x;
  
  
  
  while(1)
  {
      /* let attrcatRecId store a valid record id an entry of the relation, relName,
      in the Attribute Catalog.*/
      
      
      attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char *)"RelName", Val1, EQ);
      
      if(attrcatRecId.block==-1 && attrcatRecId.slot==-1) break;

      /* read the record entry corresponding to attrcatRecId and create an
      Attribute Cache entry on it using RecBuffer::getRecord() and
      AttrCacheTable::recordToAttrCatEntry().
      update the recId field of this Attribute Cache entry to attrcatRecId.
      add the Attribute Cache entry to the linked list of listHead .*/
      AttrCacheEntry * y = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
      RecBuffer rec2Buffer(attrcatRecId.block);
      rec2Buffer.getRecord(rec, attrcatRecId.slot);
      AttrCacheTable::recordToAttrCatEntry(rec, &y->attrCatEntry);
      y->recId = attrcatRecId;
      x->next = y;
      x = x->next;
      // allocate this on the heap because we want it to persist outside this function
      // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
  }
  x->next = nullptr;

  // set the relIdth entry of the AttrCacheTable to listHead.
  AttrCacheTable::attrCache[relId] = listHead;

  /****** Setting up metadata in the Open Relation Table for the relation******/

  // update the relIdth entry of the tableMetaInfo with free as false and
  // relName as the input.
  OpenRelTable::tableMetaInfo[relId].free = false;
  strcpy(OpenRelTable::tableMetaInfo[relId].relName,relName);

  return relId;
}


int OpenRelTable::closeRel(int relId) {
  // confirm that rel-id fits the following conditions
  //     2 <=relId < MAX_OPEN
  //     does not correspond to a free slot
  //  (you have done this already)
  
  if (relId==0 || relId==1/* rel-id corresponds to relation catalog or attribute catalog*/) {
    return E_NOTPERMITTED;
  }

  if (relId<0 || relId>=MAX_OPEN/* 0 <= relId < MAX_OPEN */) {
    return E_OUTOFBOUND;
  }

  if (OpenRelTable::tableMetaInfo[relId].free==true/* rel-id corresponds to a free slot*/) {
    return E_RELNOTOPEN;
  }

  /****** Releasing the Relation Cache entry of the relation ******/

  if (RelCacheTable::relCache[relId]->dirty == true/* RelCatEntry of the relId-th Relation Cache entry has been modified */)
  {

    /* Get the Relation Catalog entry from RelCacheTable::relCache
    Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
    RelCatEntry relCatEntry;
    Attribute rec[6];
    relCatEntry = RelCacheTable::relCache[relId]->relCatEntry;
    RelCacheTable::relCatEntryToRecord(&relCatEntry, rec);
    
    RecId recId = RelCacheTable::relCache[relId]->recId;


    // declaring an object of RecBuffer class to write back to the buffer
    RecBuffer relCatBlock(recId.block);

    // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    relCatBlock.setRecord(rec , recId.slot);
  }

  free(RelCacheTable::relCache[relId]);
  
  /****** Releasing the Attribute Cache entry of the relation ******/

  // free the memory allocated in the attribute caches which was
  // allocated in the OpenRelTable::openRel() function
  
  AttrCacheEntry* current = AttrCacheTable::attrCache[relId];
    while (current) {
      AttrCacheEntry* next = current->next;
      free(current);  // Use free instead of delete
      current = next;
    }

  // (because we are not modifying the attribute cache at this stage,
  // write-back is not required. We will do it in subsequent
  // stages when it becomes needed)


  /****** Set the Open Relation Table entry of the relation as free ******/
  OpenRelTable::tableMetaInfo[relId].free = true;
  //OpenRelTable::tableMetaInfo[relId].relName = nullptr;


  // update `metainfo` to set `relId` as a free slot

  return SUCCESS;
}

// int OpenRelTable::closeRel(int relId) {
//   if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
//     return E_NOTPERMITTED;
//   }

//   if (relId < 0 || relId >= MAX_OPEN) {
//     return E_OUTOFBOUND;
//   }

//   if (tableMetaInfo[relId].free == true) {
//     return E_RELNOTOPEN;
//   }

//   /* Releasing the Relation Cache Entry of the Relation */

//   if(RelCacheTable::relCache[relId]->dirty == true){
//     // if the relation cache entry is dirty, write it back to the relation catalog
//     Attribute relCatRecord[RELCAT_NO_ATTRS];
//     RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[relId]->relCatEntry, relCatRecord);
//     RecBuffer relCatBlock(RelCacheTable::relCache[relId]->recId.block);
//     relCatBlock.setRecord(relCatRecord, RelCacheTable::relCache[relId]->recId.slot);

//   }

//   /* Releasing the Attribute Cache Entry of the relation */

//   // free the memory allocated in the attribute caches which was allocated in openRel
//   AttrCacheEntry* current = AttrCacheTable::attrCache[relId];
//   while(current != NULL){
//     AttrCacheEntry* temp = current;
//     current = current->next;
//     free(temp);
//   }

//   // free the memory allocated in the relation and attribute caches which was
//   // allocated in the OpenRelTable::openRel() function

//   free(RelCacheTable::relCache[relId]);

//   // update `tableMetaInfo` to set `relId` as a free slot
//   tableMetaInfo[relId].free = true;
//   // update `relCache` and `attrCache` to set the entry at `relId` to nullptr
//   RelCacheTable::relCache[relId] = nullptr;
//   AttrCacheTable::attrCache[relId] = nullptr;
  
//   return SUCCESS;
// }