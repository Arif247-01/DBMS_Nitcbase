// #include "OpenRelTable.h"
// #include <cstdlib>
// #include <cstring>
// #include<stdio.h>


// OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

// OpenRelTable::OpenRelTable() {

//   // initialise all values in relCache and attrCache to be nullptr and all entries
//   // in tableMetaInfo to be free
//   for (int i = 0; i < MAX_OPEN; ++i) {
//     RelCacheTable::relCache[i] = nullptr;
//     AttrCacheTable::attrCache[i] = nullptr;
//     OpenRelTable::tableMetaInfo[i].free = true;
//   }

//   // load the relation and attribute catalog into the relation cache (we did this already)
//   // load the relation and attribute catalog into the attribute cache (we did this already)
//   /************ Setting up Relation Cache entries ************/
//   // (we need to populate relation cache with entries for the relation catalog
//   //  and attribute catalog.)

//   /**** setting up Relation Catalog relation in the Relation Cache Table****/
//   RecBuffer relCatBlock(RELCAT_BLOCK);

//   Attribute relCatRecord[RELCAT_NO_ATTRS];
//   relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

//   struct RelCacheEntry relCacheEntry;
//   RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
//   relCacheEntry.recId.block = RELCAT_BLOCK;
//   relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

//   // allocate this on the heap because we want it to persist outside this function
//   RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
//   *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

//   /**** setting up Attribute Catalog relation in the Relation Cache Table ****/

//   // set up the relation cache entry for the attribute catalog similarly
//   // from the record at RELCAT_SLOTNUM_FOR_ATTRCAT

//   relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
  
//   struct RelCacheEntry relCacheEntry2;
//   RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry2.relCatEntry);
//   relCacheEntry2.recId.block = RELCAT_BLOCK;
//   relCacheEntry2.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

//   // set the value at RelCacheTable::relCache[ATTRCAT_RELID]
  
//   RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
//   *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry2;
  

//   /************ Setting up Attribute cache entries ************/
//   // (we need to populate attribute cache with entries for the relation catalog
//   //  and attribute catalog.)

//   /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
//   RecBuffer attrCatBlock(ATTRCAT_BLOCK);

//   Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

//   // iterate through all the attributes of the relation catalog and create a linked
//   // list of AttrCacheEntry (slots 0 to 5)
//   // for each of the entries, set
//   //    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
//   //    attrCacheEntry.recId.slot = i   (0 to 5)
//   //    and attrCacheEntry.next appropriately
//   // NOTE: allocate each entry dynamically using malloc

//   AttrCacheEntry * head;
//   AttrCacheEntry *x = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
//     attrCatBlock.getRecord(attrCatRecord, 0);
//     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &x->attrCatEntry);
//     x->recId.block = ATTRCAT_BLOCK;
//     x->recId.slot = 0;
//   head = x;
//   for(int i=1; i<6; i++){
//     AttrCacheEntry * y = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
//     attrCatBlock.getRecord(attrCatRecord, i);
//     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &y->attrCatEntry);
//     y->recId.block = ATTRCAT_BLOCK;
//     y->recId.slot = i;
//     x->next = y;
//     x = x->next;
//   }
//   x->next = nullptr;

//   // set the next field in the last entry to nullptr
  
//   AttrCacheTable::attrCache[RELCAT_RELID] = head/* head of the linked list */;

//   /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/
//   AttrCacheEntry * head2;
//   AttrCacheEntry *z = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
//     attrCatBlock.getRecord(attrCatRecord, 6);
//     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &z->attrCatEntry);
//     z->recId.block = ATTRCAT_BLOCK;
//     z->recId.slot = 6;
//   head2 = z;
//   for(int i=7; i<12; i++){
//     AttrCacheEntry * y = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
//     attrCatBlock.getRecord(attrCatRecord, i);
//     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &y->attrCatEntry);
//     y->recId.block = ATTRCAT_BLOCK;
//     y->recId.slot = i;
//     z->next = y;
//     z = z->next;
//   }
//   z->next = nullptr;

//   // set up the attributes of the attribute cache similarly.
//   // read slots 6-11 from attrCatBlock and initialise recId appropriately

//   // set the value at AttrCacheTable::attrCache[ATTRCAT_RELID]
//   AttrCacheTable::attrCache[ATTRCAT_RELID] = head2;



//   /************ Setting up tableMetaInfo entries ************/

//   // in the tableMetaInfo array
//   //   set free = false for RELCAT_RELID and ATTRCAT_RELID
//   //   set relname for RELCAT_RELID and ATTRCAT_RELID-----what is this?
//   OpenRelTable::tableMetaInfo[RELCAT_RELID].free = false;
//   OpenRelTable::tableMetaInfo[ATTRCAT_RELID].free = false;
//   strcpy(OpenRelTable::tableMetaInfo[RELCAT_RELID].relName,RELCAT_RELNAME);
//   strcpy(OpenRelTable::tableMetaInfo[ATTRCAT_RELID].relName,ATTRCAT_RELNAME);
  
// }


// OpenRelTable::~OpenRelTable() {

//   for(int i=2; i<MAX_OPEN; i++)
//   {
//     if(tableMetaInfo[i].free == false)
//     {
//       // close the relation using openRelTable::closeRel().
//       closeRel(i);
//     }
//   }

//   /**** Closing the catalog relations in the relation cache ****/

//   //releasing the relation cache entry of the attribute catalog

//   if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty == true/* RelCatEntry of the ATTRCAT_RELID-th RelCacheEntry has been modified */) {

//       /* Get the Relation Catalog entry from RelCacheTable::relCache
//       Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
//       RelCatEntry relCatEntry;
//       RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
//       RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;
//       Attribute rec[6];
//       RelCacheTable::relCatEntryToRecord(&relCatEntry, rec);

//       // declaring an object of RecBuffer class to write back to the buffer
//       RecBuffer relCatBlock(recId.block);

//       // Write back to the buffer using relCatBlock.setRecord() with recId.slot
//       relCatBlock.setRecord(rec, recId.slot);
//   }
//   // free the memory dynamically allocated to this RelCacheEntry
//   free(RelCacheTable::relCache[ATTRCAT_RELID]);


//   //releasing the relation cache entry of the relation catalog

//   if(RelCacheTable::relCache[RELCAT_RELID]->dirty == true/* RelCatEntry of the RELCAT_RELID-th RelCacheEntry has been modified */) {

//       /* Get the Relation Catalog entry from RelCacheTable::relCache
//       Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
//       RelCatEntry relCatEntry;
//       RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
//       RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;
//       Attribute rec[6];
//       RelCacheTable::relCatEntryToRecord(&relCatEntry, rec);


//       // declaring an object of RecBuffer class to write back to the buffer
//       RecBuffer relCatBlock(recId.block);

//       // Write back to the buffer using relCatBlock.setRecord() with recId.slot
//       relCatBlock.setRecord(rec, recId.slot);
//   }
//   // free the memory dynamically allocated for this RelCacheEntry
//   free(RelCacheTable::relCache[RELCAT_RELID]);


//   // free the memory allocated for the attribute cache entries of the
//   // relation catalog and the attribute catalog
//   for (int i = 0; i < 2; ++i) {
    
//     // Free memory for the linked lists in the attribute cache
//     AttrCacheEntry* current = AttrCacheTable::attrCache[i];
//     while (current) {
//       AttrCacheEntry* next = current->next;
//       free(current);  // Use free instead of delete
//       current = next;
//     }
//   }
// }


// int OpenRelTable::getFreeOpenRelTableEntry() {

//   /* traverse through the tableMetaInfo array,
//     find a free entry in the Open Relation Table.*/
//     for(int i=0; i<MAX_OPEN; i++){
//       if(OpenRelTable::tableMetaInfo[i].free==true) return i;
//     }

//   // if found return the relation id, else return E_CACHEFULL.
//   return E_CACHEFULL;
// }

// int OpenRelTable::getRelId (char relName[ATTR_SIZE]) {

//   /* traverse through the tableMetaInfo array,
//     find the entry in the Open Relation Table corresponding to relName.*/
//   for(int i=0; i<MAX_OPEN; i++)
//   {
//     if(OpenRelTable::tableMetaInfo[i].free==false && strcmp(relName,OpenRelTable::tableMetaInfo[i].relName)==0)
//     {
//       return i;
//     }
//   }

//   // if found return the relation id, else indicate that the relation do not
//   // have an entry in the Open Relation Table.
//   //printf("%d\n",E_RELNOTOPEN);
//   return E_RELNOTOPEN;
// }


// // int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
// //   for (int i = 0; i < MAX_OPEN; i++) {
// //         if (!strcmp(tableMetaInfo[i].relName, (char*)relName)) {
// //             return i; // Return the relation id if an entry corresponding to relName is found
// //         }
// //     }
// //     return E_RELNOTOPEN; // Return E_RELNOTOPEN if no such entry is found

// // }

// int OpenRelTable::openRel( char relName[ATTR_SIZE]) {

//   if(OpenRelTable::getRelId(relName)!=E_RELNOTOPEN/* the relation `relName` already has an entry in the Open Relation Table */){
//     // (checked using OpenRelTable::getRelId())

//     // return that relation id;
//     return OpenRelTable::getRelId(relName);
//   }

//   /* find a free slot in the Open Relation Table
//      using OpenRelTable::getFreeOpenRelTableEntry(). */
//   int f = OpenRelTable::getFreeOpenRelTableEntry();
//   if (f==E_CACHEFULL/* free slot not available */){
//     return E_CACHEFULL;
//   }

//   // let relId be used to store the free slot.
//   int relId;
//   relId = f;

//   /****** Setting up Relation Cache entry for the relation ******/

//   /* search for the entry with relation name, relName, in the Relation Catalog using
//       BlockAccess::linearSearch().
//       Care should be taken to reset the searchIndex of the relation RELCAT_RELID
//       before calling linearSearch().*/
//   RelCacheTable::resetSearchIndex(RELCAT_RELID);

//   // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
//   RecId relcatRecId;
//   Attribute Val1;
//   strcpy(Val1.sVal, relName);
//   relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char *)"RelName", Val1, EQ);

//   if (relcatRecId.block == -1 && relcatRecId.slot == -1) {
//     // (the relation is not found in the Relation Catalog.)
//     return E_RELNOTEXIST;
//   }

//   /* read the record entry corresponding to relcatRecId and create a relCacheEntry
//       on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
//       update the recId field of this Relation Cache entry to relcatRecId.
//       use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
//     NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
//   */
//   RecBuffer recBuffer(relcatRecId.block);
//   Attribute rec[6];
//   recBuffer.getRecord(rec, relcatRecId.slot);
//   //RelCatEntry relCatEntry;
//   //RelCacheTable::recordToRelCatEntry(rec, relCatEntry);
//   //RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
//   //RelCacheTable::relCache[relId].recId = relcatRecId;
//   //RelCacheTable::relCache[relId].relCatEntry = relCatEntry;
  
  
//   struct RelCacheEntry relCacheEntry;
//   RelCacheTable::recordToRelCatEntry(rec, &relCacheEntry.relCatEntry);
//   relCacheEntry.recId = relcatRecId;

//   // allocate this on the heap because we want it to persist outside this function
//   RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
//   *(RelCacheTable::relCache[relId]) = relCacheEntry;
  

//   /****** Setting up Attribute Cache entry for the relation ******/

//   // let listHead be used to hold the head of the linked list of attrCache entries.
//   AttrCacheEntry* listHead;

//   /*iterate over all the entries in the Attribute Catalog corresponding to each
//   attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
//   care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
//   corresponding to Attribute Catalog before the first call to linearSearch().*/
//   RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  
//   RecId attrcatRecId;
//   attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char *)"RelName", Val1, EQ);
  
//   AttrCacheEntry *x = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
  
//   RecBuffer rec1Buffer(attrcatRecId.block);
//   rec1Buffer.getRecord(rec, attrcatRecId.slot);
//   AttrCacheTable::recordToAttrCatEntry(rec, &x->attrCatEntry);
//   x->recId = attrcatRecId;
//   listHead = x;
  
  
  
//   while(1)
//   {
//       /* let attrcatRecId store a valid record id an entry of the relation, relName,
//       in the Attribute Catalog.*/
      
      
//       attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char *)"RelName", Val1, EQ);
      
//       if(attrcatRecId.block==-1 && attrcatRecId.slot==-1) break;

//       /* read the record entry corresponding to attrcatRecId and create an
//       Attribute Cache entry on it using RecBuffer::getRecord() and
//       AttrCacheTable::recordToAttrCatEntry().
//       update the recId field of this Attribute Cache entry to attrcatRecId.
//       add the Attribute Cache entry to the linked list of listHead .*/
//       AttrCacheEntry * y = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
//       RecBuffer rec2Buffer(attrcatRecId.block);
//       rec2Buffer.getRecord(rec, attrcatRecId.slot);
//       AttrCacheTable::recordToAttrCatEntry(rec, &y->attrCatEntry);
//       y->recId = attrcatRecId;
//       x->next = y;
//       x = x->next;
//       // allocate this on the heap because we want it to persist outside this function
//       // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
//   }
//   x->next = nullptr;

//   // set the relIdth entry of the AttrCacheTable to listHead.
//   AttrCacheTable::attrCache[relId] = listHead;

//   /****** Setting up metadata in the Open Relation Table for the relation******/

//   // update the relIdth entry of the tableMetaInfo with free as false and
//   // relName as the input.
//   OpenRelTable::tableMetaInfo[relId].free = false;
//   strcpy(OpenRelTable::tableMetaInfo[relId].relName,relName);

//   return relId;
// }


// void freeLinkedList(AttrCacheEntry *head)
// {
//     for (AttrCacheEntry *it = head, *next; it != nullptr; it = next)
//     {
//         next = it->next;
//         free(it);
//     }
// }


// // int OpenRelTable::closeRel(int relId) {

// //     if (relId==0 || relId==1/* rel-id corresponds to relation catalog or attribute catalog*/) {
// //         return E_NOTPERMITTED;
// //       }
    
// //       if (relId<0 || relId>=MAX_OPEN/* 0 <= relId < MAX_OPEN */) {
// //         return E_OUTOFBOUND;
// //       }
    
// //       if (OpenRelTable::tableMetaInfo[relId].free==true/* rel-id corresponds to a free slot*/) {
// //         return E_RELNOTOPEN;
// //       }

// //     /****** Releasing the Relation Cache entry of the relation ******/

// //     if (RelCacheTable::relCache[relId]->dirty == true/* RelCatEntry of the relIdth Relation Cache entry has been modified */)
// //     {
// //         /* Get the Relation Catalog entry from RelCacheTable::relCache
// //         Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */

// //         // declaring an object of RecBuffer class to write back to the buffer

// //         // Write back to the buffer using relCatBlock.setRecord() with recId.slot
// //         RelCatEntry relCatEntry;
// //         Attribute rec[6];
// //         relCatEntry = RelCacheTable::relCache[relId]->relCatEntry;
// //         RelCacheTable::relCatEntryToRecord(&relCatEntry, rec);
    
// //         RecId recId = RelCacheTable::relCache[relId]->recId;


// //     // declaring an object of RecBuffer class to write back to the buffer
// //         RecBuffer relCatBlock(recId.block);

// //     // Write back to the buffer using relCatBlock.setRecord() with recId.slot
// //         relCatBlock.setRecord(rec , recId.slot);
// //     }

// //     // free the memory dynamically alloted to this Relation Cache entry
// //     // and assign nullptr to that entry
// //     free(RelCacheTable::relCache[relId]);
// //     RelCacheTable::relCache[relId] = nullptr;

// //     /****** Releasing the Attribute Cache entry of the relation ******/

// //     // for all the entries in the linked list of the relIdth Attribute Cache entry.
// //     for(AttrCacheEntry* entry = AttrCacheTable::attrCache[relId]; entry != nullptr; entry = entry->next)
// //     {
// //         if(entry->dirty == true)
// //         {
// //             /* Get the Attribute Catalog entry from attrCache
// //              Then convert it to a record using AttrCacheTable::attrCatEntryToRecord().
// //              Write back that entry by instantiating RecBuffer class. Use recId
// //              member field and recBuffer.setRecord() */
// //             AttrCatEntry attrCatEntry;
// //             Attribute rec[6];
// //             attrCatEntry = entry->attrCatEntry;
// //             AttrCacheTable::attrCatEntryToRecord(&attrCatEntry, rec);
    
// //             RecId recId = entry->recId;
// //             RecBuffer attrCatBlock(recId.block);
// //             attrCatBlock.setRecord(rec , recId.slot);
// //         }

// //         // free the memory dynamically alloted to this entry in Attribute
// //         // Cache linked list and assign nullptr to that entry
// //         free(entry);
// //         entry = nullptr;

// //     }
// //     freeLinkedList(AttrCacheTable::attrCache[relId]);
// //     AttrCacheTable::attrCache[relId] = nullptr;

    

// //     /****** Updating metadata in the Open Relation Table of the relation  ******/

// //     //free the relIdth entry of the tableMetaInfo.
// //     OpenRelTable::tableMetaInfo[relId].free = true;

// //     return SUCCESS;
// // }

// int OpenRelTable::closeRel(int relId)
// {
//     if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
//         return E_NOTPERMITTED;

//     if (relId < 0 || relId >= MAX_OPEN)
//         return E_OUTOFBOUND;

//     if (tableMetaInfo[relId].free == true)
//         return E_RELNOTOPEN;

//     RelCacheEntry *relCacheEntry = RelCacheTable::relCache[relId];

//     if (relCacheEntry && relCacheEntry->dirty == true)
//     {
//         RecBuffer relCatBlock((relCacheEntry->recId).block);

//         RelCatEntry relCatEntry = relCacheEntry->relCatEntry;
//         Attribute record[RELCAT_NO_ATTRS];

//         RelCacheTable::relCatEntryToRecord(&relCatEntry, record);

//         relCatBlock.setRecord(record, (relCacheEntry->recId).slot);
//     }

//     free(relCacheEntry);
//     RelCacheTable::relCache[relId] = nullptr;

//     for (auto attrCacheEntry = AttrCacheTable::attrCache[relId]; attrCacheEntry != nullptr; attrCacheEntry = attrCacheEntry->next)
//     {
//         if (attrCacheEntry && attrCacheEntry->dirty == true)
//         {
//             RecBuffer attrCatBlock((attrCacheEntry->recId).block);

//             AttrCatEntry attrCatEntry = attrCacheEntry->attrCatEntry;
//             Attribute record[ATTRCAT_NO_ATTRS];

//             AttrCacheTable::attrCatEntryToRecord(&attrCatEntry, record);

//             attrCatBlock.setRecord(record, (attrCacheEntry->recId).slot);
//         }
//     }

//     freeLinkedList(AttrCacheTable::attrCache[relId]);
//     AttrCacheTable::attrCache[relId] = nullptr;

//     tableMetaInfo[relId].free = true;
//     return SUCCESS;
// }






#include "OpenRelTable.h"
#include <stdlib.h>
#include <cstring>
#include <stdio.h>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

AttrCacheEntry *createLinkedList(int length)
{
    AttrCacheEntry *head = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    AttrCacheEntry *tail = head;
    for (int i = 1; i < length; i++)
    {
        tail->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
        tail = tail->next;
    }
    tail->next = nullptr;
    return head;
}

void freeLinkedList(AttrCacheEntry *head)
{
    for (AttrCacheEntry *it = head, *next; it != nullptr; it = next)
    {
        next = it->next;
        free(it);
    }
}

OpenRelTable::OpenRelTable()
{
    for (int i = 0; i < MAX_OPEN; i++)
    {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        OpenRelTable::tableMetaInfo[i].free = true;
    }

    RecBuffer relCatBlock(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    for (int i = RELCAT_RELID; i <= ATTRCAT_RELID; i++)
    {
        relCatBlock.getRecord(relCatRecord, i);

        struct RelCacheEntry relCacheEntry;
        RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
        relCacheEntry.recId.block = RELCAT_BLOCK;
        relCacheEntry.recId.slot = i;

        RelCacheTable::relCache[i] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
        *(RelCacheTable::relCache[i]) = relCacheEntry;
        tableMetaInfo[i].free = false;
        memcpy(tableMetaInfo[i].relName, relCacheEntry.relCatEntry.relName, ATTR_SIZE);
    };

    RecBuffer attrCatBlock(ATTRCAT_BLOCK);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    auto relCatListHead = createLinkedList(RELCAT_NO_ATTRS);
    auto attrCacheEntry = relCatListHead;

    for (int i = 0; i < RELCAT_NO_ATTRS; i++)
    {
        attrCatBlock.getRecord(attrCatRecord, i);

        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
        (attrCacheEntry->recId).block = ATTRCAT_BLOCK;
        (attrCacheEntry->recId).slot = i;

        attrCacheEntry = attrCacheEntry->next;
    }
    AttrCacheTable::attrCache[RELCAT_RELID] = relCatListHead;

    auto attrCatListHead = createLinkedList(ATTRCAT_NO_ATTRS);
    attrCacheEntry = attrCatListHead;
    for (int i = RELCAT_NO_ATTRS; i < RELCAT_NO_ATTRS + ATTRCAT_NO_ATTRS; i++)
    {
        attrCatBlock.getRecord(attrCatRecord, i);
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
        (attrCacheEntry->recId).block = ATTRCAT_BLOCK;
        (attrCacheEntry->recId).slot = i;

        attrCacheEntry = attrCacheEntry->next;
    }
    AttrCacheTable::attrCache[ATTRCAT_RELID] = attrCatListHead;
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE])
{
    for (int i = 0; i < MAX_OPEN; i++)
    {
        if (!tableMetaInfo[i].free && strcmp(relName, tableMetaInfo[i].relName) == 0)
            return i;
    }

    return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry()
{
    for (int i = 2; i < MAX_OPEN; i++)
    {
        if (tableMetaInfo[i].free)
            return i;
    }

    return E_CACHEFULL;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE])
{
    int exist = OpenRelTable::getRelId(relName);

    if (exist >= 0)
    {
        return exist;
    }

    int freeSlot = OpenRelTable::getFreeOpenRelTableEntry();

    if (freeSlot == E_CACHEFULL)
    {
        return E_CACHEFULL;
    }

    Attribute relNameAttribute;
    memcpy(relNameAttribute.sVal, relName, ATTR_SIZE);
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, relNameAttribute, EQ);

    if (relcatRecId.block == -1 && relcatRecId.slot == -1)
    {
        return E_RELNOTEXIST;
    }
    RecBuffer recBuffer(relcatRecId.block);

    Attribute record[RELCAT_NO_ATTRS];
    recBuffer.getRecord(record, relcatRecId.slot);

    RelCatEntry relCatEntry;

    RelCacheTable::recordToRelCatEntry(record, &relCatEntry);

    RelCacheTable::relCache[freeSlot] = (RelCacheEntry *)malloc(sizeof(RelCacheEntry));

    RelCacheTable::relCache[freeSlot]->recId = relcatRecId;
    RelCacheTable::relCache[freeSlot]->relCatEntry = relCatEntry;

    int numAttrs = relCatEntry.numAttrs;
    AttrCacheEntry *listHead = createLinkedList(numAttrs);
    AttrCacheEntry *node = listHead;

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    while (true)
    {
        RecId searchRes = BlockAccess::linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, relNameAttribute, EQ);

        if (searchRes.block != -1 && searchRes.slot != -1)
        {
            Attribute attrcatRecord[ATTRCAT_NO_ATTRS];

            RecBuffer attrRecBuffer(searchRes.block);

            attrRecBuffer.getRecord(attrcatRecord, searchRes.slot);

            AttrCatEntry attrCatEntry;
            AttrCacheTable::recordToAttrCatEntry(attrcatRecord, &attrCatEntry);

            node->recId = searchRes;
            node->attrCatEntry = attrCatEntry;
            node = node->next;
        }
        else
            break;
    }

    AttrCacheTable::attrCache[freeSlot] = listHead;

    OpenRelTable::tableMetaInfo[freeSlot].free = false;
    memcpy(OpenRelTable::tableMetaInfo[freeSlot].relName, relCatEntry.relName, ATTR_SIZE);

    return freeSlot;
}

int OpenRelTable::closeRel(int relId)
{
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;

    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (tableMetaInfo[relId].free == true)
        return E_RELNOTOPEN;

    RelCacheEntry *relCacheEntry = RelCacheTable::relCache[relId];

    if (relCacheEntry && relCacheEntry->dirty == true)
    {
        RecBuffer relCatBlock((relCacheEntry->recId).block);

        RelCatEntry relCatEntry = relCacheEntry->relCatEntry;
        Attribute record[RELCAT_NO_ATTRS];

        RelCacheTable::relCatEntryToRecord(&relCatEntry, record);

        relCatBlock.setRecord(record, (relCacheEntry->recId).slot);
    }

    free(relCacheEntry);
    RelCacheTable::relCache[relId] = nullptr;

    for (auto attrCacheEntry = AttrCacheTable::attrCache[relId]; attrCacheEntry != nullptr; attrCacheEntry = attrCacheEntry->next)
    {
        if (attrCacheEntry && attrCacheEntry->dirty == true)
        {
            RecBuffer attrCatBlock((attrCacheEntry->recId).block);

            AttrCatEntry attrCatEntry = attrCacheEntry->attrCatEntry;
            Attribute record[ATTRCAT_NO_ATTRS];

            AttrCacheTable::attrCatEntryToRecord(&attrCatEntry, record);

            attrCatBlock.setRecord(record, (attrCacheEntry->recId).slot);
        }
    }

    freeLinkedList(AttrCacheTable::attrCache[relId]);
    AttrCacheTable::attrCache[relId] = nullptr;

    tableMetaInfo[relId].free = true;
    return SUCCESS;
}

OpenRelTable::~OpenRelTable()
{

    for (int i = 2; i < MAX_OPEN; i++)
    {
        if (!tableMetaInfo[i].free)
            OpenRelTable::closeRel(i);
    }

    for (int i = 2; i < MAX_OPEN; i++)
    {
        free(RelCacheTable::relCache[i]);
        freeLinkedList(AttrCacheTable::attrCache[i]);

        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
    }

    if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty)
    {
        Attribute relCatRecord[RELCAT_NO_ATTRS];

        RelCatEntry relCatEntry = RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry;
        RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;

        RelCacheTable::relCatEntryToRecord(&relCatEntry, relCatRecord);

        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(relCatRecord, recId.slot);

        free(RelCacheTable::relCache[ATTRCAT_RELID]);
    }

    if (RelCacheTable::relCache[RELCAT_RELID]->dirty)
    {
        Attribute relCatRecord[RELCAT_NO_ATTRS];

        RelCatEntry relCatEntry = RelCacheTable::relCache[RELCAT_RELID]->relCatEntry;
        RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;

        RelCacheTable::relCatEntryToRecord(&relCatEntry, relCatRecord);

        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(relCatRecord, recId.slot);

        free(RelCacheTable::relCache[RELCAT_RELID]);
    }

    freeLinkedList(AttrCacheTable::attrCache[RELCAT_RELID]);
    freeLinkedList(AttrCacheTable::attrCache[ATTRCAT_RELID]);
}