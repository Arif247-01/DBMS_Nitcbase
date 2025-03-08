// #include "BlockAccess.h"
// #include <cstdlib>
// #include <cstring>
// #include <stdio.h>


// RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
//     // get the previous search index of the relation relId from the relation cache
//     // (use RelCacheTable::getSearchIndex() function)
//     RecId prevRecId;
//     RelCacheTable::getSearchIndex(relId, &prevRecId);

//     // let block and slot denote the record id of the record being currently checked
//     int block, slot;

//     // if the current search index record is invalid(i.e. both block and slot = -1)
//     if (prevRecId.block == -1 && prevRecId.slot == -1)
//     {
//         // (no hits from previous search; search should start from the
//         // first record itself)

//         // get the first record block of the relation from the relation cache
//         // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
//         RelCatEntry relCatEntry;
//         RelCacheTable::getRelCatEntry(relId, &relCatEntry);

//         // block = first record block of the relation
//         block = relCatEntry.firstBlk;
//         // slot = 0
//         slot = 0;
//     }
//     else
//     {
//         // (there is a hit from previous search; search should start from
//         // the record next to the search index record)

//         // block = search index's block
//         block = prevRecId.block;
//         // slot = search index's slot + 1
//         slot = prevRecId.slot + 1;
//     }

//     /* The following code searches for the next record in the relation
//        that satisfies the given condition
//        We start from the record id (block, slot) and iterate over the remaining
//        records of the relation
//     */
//     while (block != -1)
//     {
//         /* create a RecBuffer object for block (use RecBuffer Constructor for
//            existing block) */
//         RecBuffer recBuffer(block);
        
//         // get header of the block using RecBuffer::getHeader() function
//         HeadInfo head;
//         recBuffer.getHeader(&head);
//         // get the record with id (block, slot) using RecBuffer::getRecord()
//         Attribute rec[head.numAttrs];
//         recBuffer.getRecord(rec, slot);
//         // get slot map of the block using RecBuffer::getSlotMap() function
//         unsigned char slotMap[head.numSlots];
//         recBuffer.getSlotMap(slotMap);
//         // If slot >= the number of slots per block(i.e. no more slots in this block)
//         if(slot >= head.numSlots)
//         {
//             // update block = right block of block
//             block = head.rblock;
//             // update slot = 0
//             slot = 0;
//             continue;  // continue to the beginning of this while loop
//         }

//         // if slot is free skip the loop
//         // (i.e. check if slot'th entry in slot map of block contains SLOT_UNOCCUPIED)
//         if(slotMap[slot] == SLOT_UNOCCUPIED)
//         {
//             // increment slot and continue to the next record slot
//             slot++; //doubt
//             continue;
//         }

//         // compare record's attribute value to the the given attrVal as below:
//         /*
//             firstly get the attribute offset for the attrName attribute
//             from the attribute cache entry of the relation using
//             AttrCacheTable::getAttrCatEntry()
//         */
//         AttrCatEntry attrCatEntry;
//         AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
//         int offset = attrCatEntry.offset;
//         /* use the attribute offset to get the value of the attribute from
//            current record */
//         Attribute attrValue;
//         attrValue = rec[offset];
//         int attrType = attrCatEntry.attrType;
        

//         int cmpVal;  // will store the difference between the attributes
//         // set cmpVal using compareAttrs()
//         cmpVal = compareAttrs(attrValue, attrVal, attrType);//not done

//         /* Next task is to check whether this record satisfies the given condition.
//            It is determined based on the output of previous comparison and
//            the op value received.
//            The following code sets the cond variable if the condition is satisfied.
//         */
//         if (
//             (op == NE && cmpVal != 0) ||    // if op is "not equal to"
//             (op == LT && cmpVal < 0) ||     // if op is "less than"
//             (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
//             (op == EQ && cmpVal == 0) ||    // if op is "equal to"
//             (op == GT && cmpVal > 0) ||     // if op is "greater than"
//             (op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
//         ) {
//             /*
//             set the search index in the relation cache as
//             the record id of the record that satisfies the given condition
//             (use RelCacheTable::setSearchIndex function)
//             */
//             RecId x;
//             x.block = block;
//             x.slot = slot;
//             RelCacheTable::setSearchIndex(relId, &x);//doubt

//             return RecId{block, slot};
//         }

//         slot++;
//     }

//     // no record in the relation with Id relid satisfies the given condition
//     return RecId{-1, -1};
// }


// int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
//     /* reset the searchIndex of the relation catalog using
//        RelCacheTable::resetSearchIndex() */
//     RelCacheTable::resetSearchIndex(RELCAT_RELID);

//     Attribute newRelationName; // set newRelationName with newName
//     strcpy(newRelationName.sVal,newName);

//     // search the relation catalog for an entry with "RelName" = newRelationName
//     RecId z;
//     z = linearSearch(RELCAT_RELID, (char *)"RelName", newRelationName, EQ);

//     // If relation with name newName already exists (result of linearSearch
//     //                                               is not {-1, -1})
//     //    return E_RELEXIST;
    
//     if(z.block!=-1 && z.slot!=-1){
//       return E_RELEXIST;
//     }

//     /* reset the searchIndex of the relation catalog using
//        RelCacheTable::resetSearchIndex() */
//     RelCacheTable::resetSearchIndex(RELCAT_RELID);

//     Attribute oldRelationName; // set oldRelationName with oldName
//     strcpy(oldRelationName.sVal, oldName);
    

//     // search the relation catalog for an entry with "RelName" = oldRelationName
//     z = linearSearch(RELCAT_RELID, (char *)"RelName", oldRelationName, EQ);

//     // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
//     //    return E_RELNOTEXIST;
//     if(z.block==-1 && z.slot==-1){
//       return E_RELNOTEXIST;
//     }

//     /* get the relation catalog record of the relation to rename using a RecBuffer
//        on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
//     */
//     RecBuffer recBuffer(z.block);//z.block
//     Attribute rec[6];
//     recBuffer.getRecord(rec, z.slot);
//     int numAttrs = rec[1].nVal;
//     /* update the relation name attribute in the record with newName.
//        (use RELCAT_REL_NAME_INDEX) */
//     strcpy(rec[RELCAT_REL_NAME_INDEX].sVal,newName);
//     // set back the record value using RecBuffer.setRecord
//     recBuffer.setRecord(rec, z.slot);

//     /*
//     update all the attribute catalog entries in the attribute catalog corresponding
//     to the relation with relation name oldName to the relation name newName
//     */

//     /* reset the searchIndex of the attribute catalog using
//        RelCacheTable::resetSearchIndex() */
//     RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

//     //for i = 0 to numberOfAttributes :
//     //    linearSearch on the attribute catalog for relName = oldRelationName
//     //    get the record using RecBuffer.getRecord
//     //
//     //    update the relName field in the record to newName
//     //    set back the record using RecBuffer.setRecord

//     for(int i=0; i< numAttrs;i++){
//       z = linearSearch(ATTRCAT_RELID, (char *)"RelName", oldRelationName, EQ);
//       RecBuffer rec1Buffer(z.block);
//       rec1Buffer.getRecord(rec, z.slot);
//       strcpy(rec[ATTRCAT_REL_NAME_INDEX].sVal,newName);
//       rec1Buffer.setRecord(rec, z.slot);
//     }

//     return SUCCESS;
// }

// int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

//     /* reset the searchIndex of the relation catalog using
//        RelCacheTable::resetSearchIndex() */
//     RelCacheTable::resetSearchIndex(RELCAT_RELID);

//     Attribute relNameAttr;    // set relNameAttr to relName
//     strcpy(relNameAttr.sVal, relName);

//     // Search for the relation with name relName in relation catalog using linearSearch()
//     // If relation with name relName does not exist (search returns {-1,-1})
//     //    return E_RELNOTEXIST;
    
//     RecId z;
//     z = linearSearch(RELCAT_RELID, (char *)"RelName", relNameAttr, EQ);
//     if(z.block==-1 && z.slot==-1){
//       return E_RELNOTEXIST;
//     }

//     /* reset the searchIndex of the attribute catalog using
//        RelCacheTable::resetSearchIndex() */
//     RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

//     /* declare variable attrToRenameRecId used to store the attr-cat recId
//     of the attribute to rename */
//     RecId attrToRenameRecId{-1, -1};
//     Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

//     /* iterate over all Attribute Catalog Entry record corresponding to the
//        relation to find the required attribute */
//     while (true) {
//         // linear search on the attribute catalog for RelName = relNameAttr
//         RecId x = linearSearch(ATTRCAT_RELID, (char *)"RelName", relNameAttr, EQ);

//         // if there are no more attributes left to check (linearSearch returned {-1,-1})
//         //     break;
//         if(x.block==-1 && x.slot==-1) break;

//         /* Get the record from the attribute catalog using RecBuffer.getRecord
//           into attrCatEntryRecord */
//         RecBuffer rec2Buffer(x.block);
//         rec2Buffer.getRecord(attrCatEntryRecord, x.slot);

//         // if attrCatEntryRecord.attrName = oldName
//         //     attrToRenameRecId = block and slot of this record
//         if(strcmp(attrCatEntryRecord[1].sVal, oldName)==0){
//           attrToRenameRecId.block = x.block;
//           attrToRenameRecId.slot = x.slot;
//         }

//         // if attrCatEntryRecord.attrName = newName
//         //     return E_ATTREXIST;
//         if(strcmp(attrCatEntryRecord[1].sVal, newName)==0){
//           return E_ATTREXIST;
//         }
//     }

//     // if attrToRenameRecId == {-1, -1}
//     //     return E_ATTRNOTEXIST;
//     if(attrToRenameRecId.block==-1 && attrToRenameRecId.slot==-1){
//       return E_ATTRNOTEXIST;
//     }


//     // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
//     /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
//          attrToRenameRecId.slot */
//     RecBuffer rec3Buffer(attrToRenameRecId.block);
//     Attribute rec3[6];
//     rec3Buffer.getRecord(rec3, attrToRenameRecId.slot);
//     //   update the AttrName of the record with newName
//     strcpy(rec3[1].sVal, newName);
//     //   set back the record with RecBuffer.setRecord
//     rec3Buffer.setRecord(rec3, attrToRenameRecId.slot);

//     return SUCCESS;
// }


// int BlockAccess::insert(int relId, Attribute *record) {
//   // get the relation catalog entry from relation cache
//   // ( use RelCacheTable::getRelCatEntry() of Cache Layer)
//   RelCatEntry relCatEntry;
//   RelCacheTable::getRelCatEntry(relId, &relCatEntry);

//   int blockNum = relCatEntry.firstBlk;/* first record block of the relation (from the rel-cat entry)*/

//   // rec_id will be used to store where the new record will be inserted
//   RecId rec_id = {-1, -1};

//   int numOfSlots = relCatEntry.numSlotsPerBlk;/* number of slots per record block */
//   int numOfAttributes = relCatEntry.numAttrs;/* number of attributes of the relation */

//   int prevBlockNum = -1;/* block number of the last element in the linked list = -1 */

//   /*
//       Traversing the linked list of existing record blocks of the relation
//       until a free slot is found OR
//       until the end of the list is reached
//   */
//   while (blockNum != -1) {
//       // create a RecBuffer object for blockNum (using appropriate constructor!)
//       RecBuffer recBuffer(blockNum);

//       // get header of block(blockNum) using RecBuffer::getHeader() function
//       HeadInfo head;
//       recBuffer.getHeader(&head);

//       // get slot map of block(blockNum) using RecBuffer::getSlotMap() function
//       unsigned char slotMap[head.numSlots];
//       recBuffer.getSlotMap(slotMap);

//       // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
//       int flag = 0;
//       for(int i=0; i<head.numSlots; i++){
//         if((int)slotMap[i] == SLOT_UNOCCUPIED){
//           rec_id.slot = i;
//           rec_id.block = blockNum;
//           flag = 1;
//           break;
//         }
//       }
//       // (Free slot can be found by iterating over the slot map of the block)
//       /* slot map stores SLOT_UNOCCUPIED if slot is free and
//          SLOT_OCCUPIED if slot is occupied) */

//       /* if a free slot is found, set rec_id and discontinue the traversal
//          of the linked list of record blocks (break from the loop) */
//          if(flag==1) break;

//       /* otherwise, continue to check the next block by updating the
//          block numbers as follows:
//             update prevBlockNum = blockNum
//             update blockNum = header.rblock (next element in the linked
//                                              list of record blocks)
//       */
//       prevBlockNum = blockNum;
//       blockNum = head.rblock;
//   }

//   //  if no free slot is found in existing record blocks (rec_id = {-1, -1})
//   if(rec_id.block==-1 && rec_id.slot==-1)
//   {
//       // if relation is RELCAT, do not allocate any more blocks
//       //     return E_MAXRELATIONS;
//       if(relId == 0) return E_MAXRELATIONS;

//       // Otherwise,
//       // get a new record block (using the appropriate RecBuffer constructor!)
//       RecBuffer recbuf;
//       // get the block number of the newly allocated block
//       // (use BlockBuffer::getBlockNum() function)
//       // let ret be the return value of getBlockNum() function call
//       int ret = recbuf.getBlockNum();
//       if (ret == E_DISKFULL) {
//           return E_DISKFULL;
//       }

//       // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0
//       rec_id.block = ret;
//       rec_id.slot = 0;

//       /*
//           set the header of the new record block such that it links with
//           existing record blocks of the relation
//           set the block's header as follows:
//           blockType: REC, pblock: -1
//           lblock
//                 = -1 (if linked list of existing record blocks was empty
//                        i.e this is the first insertion into the relation)
//                 = prevBlockNum (otherwise),
//           rblock: -1, numEntries: 0,
//           numSlots: numOfSlots, numAttrs: numOfAttributes
//           (use BlockBuffer::setHeader() function)
//       */
//       HeadInfo head;
//       head.blockType = REC;
//       head.pblock = -1;
//       if(relCatEntry.firstBlk==-1) head.lblock = -1;
//       else head.lblock = prevBlockNum;
//       head.rblock = -1;
//       head.numEntries = 0;
//       head.numSlots = numOfSlots;
//       head.numAttrs = numOfAttributes;
      
//       recbuf.setHeader(&head);

//       /*
//           set block's slot map with all slots marked as free
//           (i.e. store SLOT_UNOCCUPIED for all the entries)
//           (use RecBuffer::setSlotMap() function)
//       */
//       unsigned char smap[numOfSlots];
//       for(int i=0; i<numOfSlots; i++){
//         smap[i] = (unsigned char)SLOT_UNOCCUPIED;
//       }
//       recbuf.setSlotMap(smap);

//       // if prevBlockNum != -1
//       if(prevBlockNum != -1)
//       {
//           // create a RecBuffer object for prevBlockNum
//           RecBuffer buffer(prevBlockNum);
//           // get the header of the block prevBlockNum and
//           HeadInfo head1;
//           buffer.getHeader(&head1);
//           // update the rblock field of the header to the new block
//           head1.rblock = rec_id.block;
//           // number i.e. rec_id.block
//           // (use BlockBuffer::setHeader() function)
//           buffer.setHeader(&head1);
//       }
//       // else
//       else
//       {
//           // update first block field in the relation catalog entry to the
//           relCatEntry.firstBlk = rec_id.block;
//           // new block (using RelCacheTable::setRelCatEntry() function)
//           RelCacheTable::setRelCatEntry(relId, &relCatEntry);
//       }

//       // update last block field in the relation catalog entry to the
//       // new block (using RelCacheTable::setRelCatEntry() function)
//       relCatEntry.lastBlk = rec_id.block;
//       RelCacheTable::setRelCatEntry(relId, &relCatEntry);
//   }

//   // create a RecBuffer object for rec_id.block
//   RecBuffer buffer1(rec_id.block);
//   // insert the record into rec_id'th slot using RecBuffer.setRecord())
//   buffer1.setRecord(record , rec_id.slot);

//   /* update the slot map of the block by marking entry of the slot to
//      which record was inserted as occupied) */
//   // (ie store SLOT_OCCUPIED in free_slot'th entry of slot map)
//   // (use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions)
//   unsigned char map[numOfSlots];
//   buffer1.getSlotMap(map);
//   map[rec_id.slot] = (unsigned char)SLOT_OCCUPIED;
//   buffer1.setSlotMap(map);

//   // increment the numEntries field in the header of the block to
//   // which record was inserted
//   // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
//   HeadInfo headx;
//   buffer1.getHeader(&headx);
//   headx.numEntries++;
//   buffer1.setHeader(&headx);
  

//   // Increment the number of records field in the relation cache entry for
//   // the relation. (use RelCacheTable::setRelCatEntry function)
//   relCatEntry.numRecs++;
//   RelCacheTable::setRelCatEntry(relId, &relCatEntry);

//   /* B+ Tree Insertions */
//     // (the following section is only relevant once indexing has been implemented)

//     int flag = SUCCESS;
//     // Iterate over all the attributes of the relation
//     // (let attrOffset be iterator ranging from 0 to numOfAttributes-1)
//     for(int i = 0; i< numOfAttributes; i++)
//     {
//         // get the attribute catalog entry for the attribute from the attribute cache
//         // (use AttrCacheTable::getAttrCatEntry() with args relId and attrOffset)
//         AttrCatEntry attrCatEntry;
//         AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);

//         // get the root block field from the attribute catalog entry
//         int rootBlock = attrCatEntry.rootBlock;

//         // if index exists for the attribute(i.e. rootBlock != -1)
//         if(rootBlock != -1)
//         {
//             /* insert the new record into the attribute's bplus tree using
//              BPlusTree::bPlusInsert()*/
//             int retVal = BPlusTree::bPlusInsert(relId, attrCatEntry.attrName,
//                                                 record[i], rec_id);

//             if (retVal == E_DISKFULL) {
//                 //(index for this attribute has been destroyed)
//                 // flag = E_INDEX_BLOCKS_RELEASED
//                 flag = E_INDEX_BLOCKS_RELEASED;
//             }
//         }
//     }

//   return flag;
// }


// /*
// NOTE: This function will copy the result of the search to the `record` argument.
//       The caller should ensure that space is allocated for `record` array
//       based on the number of attributes in the relation.
// */
// int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
//   // Declare a variable called recid to store the searched record
//   RecId recId;

//   /* get the attribute catalog entry from the attribute cache corresponding
//   to the relation with Id=relid and with attribute_name=attrName  */
//   AttrCatEntry attrCatEntry;
//   int k = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

//   // if this call returns an error, return the appropriate error code
//   if(k!=SUCCESS) return k;

//   // get rootBlock from the attribute catalog entry
//   int rootBlock = attrCatEntry.rootBlock;
//   /* if Index does not exist for the attribute (check rootBlock == -1) */ 
//   if(rootBlock == -1){

//       /* search for the record id (recid) corresponding to the attribute with
//          attribute name attrName, with value attrval and satisfying the
//          condition op using linearSearch()
//       */
//      recId = linearSearch(relId, attrName, attrVal, op);
//   }

//   /* else */ 
//   else{
//       // (index exists for the attribute)

//       /* search for the record id (recid) correspoding to the attribute with
//       attribute name attrName and with value attrval and satisfying the
//       condition op using BPlusTree::bPlusSearch() */
//     recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
//   }


//   // if there's no record satisfying the given condition (recId = {-1, -1})
//   //     return E_NOTFOUND;
//   if(recId.block == -1 && recId.slot == -1) return E_NOTFOUND;

//   /* Copy the record with record id (recId) to the record buffer (record).
//      For this, instantiate a RecBuffer class object by passing the recId and
//      call the appropriate method to fetch the record
//   */
//   RecBuffer recBuffer(recId.block);
//   recBuffer.getRecord(record, recId.slot);

//   return SUCCESS;
// }


// int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
//   // if the relation to delete is either Relation Catalog or Attribute Catalog,
//   //     return E_NOTPERMITTED
//       // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
//       // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)
//   if(strcmp(RELCAT_RELNAME, relName)==0 || strcmp(ATTRCAT_RELNAME, relName)==0) return  E_NOTPERMITTED; 

//   /* reset the searchIndex of the relation catalog using
//      RelCacheTable::resetSearchIndex() */
//      RelCacheTable::resetSearchIndex(RELCAT_RELID);

//   Attribute relNameAttr; // (stores relName as type union Attribute)
//   // assign relNameAttr.sVal = relName
//   strcpy(relNameAttr.sVal, relName);

//   //  linearSearch on the relation catalog for RelName = relNameAttr
//   RecId recId;
//   recId = linearSearch(RELCAT_RELID, (char *)"RelName", relNameAttr, EQ);

//   // if the relation does not exist (linearSearch returned {-1, -1})
//   //     return E_RELNOTEXIST
//   if(recId.slot==-1 && recId.block==-1) return E_RELNOTEXIST;

//   Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
//   /* store the relation catalog record corresponding to the relation in
//      relCatEntryRecord using RecBuffer.getRecord */
//     RecBuffer recBuffer(recId.block);
//     recBuffer.getRecord(relCatEntryRecord, recId.slot);

//   /* get the first record block of the relation (firstBlock) using the
//      relation catalog entry record */
//     int firstBlock = (int)relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
//   /* get the number of attributes corresponding to the relation (numAttrs)
//      using the relation catalog entry record */
//     int numAttrs = (int)relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

//   /*
//    Delete all the record blocks of the relation
//   */
//   // for each record block of the relation:
//   //     get block header using BlockBuffer.getHeader
//   //     get the next block from the header (rblock)
//   //     release the block using BlockBuffer.releaseBlock
//   //
//   //     Hint: to know if we reached the end, check if nextBlock = -1
//   int nextBlock = firstBlock;
//   while(nextBlock!=-1){
//     BlockBuffer blockBuffer(nextBlock);
//     HeadInfo head;
//     blockBuffer.getHeader(&head);
//     nextBlock = head.rblock;
//     blockBuffer.releaseBlock();
//   }

//   /***
//         Deleting attribute catalog entries corresponding the relation and index
//         blocks corresponding to the relation with relName on its attributes
//     ***/

//     // reset the searchIndex of the attribute catalog
//     RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

//     int numberOfAttributesDeleted = 0;

//     while(true) {
//         RecId attrCatRecId;
//         // attrCatRecId = linearSearch on attribute catalog for RelName = relNameAttr
//         attrCatRecId = linearSearch(ATTRCAT_RELID, (char *)"RelName", relNameAttr, EQ);

//         // if no more attributes to iterate over (attrCatRecId == {-1, -1})
//         //     break;
//         if(attrCatRecId.block==-1 && attrCatRecId.slot==-1) break;

//         numberOfAttributesDeleted++;

//         // create a RecBuffer for attrCatRecId.block
//         // get the header of the block
//         // get the record corresponding to attrCatRecId.slot
//         RecBuffer recBuffer(attrCatRecId.block);
//         HeadInfo head;
//         recBuffer.getHeader(&head);
//         Attribute rec[head.numAttrs];
//         recBuffer.getRecord(rec, attrCatRecId.slot);

//         // declare variable rootBlock which will be used to store the root
//         // block field from the attribute catalog record.
//         int rootBlock = rec[ATTRCAT_ROOT_BLOCK_INDEX].nVal;/* get root block from the record */;
//         // (This will be used later to delete any indexes if it exists)

//         // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
//         // Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap
//         unsigned char slotmap[head.numSlots];
//         recBuffer.getSlotMap(slotmap);
//         slotmap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
//         recBuffer.setSlotMap(slotmap);


//         /* Decrement the numEntries in the header of the block corresponding to
//            the attribute catalog entry and then set back the header
//            using RecBuffer.setHeader */
//         head.numEntries--;
//         recBuffer.setHeader(&head);

//         /* If number of entries become 0, releaseBlock is called after fixing
//            the linked list.
//         */
//         if (head.numEntries==0/*   header.numEntries == 0  */) {
//             /* Standard Linked List Delete for a Block
//                Get the header of the left block and set it's rblock to this
//                block's rblock
//             */

//             // create a RecBuffer for lblock and call appropriate methods
//             RecBuffer rec1buffer(head.lblock);
//             HeadInfo heady;
//             rec1buffer.getHeader(&heady);
//             heady.rblock = head.rblock;
//             rec1buffer.setHeader(&heady);

//             if (head.rblock != -1/* header.rblock != -1 */) {
//                 /* Get the header of the right block and set it's lblock to
//                    this block's lblock */
//                 // create a RecBuffer for rblock and call appropriate methods
//                 RecBuffer rec2buffer(head.rblock);
//                 HeadInfo headx;
//                 rec2buffer.getHeader(&headx);
//                 headx.lblock = head.lblock;
//                 rec2buffer.setHeader(&headx);
                
//             } else {
//                 // (the block being released is the "Last Block" of the relation.)
//                 /* update the Relation Catalog entry's LastBlock field for this
//                    relation with the block number of the previous block. */
//                    RelCatEntry relCatx;
//                    RelCacheTable::getRelCatEntry(ATTRCAT_RELID,&relCatx);
//                    relCatx.lastBlk = head.lblock;
//                    RelCacheTable::setRelCatEntry(ATTRCAT_RELID,&relCatx);
//             }

//             // (Since the attribute catalog will never be empty(why?), we do not
//             //  need to handle the case of the linked list becoming empty - i.e
//             //  every block of the attribute catalog gets released.)

//             // call releaseBlock()
//             recBuffer.releaseBlock();
//         }

//         if(rootBlock != -1){
//           BPlusTree::bPlusDestroy(rootBlock);
//         }
//     }

//   /*** Delete the entry corresponding to the relation from relation catalog ***/
//   // Fetch the header of Relcat block
//   BlockBuffer buf(4);
//   HeadInfo headx;
//   buf.getHeader(&headx);

//   /* Decrement the numEntries in the header of the block corresponding to the
//      relation catalog entry and set it back */
//     headx.numEntries--;
//     buf.setHeader(&headx);

//   /* Get the slotmap in relation catalog, update it by marking the slot as
//      free(SLOT_UNOCCUPIED) and set it back. */
//     RecBuffer bufx(4);
//     unsigned char map[20];
//     bufx.getSlotMap(map);
//     map[recId.slot] = (unsigned char)SLOT_UNOCCUPIED;  //doubt
//     bufx.setSlotMap(map);

//   /*** Updating the Relation Cache Table ***/
//   /** Update relation catalog record entry (number of records in relation
//       catalog is decreased by 1) **/
//   // Get the entry corresponding to relation catalog from the relation
//   // cache and update the number of records and set it back
//   // (using RelCacheTable::setRelCatEntry() function)
//   RelCatEntry relCat;
//   RelCacheTable::getRelCatEntry(RELCAT_RELID,&relCat);
//   relCat.numRecs--;
//   RelCacheTable::setRelCatEntry(RELCAT_RELID,&relCat);

//    /** Update attribute catalog entry (number of records in attribute catalog
//         is decreased by numberOfAttributesDeleted) **/
//     // i.e., #Records = #Records - numberOfAttributesDeleted

//     // Get the entry corresponding to attribute catalog from the relation
//     // cache and update the number of records and set it back
//     // (using RelCacheTable::setRelCatEntry() function)
//     RelCatEntry attrCat;
//     RelCacheTable::getRelCatEntry(ATTRCAT_RELID,&attrCat);
//     attrCat.numRecs = attrCat.numRecs - numberOfAttributesDeleted;
//     RelCacheTable::setRelCatEntry(ATTRCAT_RELID,&attrCat);
  

//   return SUCCESS;
// }


// /*
// NOTE: the caller is expected to allocate space for the argument `record` based
//       on the size of the relation. This function will only copy the result of
//       the projection onto the array pointed to by the argument.
// */
// int BlockAccess::project(int relId, Attribute *record) 
// {
//   // get the previous search index of the relation relId from the relation
//   // cache (use RelCacheTable::getSearchIndex() function)
//   RecId prevRecId; 
//   RelCacheTable::getSearchIndex(relId, &prevRecId);

//   // declare block and slot which will be used to store the record id of the
//   // slot we need to check.
//   int block, slot;

  
//   /* if the current search index record is invalid(i.e. = {-1, -1})
//      (this only happens when the caller reset the search index)
//   */
//   if (prevRecId.block == -1 && prevRecId.slot == -1)
//   {
//       // (new project operation. start from beginning)

//       // get the first record block of the relation from the relation cache
//       // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
//       RelCatEntry relCatEntry;
//       RelCacheTable::getRelCatEntry(relId,&relCatEntry);

//       // block = first record block of the relation
//       block = relCatEntry.firstBlk;
//       // slot = 0
//       slot = 0;
//   }
//   else
//   {
//       // (a project/search operation is already in progress)

//       // block = previous search index's block
//       block = prevRecId.block;
//       // slot = previous search index's slot + 1
//       slot = prevRecId.slot + 1;
//   }

//   // The following code finds the next record of the relation
//   /* Start from the record id (block, slot) and iterate over the remaining
//      records of the relation */
//   while (block != -1)
//   {
//       // create a RecBuffer object for block (using appropriate constructor!)
//       RecBuffer recBuffer(block);

//       // get header of the block using RecBuffer::getHeader() function
//       HeadInfo head;
//       recBuffer.getHeader(&head);
//       // get slot map of the block using RecBuffer::getSlotMap() function
//       unsigned char slotmap[head.numSlots];
//       recBuffer.getSlotMap(slotmap);

//       if(slot>=head.numSlots/* slot >= the number of slots per block*/)
//       {
//           // (no more slots in this block)
//           // update block = right block of block
//           block = head.rblock;
//           // update slot = 0
//           slot = 0;
//           continue;
//           // (NOTE: if this is the last block, rblock would be -1. this would
//           //        set block = -1 and fail the loop condition )
//       }
//       else if (slotmap[slot] == SLOT_UNOCCUPIED/* slot is free */)
//       { // (i.e slot-th entry in slotMap contains SLOT_UNOCCUPIED)

//           // increment slot
//           slot++;
//       }
//       else {
//           // (the next occupied slot / record has been found)
//           break;
//       }
//   }

//   if (block == -1){
//       // (a record was not found. all records exhausted)
//       return E_NOTFOUND;
//   }

//   // declare nextRecId to store the RecId of the record found
//   RecId nextRecId{block, slot};
//   nextRecId.block = block; 
//   nextRecId.slot = slot;

//   // set the search index to nextRecId using RelCacheTable::setSearchIndex
//   RelCacheTable::setSearchIndex(relId, &nextRecId);

//   /* Copy the record with record id (nextRecId) to the record buffer (record)
//      For this Instantiate a RecBuffer class object by passing the recId and
//      call the appropriate method to fetch the record
//   */
//   RecBuffer buffer(nextRecId.block);
//   buffer.getRecord(record, nextRecId.slot);

//   return SUCCESS;
// }










#include "BlockAccess.h"
#include "../Buffer/BlockBuffer.h"
#include <cstring>
#include <iostream>
using namespace std;

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op)
{
    // TODO: No error handling is done in this function. Should add error handling code.
    // get the previous search index of the relation relId from the relation cache
    // (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block, slot;

    // if the current search index record is invalid(i.e. both block and slot = -1)
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (no hits from previous search; search should start from the
        // first record itself)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);

        block = relCatEntry.firstBlk;
        slot = 0;

        // block = first record block of the relation
        // slot = 0
    }
    else
    {
        // (there is a hit from previous search; search should start from
        // the record next to the search index record)

        // TODO: What if the previous hit is the last record of the relation?
        // How exactly should I move the block and slot pointers in that case?
        // block = search index's block
        // slot = search index's slot + 1
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    /* The following code searches for the next record in the relation
       that satisfies the given condition
       We start from the record id (block, slot) and iterate over the remaining
       records of the relation
    */
    while (block != -1)
    {
        /* create a RecBuffer object for block (use RecBuffer Constructor for
           existing block) */
        RecBuffer recBuffer(block);

        // get the record with id (block, slot) using RecBuffer::getRecord()
        HeadInfo head;
        recBuffer.getHeader(&head);
        unsigned char slotMap[head.numSlots];
        recBuffer.getSlotMap(slotMap);

        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function

        // If slot >= the number of slots per block(i.e. no more slots in this block)
        if (slot >= head.numSlots)
        {
            // update block = right block of block
            // (use the block header to get the right block of the current block)
            block = head.rblock;
            slot = 0;
            // update slot = 0
            continue; // continue to the beginning of this while loop
        }

        int numAttrs = head.numAttrs;

        // if slot is free skip the loop
        // (i.e. check if slot'th entry in slot map of block contains SLOT_UNOCCUPIED)
        Attribute record[numAttrs];
        recBuffer.getRecord(record, slot);

        if (slotMap[slot] == SLOT_UNOCCUPIED)
        {
            // increment slot and continue to the next record slot
            slot++;
            continue;
        }
        // compare record's attribute value to the the given attrVal as below:
        /*
            firstly get the attribute offset for the attrName attribute
            from the attribute cache entry of the relation using
            AttrCacheTable::getAttrCatEntry()
        */
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
        /* use the attribute offset to get the value of the attribute from
           current record */
        Attribute recordAttrVal = record[attrCatEntry.offset];
        int cmpVal; // will store the difference between the attributes
        // set cmpVal using compareAttrs()
        cmpVal = compareAttrs(recordAttrVal, attrVal, attrCatEntry.attrType);

        /* Next task is to check whether this record satisfies the given condition.
           It is determined based on the output of previous comparison and
           the op value received.
           The following code sets the cond variable if the condition is satisfied.
        */
        if (
            (op == NE && cmpVal != 0) || // if op is "not equal to"
            (op == LT && cmpVal < 0) ||  // if op is "less than"
            (op == LE && cmpVal <= 0) || // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) || // if op is "equal to"
            (op == GT && cmpVal > 0) ||  // if op is "greater than"
            (op == GE && cmpVal >= 0)    // if op is "greater than or equal to"
        )
        {
            /*
            set the search index in the relation cache as
            the record id of the record that satisfies the given condition
            (use RelCacheTable::setSearchIndex function)
            */
            prevRecId = RecId{block, slot};
            RelCacheTable::setSearchIndex(relId, &prevRecId);
            return RecId{block, slot};
        }

        slot++;
    }

    // no record in the relation with Id relid satisfies the given condition
    return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName; // set newRelationName with newName
    strcpy(newRelationName.sVal, newName);

    // search the relation catalog for an entry with "RelName" = newRelationName
    // TODO : Get this dynamically
    RecId recId = linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, newRelationName, EQ);

    // If relation with name newName already exists (result of linearSearch
    //                                               is not {-1, -1})
    //    return E_RELEXIST;

    if (recId.block != -1 && recId.slot != -1)
    {
        return E_RELEXIST;
    }

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName; // set oldRelationName with oldName

    strcpy(oldRelationName.sVal, oldName);
    // search the relation catalog for an entry with "RelName" = oldRelationName
    recId = linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, oldRelationName, EQ);

    // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
    //    return E_RELNOTEXIST;
    if (recId.block == -1 && recId.slot == -1)
    {
        return E_RELNOTEXIST;
    }

    /* get the relation catalog record of the relation to rename using a RecBuffer
       on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
    */
    RecBuffer recBuffer(RELCAT_BLOCK);
    HeadInfo head;
    recBuffer.getHeader(&head);
    Attribute record[head.numAttrs];
    recBuffer.getRecord(record, recId.slot);

    /* update the relation name attribute in the record with newName.
       (use RELCAT_REL_NAME_INDEX) */
    // set back the record value using RecBuffer.setRecord
    strcpy(record[RELCAT_REL_NAME_INDEX].sVal, newName);
    recBuffer.setRecord(record, recId.slot);

    /*
    update all the attribute catalog entries in the attribute catalog corresponding
    to the relation with relation name oldName to the relation name newName
    */

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    // for i = 0 to numberOfAttributes :
    //     linearSearch on the attribute catalog for relName = oldRelationName
    //     get the record using RecBuffer.getRecord
    //
    //     update the relName field in the record to newName
    //     set back the record using RecBuffer.setRecord
    // TODO : Get this dynamically
    int numAttrs = record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    for (int i = 0; i < numAttrs; i++)
    {
        recId = linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);
        RecBuffer recBuffer(recId.block);
        recBuffer.getRecord(record, recId.slot);
        strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, newName);
        recBuffer.setRecord(record, recId.slot);
    }

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr; // set relNameAttr to relName
    strcpy(relNameAttr.sVal, relName);

    // Search for the relation with name relName in relation catalog using linearSearch()
    // If relation with name relName does not exist (search returns {-1,-1})
    //    return E_RELNOTEXIST;
    RecId recId = linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, relNameAttr, EQ);

    if (recId.block == -1 && recId.slot == -1)
    {
        return E_RELNOTEXIST;
    }

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    /* declare variable attrToRenameRecId used to store the attr-cat recId
    of the attribute to rename */
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    /* iterate over all Attribute Catalog Entry record corresponding to the
       relation to find the required attribute */
    while (true)
    {
        // linear search on the attribute catalog for RelName = relNameAttr
        // get the record using RecBuffer.getRecord
        // if the attribute name is oldName, set attrToRenameRecId to block and slot of this record

        RecId recId = linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;
        if (recId.block == -1 && recId.slot == -1)
        {
            break;
        }

        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */
        RecBuffer recBuffer(recId.block);
        recBuffer.getRecord(attrCatEntryRecord, recId.slot);

        // if attrCatEntryRecord.attrName = oldName
        //     attrToRenameRecId = block and slot of this record
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0)
        {
            attrToRenameRecId = recId;
            break;
        }

        // if attrCatEntryRecord.attrName = newName
        //     return E_ATTREXIST;
        if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
        {
            return E_ATTREXIST;
        }
    }

    // if attrToRenameRecId == {-1, -1}
    //     return E_ATTRNOTEXIST;
    if (attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1)
    {
        return E_ATTRNOTEXIST;
    }

    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    //   update the AttrName of the record with newName
    //   set back the record with RecBuffer.setRecord
    RecBuffer recBuffer(attrToRenameRecId.block);
    recBuffer.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);
    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
    recBuffer.setRecord(attrCatEntryRecord, attrToRenameRecId.slot);
    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record)
{
    RelCatEntry relCatBuf;
    int ret = RelCacheTable::getRelCatEntry(relId, &relCatBuf);

    if (ret != SUCCESS)
    {
        return ret;
    }

    int blockNum = relCatBuf.firstBlk;

    RecId recId = {-1, -1};

    int numSlots = relCatBuf.numSlotsPerBlk;
    int numAttrs = relCatBuf.numAttrs;

    int prevBlockNum = -1;

    while (blockNum != -1)
    {
        RecBuffer currentBlock(blockNum);

        HeadInfo currentHeader;
        currentBlock.getHeader(&currentHeader);

        unsigned char slotMap[numSlots];
        currentBlock.getSlotMap(slotMap);

        int freeSlot = -1;
        for (int i = 0; i < numSlots; i++)
        {
            if (slotMap[i] == SLOT_UNOCCUPIED)
            {
                freeSlot = i;
                break;
            }
        }

        if (freeSlot != -1)
        {
            recId.block = blockNum;
            recId.slot = freeSlot;
            break;
        }

        prevBlockNum = blockNum;
        blockNum = currentHeader.rblock;
    }

    if (recId.block == -1 || recId.slot == -1)
    {
        if (relId == RELCAT_RELID)
        {
            return E_MAXRELATIONS;
        }

        RecBuffer newBlock;

        int newBlockNum = newBlock.getBlockNum();

        if (newBlockNum == E_DISKFULL)
        {
            return E_DISKFULL;
        }

        recId.block = newBlockNum;
        recId.slot = 0;

        HeadInfo newBlockHeader;
        newBlock.getHeader(&newBlockHeader);
        newBlockHeader.lblock = prevBlockNum;
        newBlockHeader.numAttrs = numAttrs;
        newBlockHeader.numSlots = numSlots;
        newBlock.setHeader(&newBlockHeader);

        unsigned char newBlockSlotMap[numSlots];
        newBlock.getSlotMap(newBlockSlotMap);
        for (int i = 0; i < numSlots; i++)
            newBlockSlotMap[i] = SLOT_UNOCCUPIED;
        newBlock.setSlotMap(newBlockSlotMap);

        if (prevBlockNum != -1)
        {
            RecBuffer prevBlock(prevBlockNum);

            HeadInfo prevBlockHeader;
            prevBlock.getHeader(&prevBlockHeader);
            prevBlockHeader.rblock = recId.block;
            prevBlock.setHeader(&prevBlockHeader);
        }
        else
        {
            relCatBuf.firstBlk = recId.block;
            RelCacheTable::setRelCatEntry(relId, &relCatBuf);
        }
        relCatBuf.lastBlk = recId.block;
    }

    RecBuffer blockToInsert(recId.block);
    blockToInsert.setRecord(record, recId.slot);

    unsigned char slotMapToInsert[numSlots];
    blockToInsert.getSlotMap(slotMapToInsert);
    slotMapToInsert[recId.slot] = SLOT_OCCUPIED;
    blockToInsert.setSlotMap(slotMapToInsert);

    HeadInfo headerToInsert;
    blockToInsert.getHeader(&headerToInsert);
    headerToInsert.numEntries++;
    blockToInsert.setHeader(&headerToInsert);

    relCatBuf.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatBuf);

    int flag = SUCCESS;
    for (int attrOffset = 0; attrOffset < numAttrs; attrOffset++)
    {
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId, attrOffset, &attrCatBuf);
        if (attrCatBuf.rootBlock == -1)
        {
            continue;
        }

        int ret = BPlusTree::bPlusInsert(relId, attrCatBuf.attrName, record[attrOffset], recId);
        if (ret == E_DISKFULL)
        {
            flag = E_INDEX_BLOCKS_RELEASED;
            BPlusTree::bPlusDestroy(attrCatBuf.rootBlock);
        }
    }

    return flag;
}

/*
NOTE: This function will copy the result of the search to the `record` argument.
      The caller should ensure that space is allocated for `record` array
      based on the number of attributes in the relation.
*/
int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    // Declare a variable called recid to store the searched record
    RecId recId;

    /* get the attribute catalog entry from the attribute cache corresponding
    to the relation with Id=relid and with attribute_name=attrName  */
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // if this call returns an error, return the appropriate error code
    if (ret != SUCCESS)
    {
        return ret;
    }

    // get rootBlock from the attribute catalog entry
    int rootBlock = attrCatEntry.rootBlock;
    /* if Index does not exist for the attribute (check rootBlock == -1) */
    if (rootBlock == -1)
    {

        /* search for the record id (recid) corresponding to the attribute with
           attribute name attrName, with value attrval and satisfying the
           condition op using linearSearch()
        */
        recId = linearSearch(relId, attrName, attrVal, op);
        // resetting the search index will be handled by linear search
    }

    /* else */
    else
    {
        // (index exists for the attribute)

        /* search for the record id (recid) correspoding to the attribute with
        attribute name attrName and with value attrval and satisfying the
        condition op using BPlusTree::bPlusSearch() */
        recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
    }

    // if there's no record satisfying the given condition (recId = {-1, -1})
    //     return E_NOTFOUND;
    if (recId.block == -1 && recId.slot == -1)
    {
        return E_NOTFOUND;
    }

    /* Copy the record with record id (recId) to the record buffer (record).
       For this, instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE])
{
    if (
        strcmp(relName, (char *)RELCAT_RELNAME) == 0 ||
        strcmp(relName, (char *)ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttribute;
    strcpy(relNameAttribute.sVal, relName);

    RecId recId = linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, relNameAttribute, EQ);

    if (recId.block == -1 || recId.slot == -1)
    {
        return E_RELNOTEXIST;
    }

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];

    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(relCatEntryRecord, recId.slot);

    int currentBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;

    while (currentBlock != -1)
    {
        RecBuffer currentBlockBuffer(currentBlock);
        HeadInfo currentBlockHeader;
        currentBlockBuffer.getHeader(&currentBlockHeader);

        int nextBlock = currentBlockHeader.rblock;

        currentBlockBuffer.releaseBlock();
        currentBlock = nextBlock;
    }

    int numAttrsDeleted = 0;
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    while (true)
    {
        RecId attrCatRecId = linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, relNameAttribute, EQ);

        if (attrCatRecId.slot == -1 || attrCatRecId.block == -1)
        {
            break;
        }

        numAttrsDeleted++;

        RecBuffer currentBlock(attrCatRecId.block);

        HeadInfo currentBlockHeader;
        currentBlock.getHeader(&currentBlockHeader);

        Attribute record[ATTRCAT_NO_ATTRS];
        currentBlock.getRecord(record, attrCatRecId.slot);

        int rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;

        unsigned char slotMap[currentBlockHeader.numSlots];

        currentBlock.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        currentBlock.setSlotMap(slotMap);

        currentBlockHeader.numEntries--;
        currentBlock.setHeader(&currentBlockHeader);

        if (currentBlockHeader.numEntries == 0)
        {
            int leftBlock = currentBlockHeader.lblock;
            int rightBlock = currentBlockHeader.rblock;

            if (leftBlock != -1)
            {
                RecBuffer prevBlock(leftBlock);
                HeadInfo prevBlockHeader;

                prevBlock.getHeader(&prevBlockHeader);
                prevBlockHeader.rblock = rightBlock;
                prevBlock.setHeader(&prevBlockHeader);
            }

            if (rightBlock != -1)
            {
                RecBuffer nextBlock(rightBlock);
                HeadInfo nextBlockHeader;

                nextBlock.getHeader(&nextBlockHeader);
                nextBlockHeader.lblock = leftBlock;
                nextBlock.setHeader(&nextBlockHeader);
            }

            currentBlock.releaseBlock();
        }

        // condition to handle b+ trees
        if (rootBlock != -1)
        {
            BPlusTree::bPlusDestroy(rootBlock);
        }
    }

    HeadInfo relCatHeader;
    recBuffer.getHeader(&relCatHeader);

    unsigned char recSlotMap[relCatHeader.numSlots];

    recBuffer.getSlotMap(recSlotMap);
    recSlotMap[recId.slot] = SLOT_UNOCCUPIED;
    recBuffer.setSlotMap(recSlotMap);

    relCatHeader.numEntries--;
    recBuffer.setHeader(&relCatHeader);

    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatBuf);
    relCatBuf.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatBuf);

    RelCatEntry attrCatBuf;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatBuf);
    attrCatBuf.numRecs -= numAttrsDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatBuf);

    return SUCCESS;
}

/*
NOTE: the caller is expected to allocate space for the argument `record` based
      on the size of the relation. This function will only copy the result of
      the projection onto the array pointed to by the argument.
*/
int BlockAccess::project(int relId, Attribute *record)
{
    // get the previous search index of the relation relId from the relation
    // cache (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    // declare block and slot which will be used to store the record id of the
    // slot we need to check.
    int block, slot;

    /* if the current search index record is invalid(i.e. = {-1, -1})
       (this only happens when the caller reset the search index)
    */
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (new project operation. start from beginning)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);

        // block = first record block of the relation
        // slot = 0
        block = relCatEntry.firstBlk;
        slot = 0;
    }
    else
    {
        // (a project/search operation is already in progress)

        // block = previous search index's block
        // slot = previous search index's slot + 1
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    // The following code finds the next record of the relation
    /* Start from the record id (block, slot) and iterate over the remaining
       records of the relation */
    while (block != -1)
    {
        // create a RecBuffer object for block (using appropriate constructor!)
        RecBuffer recBlock(block);

        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function
        struct HeadInfo header;
        recBlock.getHeader(&header);
        unsigned char slotMap[header.numSlots];
        recBlock.getSlotMap(slotMap);

        if (slot >= header.numSlots)
        {
            // (no more slots in this block)
            // update block = right block of block
            // update slot = 0
            // (NOTE: if this is the last block, rblock would be -1. this would
            //        set block = -1 and fail the loop condition )
            block = header.rblock;
            slot = 0;
            continue; // continue to the beginning of this while loop
        }
        else if (slotMap[slot] == SLOT_UNOCCUPIED)
        {
            // (slot is free)
            // increment slot and continue to the next record slot
            slot++;
            continue;
        }
        else
        {
            // (the next occupied slot / record has been found)
            // declare nextRecId to store the RecId of the record found
            break;
        }
    }

    if (block == -1)
    {
        // (a record was not found. all records exhausted)
        return E_NOTFOUND;
    }

    // declare nextRecId to store the RecId of the record found
    RecId nextRecId{block, slot};

    // set the search index to nextRecId using RelCacheTable::setSearchIndex
    RelCacheTable::setSearchIndex(relId, &nextRecId);

    /* Copy the record with record id (nextRecId) to the record buffer (record)
       For this Instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
    RecBuffer recBuffer(nextRecId.block);
    recBuffer.getRecord(record, nextRecId.slot);

    return SUCCESS;
}