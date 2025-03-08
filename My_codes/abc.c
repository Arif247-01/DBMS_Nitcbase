#include "BlockAccess.h"
#include <cstdlib>
#include <cstring>


RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    // get the previous search index of the relation relId from the relation cache
    // (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    // let block and slot denote the record id of the record being currently checked
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

        // block = first record block of the relation
        block = relCatEntry.firstBlk;
        // slot = 0
        slot = 0;
    }
    else
    {
        // (there is a hit from previous search; search should start from
        // the record next to the search index record)

        // block = search index's block
        block = prevRecId.block;
        // slot = search index's slot + 1
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
        
        // get header of the block using RecBuffer::getHeader() function
        HeadInfo head;
        recBuffer.getHeader(&head);
        // get the record with id (block, slot) using RecBuffer::getRecord()
        Attribute rec[head.numAttrs];
        recBuffer.getRecord(rec, slot);
        // get slot map of the block using RecBuffer::getSlotMap() function
        unsigned char slotMap[head.numSlots];
        recBuffer.getSlotMap(slotMap);
        // If slot >= the number of slots per block(i.e. no more slots in this block)
        if(slot >= head.numSlots)
        {
            // update block = right block of block
            block = head.rblock;
            // update slot = 0
            slot = 0;
            continue;  // continue to the beginning of this while loop
        }

        // if slot is free skip the loop
        // (i.e. check if slot'th entry in slot map of block contains SLOT_UNOCCUPIED)
        if(slotMap[slot] == SLOT_UNOCCUPIED)
        {
            // increment slot and continue to the next record slot
            slot++; //doubt
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
        int offset = attrCatEntry.offset;
        /* use the attribute offset to get the value of the attribute from
           current record */
        Attribute attrValue;
        attrValue = rec[offset];
        int attrType = attrCatEntry.attrType;
        

        int cmpVal;  // will store the difference between the attributes
        // set cmpVal using compareAttrs()
        cmpVal = compareAttrs(attrValue, attrVal, attrType);//not done

        /* Next task is to check whether this record satisfies the given condition.
           It is determined based on the output of previous comparison and
           the op value received.
           The following code sets the cond variable if the condition is satisfied.
        */
        if (
            (op == NE && cmpVal != 0) ||    // if op is "not equal to"
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
        ) {
            /*
            set the search index in the relation cache as
            the record id of the record that satisfies the given condition
            (use RelCacheTable::setSearchIndex function)
            */
            RecId x;
            x.block = block;
            x.slot = slot;
            RelCacheTable::setSearchIndex(relId, &x);//doubt

            return RecId{block, slot};
        }

        slot++;
    }

    // no record in the relation with Id relid satisfies the given condition
    return RecId{-1, -1};
}


int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName; // set newRelationName with newName
    strcpy(newRelationName.sVal,newName);

    // search the relation catalog for an entry with "RelName" = newRelationName
    RecId z;
    z = linearSearch(RELCAT_RELID, (char *)"RelName", newRelationName, EQ);

    // If relation with name newName already exists (result of linearSearch
    //                                               is not {-1, -1})
    //    return E_RELEXIST;
    
    if(z.block!=-1 && z.slot!=-1){
      return E_RELEXIST;
    }

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName; // set oldRelationName with oldName
    strcpy(oldRelationName.sVal, oldName);
    

    // search the relation catalog for an entry with "RelName" = oldRelationName
    z = linearSearch(RELCAT_RELID, (char *)"RelName", oldRelationName, EQ);

    // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
    //    return E_RELNOTEXIST;
    if(z.block==-1 && z.slot==-1){
      return E_RELNOTEXIST;
    }

    /* get the relation catalog record of the relation to rename using a RecBuffer
       on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
    */
    RecBuffer recBuffer(z.block);//z.block
    Attribute rec[6];
    recBuffer.getRecord(rec, z.slot);
    int numAttrs = rec[1].nVal;
    /* update the relation name attribute in the record with newName.
       (use RELCAT_REL_NAME_INDEX) */
    strcpy(rec[RELCAT_REL_NAME_INDEX].sVal,newName);
    // set back the record value using RecBuffer.setRecord
    recBuffer.setRecord(rec, z.slot);

    /*
    update all the attribute catalog entries in the attribute catalog corresponding
    to the relation with relation name oldName to the relation name newName
    */

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    //for i = 0 to numberOfAttributes :
    //    linearSearch on the attribute catalog for relName = oldRelationName
    //    get the record using RecBuffer.getRecord
    //
    //    update the relName field in the record to newName
    //    set back the record using RecBuffer.setRecord

    for(int i=0; i< numAttrs;i++){
      z = linearSearch(ATTRCAT_RELID, (char *)"RelName", oldRelationName, EQ);
      RecBuffer rec1Buffer(z.block);
      rec1Buffer.getRecord(rec, z.slot);
      strcpy(rec[ATTRCAT_REL_NAME_INDEX].sVal,newName);
      rec1Buffer.setRecord(rec, z.slot);
    }

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;    // set relNameAttr to relName
    strcpy(relNameAttr.sVal, relName);

    // Search for the relation with name relName in relation catalog using linearSearch()
    // If relation with name relName does not exist (search returns {-1,-1})
    //    return E_RELNOTEXIST;
    
    RecId z;
    z = linearSearch(RELCAT_RELID, (char *)"RelName", relNameAttr, EQ);
    if(z.block==-1 && z.slot==-1){
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
    while (true) {
        // linear search on the attribute catalog for RelName = relNameAttr
        RecId x = linearSearch(ATTRCAT_RELID, (char *)"RelName", relNameAttr, EQ);

        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;
        if(x.block==-1 && x.slot==-1) break;

        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */
        RecBuffer rec2Buffer(x.block);
        rec2Buffer.getRecord(attrCatEntryRecord, x.slot);

        // if attrCatEntryRecord.attrName = oldName
        //     attrToRenameRecId = block and slot of this record
        if(strcmp(attrCatEntryRecord[1].sVal, oldName)==0){
          attrToRenameRecId.block = x.block;
          attrToRenameRecId.slot = x.slot;
        }

        // if attrCatEntryRecord.attrName = newName
        //     return E_ATTREXIST;
        if(strcmp(attrCatEntryRecord[1].sVal, newName)==0){
          return E_ATTREXIST;
        }
    }

    // if attrToRenameRecId == {-1, -1}
    //     return E_ATTRNOTEXIST;
    if(attrToRenameRecId.block==-1 && attrToRenameRecId.slot==-1){
      return E_ATTRNOTEXIST;
    }


    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    RecBuffer rec3Buffer(attrToRenameRecId.block);
    Attribute rec3[6];
    rec3Buffer.getRecord(rec3, attrToRenameRecId.slot);
    //   update the AttrName of the record with newName
    strcpy(rec3[1].sVal, newName);
    //   set back the record with RecBuffer.setRecord
    rec3Buffer.setRecord(rec3, attrToRenameRecId.slot);

    return SUCCESS;
}



int BlockAccess::insert(int relId, Attribute *record) {
    // get the relation catalog entry from relation cache
    // ( use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int blockNum = relCatEntry.firstBlk;/* first record block of the relation (from the rel-cat entry)*/

    // rec_id will be used to store where the new record will be inserted
    RecId rec_id = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk;/* number of slots per record block */
    int numOfAttributes = relCatEntry.numAttrs;/* number of attributes of the relation */

    int prevBlockNum = -1;/* block number of the last element in the linked list = -1 */

    /*
        Traversing the linked list of existing record blocks of the relation
        until a free slot is found OR
        until the end of the list is reached
    */
    while (blockNum != -1) {
        // create a RecBuffer object for blockNum (using appropriate constructor!)
        RecBuffer recBuffer(blockNum);

        // get header of block(blockNum) using RecBuffer::getHeader() function
        HeadInfo head;
        recBuffer.getHeader(&head);

        // get slot map of block(blockNum) using RecBuffer::getSlotMap() function
        unsigned char slotMap[head.numSlots];
        recBuffer.getSlotMap(slotMap);

        // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
        int flag = 0;
        for(int i=0; i<head.numSlots; i++){
          if((int)slotMap[i] == SLOT_UNOCCUPIED){
            rec_id.slot = i;
            rec_id.block = blockNum;
            flag = 1;
            break;
          }
        }
        // (Free slot can be found by iterating over the slot map of the block)
        /* slot map stores SLOT_UNOCCUPIED if slot is free and
           SLOT_OCCUPIED if slot is occupied) */

        /* if a free slot is found, set rec_id and discontinue the traversal
           of the linked list of record blocks (break from the loop) */
           if(flag==1) break;

        /* otherwise, continue to check the next block by updating the
           block numbers as follows:
              update prevBlockNum = blockNum
              update blockNum = header.rblock (next element in the linked
                                               list of record blocks)
        */
        prevBlockNum = blockNum;
        blockNum = head.rblock;
    }

    //  if no free slot is found in existing record blocks (rec_id = {-1, -1})
    if(rec_id.block==-1 && rec_id.slot==-1)
    {
        // if relation is RELCAT, do not allocate any more blocks
        //     return E_MAXRELATIONS;
        if(relId == 0) return E_MAXRELATIONS;

        // Otherwise,
        // get a new record block (using the appropriate RecBuffer constructor!)
        RecBuffer recbuf;
        // get the block number of the newly allocated block
        // (use BlockBuffer::getBlockNum() function)
        // let ret be the return value of getBlockNum() function call
        int ret = recbuf.getBlockNum();
        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }

        // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0
        rec_id.block = ret;
        rec_id.slot = 0;

        /*
            set the header of the new record block such that it links with
            existing record blocks of the relation
            set the block's header as follows:
            blockType: REC, pblock: -1
            lblock
                  = -1 (if linked list of existing record blocks was empty
                         i.e this is the first insertion into the relation)
                  = prevBlockNum (otherwise),
            rblock: -1, numEntries: 0,
            numSlots: numOfSlots, numAttrs: numOfAttributes
            (use BlockBuffer::setHeader() function)
        */
        HeadInfo head;
        head.blockType = REC;
        head.pblock = -1;
        if(relCatEntry.firstBlk==-1) head.lblock = -1;
        else head.lblock = prevBlockNum;
        head.rblock = -1;
        head.numEntries = 0;
        head.numSlots = numOfSlots;
        head.numAttrs = numOfAttributes;
        
        recbuf.setHeader(&head);

        /*
            set block's slot map with all slots marked as free
            (i.e. store SLOT_UNOCCUPIED for all the entries)
            (use RecBuffer::setSlotMap() function)
        */
        unsigned char smap[numOfSlots];
        for(int i=0; i<numOfSlots; i++){
          smap[i] = (unsigned char)SLOT_UNOCCUPIED;
        }
        recbuf.setSlotMap(smap);

        // if prevBlockNum != -1
        if(prevBlockNum != -1)
        {
            // create a RecBuffer object for prevBlockNum
            RecBuffer buffer(prevBlockNum);
            // get the header of the block prevBlockNum and
            HeadInfo head1;
            buffer.getHeader(&head1);
            // update the rblock field of the header to the new block
            head1.rblock = rec_id.block;
            // number i.e. rec_id.block
            // (use BlockBuffer::setHeader() function)
            buffer.setHeader(&head1);
        }
        // else
        else
        {
            // update first block field in the relation catalog entry to the
            relCatEntry.firstBlk = rec_id.block;
            // new block (using RelCacheTable::setRelCatEntry() function)
            RelCacheTable::setRelCatEntry(relId, &relCatEntry);
        }

        // update last block field in the relation catalog entry to the
        // new block (using RelCacheTable::setRelCatEntry() function)
        relCatEntry.lastBlk = rec_id.block;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    // create a RecBuffer object for rec_id.block
    RecBuffer buffer1(rec_id.block);
    // insert the record into rec_id'th slot using RecBuffer.setRecord())
    buffer1.setRecord(record , rec_id.slot);

    /* update the slot map of the block by marking entry of the slot to
       which record was inserted as occupied) */
    // (ie store SLOT_OCCUPIED in free_slot'th entry of slot map)
    // (use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions)
    unsigned char map[numOfSlots];
    buffer1.getSlotMap(map);
    map[rec_id.slot] = (unsigned char)SLOT_OCCUPIED;
    buffer1.setSlotMap(map);

    // increment the numEntries field in the header of the block to
    // which record was inserted
    // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
    HeadInfo headx;
    buffer1.getHeader(&headx);
    headx.numEntries++;
    buffer1.setHeader(&headx);
    

    // Increment the number of records field in the relation cache entry for
    // the relation. (use RelCacheTable::setRelCatEntry function)
    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    return SUCCESS;
}

/*
NOTE: This function will copy the result of the search to the `record` argument.
      The caller should ensure that space is allocated for `record` array
      based on the number of attributes in the relation.
*/
int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
  // Declare a variable called recid to store the searched record
  RecId recId;

  /* search for the record id (recid) corresponding to the attribute with
  attribute name attrName, with value attrval and satisfying the condition op
  using linearSearch() */
  recId = linearSearch(relId, attrName, attrVal, op);

  // if there's no record satisfying the given condition (recId = {-1, -1})
  //    return E_NOTFOUND;
  if(recId.slot==-1 && recId.block==-1) return E_NOTFOUND;

  /* Copy the record with record id (recId) to the record buffer (record)
     For this Instantiate a RecBuffer class object using recId and
     call the appropriate method to fetch the record
  */
  RecBuffer recBuffer(recId.block);
  recBuffer.getRecord(record, recId.slot);
  return SUCCESS;
}


int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
  // if the relation to delete is either Relation Catalog or Attribute Catalog,
  //     return E_NOTPERMITTED
      // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
      // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)
  if(strcmp(RELCAT_RELNAME, relName)==0 || strcmp(ATTRCAT_RELNAME, relName)==0) return  E_NOTPERMITTED; 

  /* reset the searchIndex of the relation catalog using
     RelCacheTable::resetSearchIndex() */
     RelCacheTable::resetSearchIndex(RELCAT_RELID);

  Attribute relNameAttr; // (stores relName as type union Attribute)
  // assign relNameAttr.sVal = relName
  strcpy(relNameAttr.sVal, relName);

  //  linearSearch on the relation catalog for RelName = relNameAttr
  RecId recId;
  recId = linearSearch(RELCAT_RELID, (char *)"RelName", relNameAttr, EQ);

  // if the relation does not exist (linearSearch returned {-1, -1})
  //     return E_RELNOTEXIST
  if(recId.slot==-1 && recId.block==-1) return E_RELNOTEXIST;

  Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
  /* store the relation catalog record corresponding to the relation in
     relCatEntryRecord using RecBuffer.getRecord */
    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(relCatEntryRecord, recId.slot);

  /* get the first record block of the relation (firstBlock) using the
     relation catalog entry record */
    int firstBlock = (int)relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
  /* get the number of attributes corresponding to the relation (numAttrs)
     using the relation catalog entry record */
    int numAttrs = (int)relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

  /*
   Delete all the record blocks of the relation
  */
  // for each record block of the relation:
  //     get block header using BlockBuffer.getHeader
  //     get the next block from the header (rblock)
  //     release the block using BlockBuffer.releaseBlock
  //
  //     Hint: to know if we reached the end, check if nextBlock = -1
  int nextBlock = firstBlock;
  while(nextBlock!=-1){
    BlockBuffer blockBuffer(nextBlock);
    HeadInfo head;
    blockBuffer.getHeader(&head);
    nextBlock = head.rblock;
    blockBuffer.releaseBlock();
  }

  /*** Delete the entry corresponding to the relation from relation catalog ***/
  // Fetch the header of Relcat block
  BlockBuffer buf(4);
  HeadInfo headx;
  buf.getHeader(&headx);

  /* Decrement the numEntries in the header of the block corresponding to the
     relation catalog entry and set it back */
    headx.numEntries--;
    buf.setHeader(&headx);

  /* Get the slotmap in relation catalog, update it by marking the slot as
     free(SLOT_UNOCCUPIED) and set it back. */
    RecBuffer bufx(4);
    unsigned char map[20];
    bufx.getSlotMap(map);
    map[recId.slot] = (unsigned char)SLOT_UNOCCUPIED;  //doubt
    bufx.setSlotMap(map);

  /*** Updating the Relation Cache Table ***/
  /** Update relation catalog record entry (number of records in relation
      catalog is decreased by 1) **/
  // Get the entry corresponding to relation catalog from the relation
  // cache and update the number of records and set it back
  // (using RelCacheTable::setRelCatEntry() function)
  RelCatEntry relCat;
  RelCacheTable::getRelCatEntry(RELCAT_RELID,&relCat);
  relCat.numRecs--;
  RelCacheTable::setRelCatEntry(RELCAT_RELID,&relCat);

  return SUCCESS;
}


