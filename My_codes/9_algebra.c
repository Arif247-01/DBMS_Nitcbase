#include "Algebra.h"

#include <cstring>
#include<cstdio>
#include<cstdlib>


// will return if a string can be parsed as a floating point number
bool isNumber(char *str) {
  int len;
  float ignore;
  /*
    sscanf returns the number of elements read, so if there is no float matching
    the first %f, ret will be 0, else it'll be 1

    %n gets the number of characters read. this scanf sequence will read the
    first float ignoring all the whitespace before and after. and the number of
    characters read that far will be stored in len. if len == strlen(str), then
    the string only contains a float with/without whitespace. else, there's other
    characters.
  */
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}




/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into. (ignore for now)
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/


int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
    // if relName is equal to "RELATIONCAT" or "ATTRIBUTECAT"
    // return E_NOTPERMITTED;
    if(strcmp(relName, "RELATIONCAT")==0 || strcmp(relName, "ATTRIBUTECAT")==0) return E_NOTPERMITTED;

    // get the relation's rel-id using OpenRelTable::getRelId() method
    int relId = OpenRelTable::getRelId(relName);

    // if relation is not open in open relation table, return E_RELNOTOPEN
    // (check if the value returned from getRelId function call = E_RELNOTOPEN)
    if(relId == E_RELNOTOPEN) return E_RELNOTOPEN;
    // get the relation catalog entry from relation cache
    // (use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId , &relCatEntry);

    /* if relCatEntry.numAttrs != numberOfAttributes in relation,
       return E_NATTRMISMATCH */
    if(relCatEntry.numAttrs != nAttrs) return E_NATTRMISMATCH;

    // let recordValues[numberOfAttributes] be an array of type union Attribute
    Attribute recordValues[nAttrs];

    /*
        Converting 2D char array of record values to Attribute array recordValues
     */
    // iterate through 0 to nAttrs-1: (let i be the iterator)
    for(int i=0; i<nAttrs; i++)
    {
        // get the attr-cat entry for the i'th attribute from the attr-cache
        // (use AttrCacheTable::getAttrCatEntry())
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);

        // let type = attrCatEntry.attrType;
        int type = attrCatEntry.attrType;

        if (type == NUMBER)
        {
            // if the char array record[i] can be converted to a number
            // (check this using isNumber() function)
            if (isNumber(record[i]))
            {
                /* convert the char array to numeral and store it
                   at recordValues[i].nVal using atof() */
                   recordValues[i].nVal = atof(record[i]);
            }
            // else
            else
            {
                return E_ATTRTYPEMISMATCH;
            }
        }
        else if (type == STRING)
        {
            // copy record[i] to recordValues[i].sVal
            strcpy(recordValues[i].sVal, record[i]);  //doubt
        }
    }

    // insert the record by calling BlockAccess::insert() function
    int retVal = BlockAccess::insert(relId, recordValues);
    // let retVal denote the return value of insert call

    return retVal;
}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
  // get the srcRel's rel-id (let it be srcRelid), using OpenRelTable::getRelId()
  int srcRelid = OpenRelTable::getRelId(srcRel);
  // if srcRel is not open in open relation table, return E_RELNOTOPEN
  if (srcRelid == E_RELNOTOPEN) return E_RELNOTOPEN;

  // get the attr-cat entry for attr, using AttrCacheTable::getAttrCatEntry()
  // if getAttrcatEntry() call fails return E_ATTRNOTEXIST
  AttrCatEntry attrCatEntry;
  int ret = AttrCacheTable::getAttrCatEntry(srcRelid, attr, &attrCatEntry);

  if(ret == E_ATTRNOTEXIST)
  	return E_ATTRNOTEXIST;


  /*** Convert strVal to an attribute of data type NUMBER or STRING ***/

  Attribute attrVal;
  int type = attrCatEntry.attrType;

  if (type == NUMBER)
  {
      // if the input argument strVal can be converted to a number
      if(isNumber(strVal))
      {
        attrVal.nVal = atof(strVal);// convert strVal to double and store it at attrVal.nVal using atof()
      }
      else
      {
          return E_ATTRTYPEMISMATCH;
      }
  }
  else if (type == STRING)
  {
      // copy strVal to attrVal.sVal
      strcpy(attrVal.sVal, strVal);
  }

  /*** Creating and opening the target relation ***/
  // Prepare arguments for createRel() in the following way:
  // get RelcatEntry of srcRel using RelCacheTable::getRelCatEntry()
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(srcRelid, &relCatEntry);
  int src_nAttrs = relCatEntry.numAttrs/* the no. of attributes present in src relation */ ;


  /* let attr_names[src_nAttrs][ATTR_SIZE] be a 2D array of type char
      (will store the attribute names of rel). */
  char attr_names[src_nAttrs][ATTR_SIZE];
  // let attr_types[src_nAttrs] be an array of type int
  int attr_types[src_nAttrs];

  /*iterate through 0 to src_nAttrs-1 :
      get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
      fill the attr_names, attr_types arrays that we declared with the entries
      of corresponding attributes
  */
  for(int i=0; i<=src_nAttrs-1; i++){
    AttrCatEntry attrCatEntryx;
    AttrCacheTable::getAttrCatEntry(srcRelid, i, &attrCatEntryx);
    attr_types[i] = attrCatEntryx.attrType;
    strcpy(attr_names[i], attrCatEntryx.attrName);
  }


  /* Create the relation for target relation by calling Schema::createRel()
     by providing appropriate arguments */
     int kx = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
  // if the createRel returns an error code, then return that value.
  if(kx!=SUCCESS) return kx;

  /* Open the newly created target relation by calling OpenRelTable::openRel()
     method and store the target relid */
  int new_relid = OpenRelTable::openRel(targetRel);
  /* If opening fails, delete the target relation by calling Schema::deleteRel()
     and return the error value returned from openRel() */
  if(new_relid == E_CACHEFULL){
    Schema::deleteRel(targetRel);
    return new_relid;
  } 

  /*** Selecting and inserting records into the target relation ***/
  /* Before calling the search function, reset the search to start from the
     first using RelCacheTable::resetSearchIndex() */
  //RelCacheTable::resetSearchIndex(new_relid);

  Attribute record[src_nAttrs];

  /*
      The BlockAccess::search() function can either do a linearSearch or
      a B+ tree search. Hence, reset the search index of the relation in the
      relation cache using RelCacheTable::resetSearchIndex().
      Also, reset the search index in the attribute cache for the select
      condition attribute with name given by the argument `attr`. Use
      AttrCacheTable::resetSearchIndex().
      Both these calls are necessary to ensure that search begins from the
      first record.
  */
  RelCacheTable::resetSearchIndex(srcRelid/* fill arguments */);
  //AttrCacheTable::resetSearchIndex(/* fill arguments */);   omitted for now...

  // read every record that satisfies the condition by repeatedly calling
  // BlockAccess::search() until there are no more records to be read

  while (BlockAccess::search(srcRelid, record, attr, attrVal, op)==SUCCESS/* BlockAccess::search() returns success */) {

      // ret = BlockAccess::insert(targetRelId, record);
      int ret = BlockAccess::insert(new_relid, record);

      // if (insert fails) {
      //     close the targetrel(by calling Schema::closeRel(targetrel))
      //     delete targetrel (by calling Schema::deleteRel(targetrel))
      //     return ret;
      // }
      if(ret!=SUCCESS){
        Schema::closeRel(targetRel);
        Schema::deleteRel(targetRel);
        return ret;
      }
  }

  // Close the targetRel by calling closeRel() method of schema layer
  Schema::closeRel(targetRel);

  // return SUCCESS.
  return SUCCESS;
}



int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {

  int srcRelId = OpenRelTable::getRelId(srcRel);/*srcRel's rel-id (use OpenRelTable::getRelId() function)*/

  // if srcRel is not open in open relation table, return E_RELNOTOPEN
  if (srcRelId == E_RELNOTOPEN) return E_RELNOTOPEN;

  // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

  // get the no. of attributes present in relation from the fetched RelCatEntry.
  int numAttrs = relCatEntry.numAttrs;

  // attrNames and attrTypes will be used to store the attribute names
  // and types of the source relation respectively
  char attrNames[numAttrs][ATTR_SIZE];
  int attrTypes[numAttrs];

  /*iterate through every attribute of the source relation :
      - get the AttributeCat entry of the attribute with offset.
        (using AttrCacheTable::getAttrCatEntry())
      - fill the arrays `attrNames` and `attrTypes` that we declared earlier
        with the data about each attribute
  */
  for(int i=0; i<numAttrs; i++){
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry( srcRelId, i, &attrCatEntry);
    strcpy(attrNames[i], attrCatEntry.attrName);
    attrTypes[i] = attrCatEntry.attrType;
  }


  /*** Creating and opening the target relation ***/

  // Create a relation for target relation by calling Schema::createRel()
  int z = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);

  // if the createRel returns an error code, then return that value.
  if(z!=SUCCESS) return z;

  // Open the newly created target relation by calling OpenRelTable::openRel()
  // and get the target relid
  int target_relid = OpenRelTable::openRel(targetRel);
  if(target_relid<0 || target_relid>=MAX_OPEN){
    Schema::deleteRel(targetRel);
    return target_relid;
  }

  // If opening fails, delete the target relation by calling Schema::deleteRel() of
  // return the error value returned from openRel().


  /*** Inserting projected records into the target relation ***/

  // Take care to reset the searchIndex before calling the project function
  // using RelCacheTable::resetSearchIndex()
  RelCacheTable::resetSearchIndex(srcRelId);

  Attribute record[numAttrs];


  while (BlockAccess::project(srcRelId, record) == SUCCESS/* BlockAccess::project(srcRelId, record) returns SUCCESS */)
  {
      // record will contain the next record

      // ret = BlockAccess::insert(targetRelId, proj_record);
      int ret = BlockAccess::insert(target_relid, record);

      if (ret!=SUCCESS/* insert fails */) {
          // close the targetrel by calling Schema::closeRel()
          // delete targetrel by calling Schema::deleteRel()
          // return ret;
          Schema::closeRel(targetRel);
        Schema::deleteRel(targetRel);
        return ret;
      }
  }

  // Close the targetRel by calling Schema::closeRel()
  Schema::closeRel(targetRel);

  // return SUCCESS.
  return SUCCESS;
}


int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {

  int srcRelId = OpenRelTable::getRelId(srcRel);/*srcRel's rel-id (use OpenRelTable::getRelId() function)*/

  // if srcRel is not open in open relation table, return E_RELNOTOPEN
  if (srcRelId == E_RELNOTOPEN) return E_RELNOTOPEN;

  // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

  // get the no. of attributes present in relation from the fetched RelCatEntry.
  int src_nAttrs = relCatEntry.numAttrs;

  // declare attr_offset[tar_nAttrs] an array of type int.
  int attr_offset[tar_nAttrs];
  // where i-th entry will store the offset in a record of srcRel for the
  // i-th attribute in the target relation.

  // let attr_types[tar_nAttrs] be an array of type int.
  int attr_types[tar_nAttrs];
  // where i-th entry will store the type of the i-th attribute in the
  // target relation.


  /*** Checking if attributes of target are present in the source relation
       and storing its offsets and types ***/

  /*iterate through 0 to tar_nAttrs-1 :
      - get the attribute catalog entry of the attribute with name tar_attrs[i].
      - if the attribute is not found return E_ATTRNOTEXIST
      - fill the attr_offset, attr_types arrays of target relation from the
        corresponding attribute catalog entries of source relation
  */
  for(int i=0; i<tar_nAttrs; i++)
  {
    AttrCatEntry attrCatEntry;
    int k = AttrCacheTable::getAttrCatEntry( srcRelId, tar_Attrs[i], &attrCatEntry);
    if(k!=SUCCESS)
    {
      return k;
    }
    attr_offset[i] = attrCatEntry.offset;
    attr_types[i] = attrCatEntry.attrType;
  }


  /*** Creating and opening the target relation ***/

  // Create a relation for target relation by calling Schema::createRel()
  int z = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);

  // if the createRel returns an error code, then return that value.
  if(z!=SUCCESS) return z;

  // Open the newly created target relation by calling OpenRelTable::openRel()
  // and get the target relid

  // If opening fails, delete the target relation by calling Schema::deleteRel()
  // and return the error value from openRel()

  int target_relid = OpenRelTable::openRel(targetRel);
  if(target_relid<0 || target_relid>=MAX_OPEN)
  {
    Schema::deleteRel(targetRel);
    return target_relid;
  }

  /*** Inserting projected records into the target relation ***/

  // Take care to reset the searchIndex before calling the project function
  // using RelCacheTable::resetSearchIndex()
  RelCacheTable::resetSearchIndex(srcRelId);

  Attribute record[src_nAttrs];

  while (BlockAccess::project(srcRelId, record) == SUCCESS/* BlockAccess::project(srcRelId, record) returns SUCCESS */) {
      // the variable `record` will contain the next record

      Attribute proj_record[tar_nAttrs];

      //iterate through 0 to tar_attrs-1:
      //    proj_record[attr_iter] = record[attr_offset[attr_iter]]
      for(int i=0; i<tar_nAttrs; i++){
        proj_record[i] = record[attr_offset[i]];
      }

      // ret = BlockAccess::insert(targetRelId, proj_record);
      int ret = BlockAccess::insert(target_relid, proj_record);

      if (ret!= SUCCESS/* insert fails */) {
          // close the targetrel by calling Schema::closeRel()
          // delete targetrel by calling Schema::deleteRel()
          // return ret;
          Schema::closeRel(targetRel);
        Schema::deleteRel(targetRel);
        return ret;
      }
  }

  // Close the targetRel by calling Schema::closeRel()
  Schema::closeRel(targetRel);

  // return SUCCESS.
  return SUCCESS;
}
