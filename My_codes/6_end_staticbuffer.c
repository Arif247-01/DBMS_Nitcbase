#include "StaticBuffer.h"

// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

StaticBuffer::StaticBuffer() {

  for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++/*bufferIndex = 0 to BUFFER_CAPACITY-1*/) {
    // set metainfo[bufferindex] with the following values
    //   free = true
    //   dirty = false
    //   timestamp = -1
    //   blockNum = -1
    metainfo[bufferIndex].free = true;
    metainfo[bufferIndex].dirty = false;
    metainfo[bufferIndex].timeStamp = -1;
    metainfo[bufferIndex].blockNum = -1;
  }
}

/*
At this stage, we are not writing back from the buffer to the disk since we are
not modifying the buffer. So, we will define an empty destructor for now. In
subsequent stages, we will implement the write-back functionality here.
*/
// write back all modified blocks on system exit //stage 6
StaticBuffer::~StaticBuffer() {///small doubt
  /*iterate through all the buffer blocks,
    write back blocks with metainfo as free=false,dirty=true
    using Disk::writeBlock()
    */
    for(int i=0; i<BUFFER_CAPACITY; i++){
      if(metainfo[i].free==false && metainfo[i].dirty==true){
        Disk::writeBlock(blocks[i], metainfo[i].blockNum);  //doubt
      }
    }
}

/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum) {
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
  
  for(int i=0; i<BUFFER_CAPACITY; i++){
    if(metainfo[i].blockNum == blockNum){
      return i;
    }
  }
  // find and return the bufferIndex which corresponds to blockNum (check metainfo)

  // if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}


int StaticBuffer::getFreeBuffer(int blockNum){
    // Check if blockNum is valid (non zero and less than DISK_BLOCKS)
    if (blockNum < 0 || blockNum > DISK_BLOCKS) {
      return E_OUTOFBOUND;
    }
    // and return E_OUTOFBOUND if not valid.

    // increase the timeStamp in metaInfo of all occupied buffers.
    for(int i=0; i<BUFFER_CAPACITY; i++){
      if(metainfo[i].free==false){
        metainfo[i].timeStamp++;
      }
    }

    // let bufferNum be used to store the buffer number of the free/freed buffer.
    int bufferNum;
    int flag = 0;

    // iterate through metainfo and check if there is any buffer free
    for(int i=0; i<BUFFER_CAPACITY; i++){
      if(metainfo[i].free==true){
        bufferNum = i;
        flag = 1;
        break;
      }
    }

    // if a free buffer is available, set bufferNum = index of that free buffer.

    // if a free buffer is not available,
    if(flag==0){
      int largest = -1;
      int id;
      for(int i=0; i<BUFFER_CAPACITY; i++){
        if(largest < metainfo[i].timeStamp){
          largest = metainfo[i].timeStamp;
          id = i;
        }
      }
      if(metainfo[id].dirty==true){
        Disk::writeBlock(blocks[id],metainfo[id].blockNum);
      }
      bufferNum = id;
    }
    //     find the buffer with the largest timestamp
    //     IF IT IS DIRTY, write back to the disk using Disk::writeBlock()
    //     set bufferNum = index of this buffer
    
    metainfo[bufferNum].free = false;
    metainfo[bufferNum].dirty = false;
    metainfo[bufferNum].blockNum = blockNum;
    metainfo[bufferNum].timeStamp = 0;

    // update the metaInfo entry corresponding to bufferNum with
    // free:false, dirty:false, blockNum:the input block number, timeStamp:0.

    // return the bufferNum.
    return bufferNum;
}

int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum().
    int bufferNum = getBufferNum(blockNum);

    // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
    //     return E_BLOCKNOTINBUFFER
    if(bufferNum==E_BLOCKNOTINBUFFER){
      return E_BLOCKNOTINBUFFER;
    }

    // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
    //     return E_OUTOFBOUND
    if (blockNum < 0 || blockNum > DISK_BLOCKS) {
      return E_OUTOFBOUND;
    }

    // else
    //     (the bufferNum is valid)
    //     set the dirty bit of that buffer to true in metainfo
    else{
      metainfo[bufferNum].dirty = true;
    }

    // return SUCCESS
    return SUCCESS;
}


