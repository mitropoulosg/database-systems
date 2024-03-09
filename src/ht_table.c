#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      return -1;             \
    }                         \
  }


int HT_CreateFile(char *fileName,  int buckets){
  //Δημιουργία αρχείου και άνοιγμα για να γίνει 
  //η επεξεργασία σε αυτό
  int fd;
  CALL_OR_DIE(BF_CreateFile(fileName));
  CALL_OR_DIE(BF_OpenFile(fileName,&fd));

  //Δημιουργία του μπλοκ μεταδεδομένων
  BF_Block* block;
  BF_Block_Init(&block);                //Δέσμευση στην ενδιάμεση για ένα μπλοκ 
  BF_AllocateBlock(fd,block);           //+1 μπλοκ στο αρχειο και φορτωση στον χώρο που δεσμεύτηκε
  void* data = BF_Block_GetData(block); //Σκουπίδια(είναι άδειο το μπλοκ)
  
  //Δημιουργία instance HT_info
  HT_info info_instance;
  info_instance.fileDesc = fd;
  memcpy(info_instance.file_type,"Hash Table",strlen("Hash Table") + 1);
  info_instance.numBuckets = buckets;

  for(int i = 0; i<info_instance.numBuckets; i++){
    info_instance.buckets[i][0] = info_instance.buckets[i][1] = -1; //αρχικά δεν υπάρχουν μπλοκ στα buckets 
  }
  info_instance.rec_capacity = (BF_BLOCK_SIZE - sizeof(HT_block_info)) / sizeof(Record);
  info_instance.owner_block_buffer_pos = NULL;
  //Μεταφορά στον buffer στο πρώτο μπλοκ
  HT_info* info = data;
  info[0] = info_instance;

  //metadata του πρώτου μπλοκ
  HT_block_info b_info_instance;
  b_info_instance.num_of_records = 0;
  b_info_instance.next = -1;
  //Μεταφορά στον buffer στο πρώτο μπλοκ
  HT_block_info* block_info = data;
  block_info[BF_BLOCK_SIZE / sizeof(HT_block_info) - 1 ] = b_info_instance;
  
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  CALL_OR_DIE(BF_CloseFile(fd));

  BF_Block_Destroy(&block);             //Αποδέσμευση της μνήμης στο buffer
  return 0;
}

HT_info* HT_OpenFile(char *fileName){
  int fd;
  BF_ErrorCode check;
  
  check = BF_OpenFile(fileName,&fd);
  if (check != BF_OK) return NULL;
  
  //Δέσμευση στον ενταμιευτη και φόρτωση απο το αρχείο
  BF_Block* block;
  BF_Block_Init(&block);
  check = BF_GetBlock(fd,0,block);
  if (check != BF_OK) return NULL;
  
  //Πρόσβαση στα δεδομένα του μπλοκ
  void* data = BF_Block_GetData(block);
  HT_info* file_info = data;
  
  //ενημέρωση του fd
  file_info[0].fileDesc = fd;
  //Πρέπει να έιναι heapfile
  if (strcmp(file_info[0].file_type,"Hash Table") != 0) return NULL;

  //ΣΗΜΕΙΩΣΗ: Tο πρωτο μπλοκ ιδανικα μενει στον buffer!
  //Ενημερώνουμε τo ht_info για την (μόνιμη) θέση του στο buffer
  file_info->owner_block_buffer_pos = block;
  
  //Δεν κάνουμε unpin! (Συμφωνα με εκφώνση - πρώτο bulletpoint)
  BF_Block_SetDirty(block);

  //Σχεδιαστική επιλογή:
  //Τελος επιστρεφουμε ενα δυναμικα allocated αντιγραφο των metadata 
  //(θα μπορουσαμε να επιστρεψουμε απλα το file_info,
  // αλλα θα υπηρχε ο κινδυνος ο χρηστης να το πειραξει)
  HT_info* info = malloc(sizeof(HT_info));
  *info = file_info[0];
  
  return info;
    
}


int HT_CloseFile( HT_info* header_info){
  BF_Block* buffer_ptr = header_info->owner_block_buffer_pos;

  CALL_OR_DIE(BF_UnpinBlock(buffer_ptr));   //Είναι pinned απο το ανοιγμα του αρχείου

  BF_Block_Destroy(&buffer_ptr);
  CALL_OR_DIE(BF_CloseFile(header_info->fileDesc));
  free(header_info);

  return 0;
}

int HT_InsertEntry(HT_info* ht_info, Record record){
  int entry_bucket = record.id % ht_info->numBuckets;
  
  int blockId;
  int fd = ht_info->fileDesc;
  int block_count;
  CALL_OR_DIE(BF_GetBlockCounter(fd,&block_count));
  int last_pos = BF_BLOCK_SIZE / sizeof(HT_block_info) - 1;
  

  //Το first δεν ειναι χρήσιμο αλλα το βάζουμε για να γίνει κατανοητή η πρωτη στήλη του πινακα
  int first = ht_info->buckets[entry_bucket][0];
  int last = ht_info->buckets[entry_bucket][1];
  BF_Block* first_block = NULL;
  void* data_of_first = NULL;
  BF_Block* last_block = NULL;
  void* data_of_last = NULL;

  int last_exists = 0; //boolean 
  if(last != -1){
    last_exists = 1;
    BF_Block_Init(&last_block);
    CALL_OR_DIE(BF_GetBlock(fd,last,last_block));
    data_of_last = BF_Block_GetData(last_block);
  }

  HT_block_info* last_block_info = data_of_last;

  //Αν δεν χωραει στο τελευταιο μπλοκ του μπακετ ή το bucket ειναι αδειο
  if((last_exists && last_block_info[last_pos].num_of_records == ht_info->rec_capacity) || (!last_exists)){
    //Δημιουργία νέου
    BF_Block* new_block = NULL;
    void* data_of_new = NULL;
    BF_Block_Init(&new_block);
    CALL_OR_DIE(BF_AllocateBlock(fd,new_block));
    CALL_OR_DIE(BF_GetBlockCounter(fd,&block_count));
    data_of_new = BF_Block_GetData(new_block);

    //Γέμισμα νεου

    //Εγγραφή
    Record* rec = data_of_new;
    rec[0] = record;
    //block metadata
    HT_block_info block_info_instance;
    block_info_instance.num_of_records = 1;           //Μπήκε μόλις μια εγγραφή
    block_info_instance.next = -1;
    HT_block_info* b_info = data_of_new;
    b_info[last_pos] = block_info_instance;

    BF_Block_SetDirty(new_block);
    CALL_OR_DIE(BF_UnpinBlock(new_block));
    BF_Block_Destroy(&new_block);

    //Ενημέρωση του #0 (συγκεκριμενα του hash table)
    if(!last_exists){
      //Αρα και το πρωτο θα ειναι αρχικα -1, οποτε το αλλαζουμε
      ht_info->buckets[entry_bucket][0] = block_count - 1;
    }else {
      //Ενωση με το προηγούμενο μπλοκ
      last_block_info[last_pos].next = block_count - 1;
    }
    ht_info->buckets[entry_bucket][1] = block_count - 1;
    void* data =  BF_Block_GetData(ht_info->owner_block_buffer_pos);
    memcpy(data,ht_info,sizeof(HT_info));       //Eνημερωνουμε το hash table και στο buffer 

    blockId = block_count - 1;
  }else{
    //χωραέι στο τελευταίο μπλοκ του μπακετ
    int pos = last_block_info[last_pos].num_of_records;

    Record* rec = data_of_last;
    rec[pos] = record;

    last_block_info[last_pos].num_of_records++;
 
    blockId = ht_info->buckets[entry_bucket][1];
    BF_Block_SetDirty(last_block);

  }

  if(last_exists){ //Αν υπήρξε τελευταίο εξαρχής (συμμετρική της 1ης if)
    CALL_OR_DIE(BF_UnpinBlock(last_block));
    BF_Block_Destroy(&last_block);
  }
  
  return blockId;
}

int HT_GetAllEntries(HT_info* ht_info, int value ){
  int pos = value % ht_info->numBuckets;
  int block_count = 0;

  int start = ht_info->buckets[pos][0];
  for(int i = start; i != -1;) {
    block_count++;
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc,i,block));
    void* data = BF_Block_GetData(block);

    HT_block_info* b_info = data;
    int last_pos = BF_BLOCK_SIZE / sizeof(HT_block_info) - 1;
    for(int j = 0; j < b_info[last_pos].num_of_records; j++) {
      Record* rec = data;

      if (rec[j].id == value){
        printRecord(rec[j]);
      }
    }

    i = b_info[last_pos].next;      //increment

    BF_UnpinBlock(block);
    BF_Block_Destroy(&block);
  }
  return block_count;
}

int HT_HashStatistics(char* filename) {
    BF_Init(LRU);
    HT_info* ht_info = HT_OpenFile(filename);

    //1
    int block_count = 0;
    CALL_OR_DIE(BF_GetBlockCounter(ht_info->fileDesc,&block_count));
    printf("Number of blocks of file %s is : %d\n\n",filename,block_count);

    int min_recs = -1;
    int max_recs = -1;
    int total_sum_of_recs = 0;  
    //Για κάθε κουβα
    for(int i=0; i<ht_info->numBuckets; i++){
        int sum_of_recs = 0;
        
        //Για κάθε μπλοκ του κουβά
        int start = ht_info->buckets[i][0];
        for(int j = start; j != -1;) {
            BF_Block* block;
            BF_Block_Init(&block);
            CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc,j,block));
            void* data = BF_Block_GetData(block);

            HT_block_info* b_info = data;
            int last_pos = BF_BLOCK_SIZE / sizeof(HT_block_info) - 1;
            sum_of_recs += b_info[last_pos].num_of_records;
            total_sum_of_recs += b_info[last_pos].num_of_records;
            j = b_info[last_pos].next;      //increment

            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);
        }

        if(min_recs == -1 && max_recs == -1){
            //Αρχικοποιουμε
            min_recs = max_recs = sum_of_recs;
        } else {
            if(sum_of_recs < min_recs){
                min_recs = sum_of_recs;
            }
            if(sum_of_recs > max_recs){
                max_recs = sum_of_recs;
            }
        }
    }

    //2
    printf("Max records in bucket: %d\n",max_recs);
    printf("Min records in bucket: %d\n",min_recs);
    printf("Average records per bucket: %f\n\n",(double) total_sum_of_recs / (double) ht_info->numBuckets);

    //3
    printf("Average blocks per bucket: %f\n",(double)block_count / (double) ht_info->numBuckets);

    //4
    int count_of_buckets_with_overflows = 0;
    for(int i=0; i<ht_info->numBuckets; i++){
        int start = ht_info->buckets[i][0];
        int last = ht_info->buckets[i][1];

        if(start != last){
          count_of_buckets_with_overflows++;
        }
        
        int num_of_blocks_of_bucket = 0;
        //Για κάθε μπλοκ του κουβά
        for(int j = start; j != -1;) {
            BF_Block* block;
            BF_Block_Init(&block);
            CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc,j,block));
            void* data = BF_Block_GetData(block);
            
            num_of_blocks_of_bucket++;

            HT_block_info* b_info = data;
            int last_pos = BF_BLOCK_SIZE / sizeof(HT_block_info) - 1;
            j = b_info[last_pos].next;      //increment

            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);
        }

        if(start != last){
          printf("Bucket #%d has %d overflow blocks\n",i,num_of_blocks_of_bucket - 1);

        }else {
          printf("Bucket #%d has %d overflow blocks\n",i,0);
        }

    }

    printf("Buckets with overflows: %d\n",count_of_buckets_with_overflows);


    int check = HT_CloseFile(ht_info);
    if(check == -1) return check;
    
    CALL_OR_DIE(BF_Close());
    return 0;
}