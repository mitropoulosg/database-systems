#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"

struct Pair {
  char name[20];
  int block_id;
};

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      return -1;             \
    }                         \
  }

int hash(char* name,int num_buckets);

int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){
  //Δημιουργία αρχείου και άνοιγμα για να γίνει 
  //η επεξεργασία σε αυτό
  int fd;
  CALL_OR_DIE(BF_CreateFile(sfileName));
  CALL_OR_DIE(BF_OpenFile(sfileName,&fd));

  //Δημιουργία του μπλοκ μεταδεδομένων
  BF_Block* block;
  BF_Block_Init(&block);                //Δέσμευση στην ενδιάμεση για ένα μπλοκ 
  BF_AllocateBlock(fd,block);           //+1 μπλοκ στο αρχειο και φορτωση στον χώρο που δεσμεύτηκε
  void* data = BF_Block_GetData(block); //Σκουπίδια(είναι άδειο το μπλοκ)
  
  //Δημιουργία instance SHT_info
  SHT_info info_instance;
  info_instance.fileDesc = fd;          
  memcpy(info_instance.file_type,"Secondary Hash Table",strlen("Secondary Hash Table") + 1);
  info_instance.numBuckets = buckets;
  
  for(int i = 0; i<info_instance.numBuckets; i++){
    info_instance.buckets[i][0] = info_instance.buckets[i][1] = -1; //αρχικά δεν υπάρχουν μπλοκ στα buckets 
  }
  info_instance.pair_capacity = (BF_BLOCK_SIZE - sizeof(SHT_block_info)) / sizeof(struct Pair);
  info_instance.owner_block_buffer_pos = NULL;
  //Μεταφορά στον buffer στο πρώτο μπλοκ
  SHT_info* info = data;
  info[0] = info_instance;

  //metadata του πρώτου μπλοκ
  SHT_block_info b_info_instance;
  b_info_instance.num_of_pairs = 0;
  b_info_instance.next = -1;
  //Μεταφορά στον buffer στο πρώτο μπλοκ
  SHT_block_info* block_info = data;
  block_info[BF_BLOCK_SIZE / sizeof(SHT_block_info) - 1 ] = b_info_instance;
  
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  CALL_OR_DIE(BF_CloseFile(fd));

  BF_Block_Destroy(&block);             //Αποδέσμευση της μνήμης στο buffer
  return 0;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName){
  int fd;
  BF_ErrorCode check;
  
  check = BF_OpenFile(indexName,&fd);
  if (check != BF_OK) return NULL;
  
  //Δέσμευση στον ενταμιευτη και φόρτωση απο το αρχείο
  BF_Block* block;
  BF_Block_Init(&block);
  check = BF_GetBlock(fd,0,block);
  if (check != BF_OK) return NULL;
  
  //Πρόσβαση στα δεδομένα του μπλοκ
  void* data = BF_Block_GetData(block);
  SHT_info* file_info = data;
  
  //ενημέρωση του fd
  file_info[0].fileDesc = fd;
  //Πρέπει να έιναι heapfile
  if (strcmp(file_info[0].file_type,"Secondary Hash Table") != 0) return NULL;

  //ΣΗΜΕΙΩΣΗ: Tο πρωτο μπλοκ ιδανικα μενει στον buffer!
  //Ενημερώνουμε τo sht_info για την (μόνιμη) θέση του στο buffer
  file_info->owner_block_buffer_pos = block;
  
  //Δεν κάνουμε unpin! (Συμφωνα με εκφώνση - πρώτο bulletpoint)
  BF_Block_SetDirty(block);

  //Σχεδιαστική επιλογή:
  //Τελος επιστρεφουμε ενα δυναμικα allocated αντιγραφο των metadata 
  //(θα μπορουσαμε να επιστρεψουμε απλα το file_info,
  // αλλα θα υπηρχε ο κινδυνος ο χρηστης να το πειραξει)
  //ΠΡΟΣΟΧΗ : ο χρήστης θα μπορεί να πειράξει τον πίνακα!!!
  SHT_info* info = malloc(sizeof(SHT_info));
  *info = file_info[0];
  
  return info;
    
}


int SHT_CloseSecondaryIndex( SHT_info* header_info ){
  BF_Block* buffer_ptr = header_info->owner_block_buffer_pos;

  CALL_OR_DIE(BF_UnpinBlock(buffer_ptr));   //Είναι pinned απο το ανοιγμα του αρχείου

  BF_Block_Destroy(&buffer_ptr);
  CALL_OR_DIE(BF_CloseFile(header_info->fileDesc));
  free(header_info);

  return 0;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){
  int entry_bucket = hash(record.name,sht_info->numBuckets);
  
  int blockId;
  int fd = sht_info->fileDesc;
  int block_count;
  CALL_OR_DIE(BF_GetBlockCounter(fd,&block_count));
  int last_pos = BF_BLOCK_SIZE / sizeof(SHT_block_info) - 1;
  


  int first = sht_info->buckets[entry_bucket][0];
  int last = sht_info->buckets[entry_bucket][1];
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

  SHT_block_info* last_block_info = data_of_last;

  //Αν δεν χωραει στο τελευταιο μπλοκ του μπακετ ή το bucket ειναι αδειο
  if((last_exists && last_block_info[last_pos].num_of_pairs == sht_info->pair_capacity) || (!last_exists)){
    //Δημιουργία νέου
    BF_Block* new_block = NULL;
    void* data_of_new = NULL;
    BF_Block_Init(&new_block);
    CALL_OR_DIE(BF_AllocateBlock(fd,new_block));
    CALL_OR_DIE(BF_GetBlockCounter(fd,&block_count));
    data_of_new = BF_Block_GetData(new_block);

    //Γέμισμα νεου

    //Ζευγάρι
    struct Pair pair;
    memcpy(pair.name,record.name,strlen(record.name)+1);
    pair.block_id = block_id;
    struct Pair* pr = data_of_new;
    pr[0] = pair;

    //block metadata
    SHT_block_info block_info_instance;
    block_info_instance.num_of_pairs = 1;           //Μπήκε μόλις μια εγγραφή
    block_info_instance.next = -1;
    SHT_block_info* b_info = data_of_new;
    b_info[last_pos] = block_info_instance;

    BF_Block_SetDirty(new_block);
    CALL_OR_DIE(BF_UnpinBlock(new_block));
    BF_Block_Destroy(&new_block);

    //Ενημέρωση του #0 (συγκεκριμενα του hash table)
    if(!last_exists){
      //Αρα και το πρωτο θα ειναι αρχικα -1, οποτε το αλλαζουμε
      sht_info->buckets[entry_bucket][0] = block_count - 1;
    }else {
      //Ενωση με το προηγούμενο μπλοκ
      last_block_info[last_pos].next = block_count - 1;
    }
    sht_info->buckets[entry_bucket][1] = block_count - 1;
    void* data =  BF_Block_GetData(sht_info->owner_block_buffer_pos);
    memcpy(data,sht_info,sizeof(SHT_info));       //Eνημερωνουμε το hash table και στο buffer
  }else{
    //χωραέι στο τελευταίο μπλοκ του μπακετ
    int pos = last_block_info[last_pos].num_of_pairs;

    struct Pair pair;
    //strcpy(pair.name,record.name);
    memcpy(pair.name,record.name,strlen(record.name)+1);
    pair.block_id = block_id;
    struct Pair* pr = data_of_last;
    pr[pos] = pair;

    last_block_info[last_pos].num_of_pairs++;
    BF_Block_SetDirty(last_block);

  }

  if(last_exists){ //Αν υπήρξε τελευταίο εξαρχής (συμμετρική της 1ης if)
    CALL_OR_DIE(BF_UnpinBlock(last_block));
    BF_Block_Destroy(&last_block);
  }
  
  return 0;
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* value){
  int pos = hash(value,sht_info->numBuckets);
  int sht_block_count = 0;
  int ht_block_count = 0;
  
  //Για κάθε μπλοκ του κουβα
  int start = sht_info->buckets[pos][0];
  for(int i = start; i != -1;) {
    sht_block_count++;
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(sht_info->fileDesc,i,block));
    void* data = BF_Block_GetData(block);

    SHT_block_info* b_info = data;
    int last_pos = BF_BLOCK_SIZE / sizeof(SHT_block_info) - 1;
    
    //Για κάθε ζεύγος του sht_block
    for(int j = 0; j < b_info[last_pos].num_of_pairs; j++) {
      
      struct Pair* pr = data;
      char* test = pr[j].name;
      if (strcmp(pr[j].name,value) == 0){
        ht_block_count++;     //Θα επισκεφθούμε ενα ht block,άρα το προσμετράμε
        //Βαζουμε στον buffer το block που δειχνει το pair
        BF_Block* block;
        BF_Block_Init(&block);
        CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc,pr[j].block_id,block));
        void* data = BF_Block_GetData(block);
        
        //Για κάθε εγγραφή του ht block
        HT_block_info* b_info = data;
        int last_pos = BF_BLOCK_SIZE / sizeof(HT_block_info) - 1;
        for(int k=0; k<b_info[last_pos].num_of_records; k++){
          Record* rec = data;
          if (strcmp(rec[k].name,value) == 0){
            printRecord(rec[k]);
          }

        }

        CALL_OR_DIE(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
      }
    }

    i = b_info[last_pos].next;      //increment

    BF_UnpinBlock(block);
    BF_Block_Destroy(&block);
  }

  return ht_block_count + sht_block_count;
}

int SHT_HashStatistics(char* filename){
    BF_Init(LRU);
    SHT_info* sht_info = SHT_OpenSecondaryIndex(filename);

    //1
    int block_count = 0;
    CALL_OR_DIE(BF_GetBlockCounter(sht_info->fileDesc,&block_count));
    printf("Number of blocks of file %s is : %d\n\n",filename,block_count);

    int min_pairs = -1;
    int max_pairs = -1;
    int total_sum_of_pairs = 0;  
    //Για κάθε κουβα
    for(int i=0; i<sht_info->numBuckets; i++){
        int sum_of_pairs = 0;
        
        //Για κάθε μπλοκ του κουβά
        int start = sht_info->buckets[i][0];
        for(int j = start; j != -1;) {
            BF_Block* block;
            BF_Block_Init(&block);
            CALL_OR_DIE(BF_GetBlock(sht_info->fileDesc,j,block));
            void* data = BF_Block_GetData(block);

            SHT_block_info* b_info = data;
            int last_pos = BF_BLOCK_SIZE / sizeof(SHT_block_info) - 1;
            sum_of_pairs += b_info[last_pos].num_of_pairs;
            total_sum_of_pairs += b_info[last_pos].num_of_pairs;
            j = b_info[last_pos].next;      //increment

            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);
        }

        if(min_pairs == -1 && max_pairs == -1){
            //Αρχικοποιουμε
            min_pairs = max_pairs = sum_of_pairs;
        } else {
            if(sum_of_pairs < min_pairs){
                min_pairs = sum_of_pairs;
            }
            if(sum_of_pairs > max_pairs){
                max_pairs = sum_of_pairs;
            }
        }
    }

    //2
    printf("Max pairs in bucket: %d\n",max_pairs);
    printf("Min pairs in bucket: %d\n",min_pairs);
    printf("Average pairs per bucket: %f\n\n",(double) total_sum_of_pairs / (double) sht_info->numBuckets);

    //3
    printf("Average blocks per bucket: %f\n",(double)block_count / (double) sht_info->numBuckets);

    //4
    int count_of_buckets_with_overflows = 0;
    for(int i=0; i<sht_info->numBuckets; i++){
        int start = sht_info->buckets[i][0];
        int last = sht_info->buckets[i][1];

        if(start != last){
          count_of_buckets_with_overflows++;
        }
        
        int num_of_blocks_of_bucket = 0;
        //Για κάθε μπλοκ του κουβά
        for(int j = start; j != -1;) {
            BF_Block* block;
            BF_Block_Init(&block);
            CALL_OR_DIE(BF_GetBlock(sht_info->fileDesc,j,block));
            void* data = BF_Block_GetData(block);
            
            num_of_blocks_of_bucket++;

            HT_block_info* b_info = data;
            int last_pos = BF_BLOCK_SIZE / sizeof(SHT_block_info) - 1;
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


    int check = SHT_CloseSecondaryIndex(sht_info);
    if(check == -1) return check;
    
    CALL_OR_DIE(BF_Close());
    return 0;
}

int hash(char *str,int num_buckets){
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash % num_buckets;
}
