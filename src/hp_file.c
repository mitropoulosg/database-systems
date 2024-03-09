#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return -1;        \
  }                         \
}

int HP_CreateFile(char *fileName){
  //Δημιουργία αρχείου και άνοιγμα για να γίνει 
  //η επεξεργασία σε αυτό
  int fd;
  CALL_BF(BF_CreateFile(fileName));
  CALL_BF(BF_OpenFile(fileName,&fd));

  //Δημιουργία του μπλοκ μεταδεδομένων
  BF_Block* block;
  BF_Block_Init(&block);                //Δέσμευση στην ενδιάμεση για ένα μπλοκ 
  BF_AllocateBlock(fd,block);           //+1 μπλοκ στο αρχειο και φορτωση στον χώρο που δεσμεύτηκε
  void* data = BF_Block_GetData(block); //Σκουπίδια(είναι άδειο το μπλοκ)

  //Δημιουργία του instance HP_Info
  HP_info info_instance;
  memcpy(info_instance.file_type,"Heapfile",strlen("Heapfile")+1);
  info_instance.owner_block_buffer_pos = NULL; //Δεν έχει νόημα να το κρατήσουμε - η μνήμη του buffer θα αποδεσμευτεί
  info_instance.last_block_id = 0; 
  info_instance.rec_capacity = (BF_BLOCK_SIZE - sizeof(HP_block_info)) / sizeof(Record);
  //Μεταφορά στο block που δεσμεύσαμε (Στην αρχή του)
  HP_info *info = data;                 //Απαραίτητο cast (απο char σε HP_info)
  info[0] = info_instance;

  //Δημιουργία του instance HP_Block_info (δεν είναι απαραίτητο για το block 0)
  HP_block_info binfo_instance;
  binfo_instance.num_of_records = 0;
  //Μεταφορά στο block που δεσμεύσαμε (Στο τέλος του)
  HP_block_info* block_info = data;
  block_info[BF_BLOCK_SIZE / sizeof(HP_block_info) - 1] = binfo_instance;

  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  CALL_BF(BF_CloseFile(fd));

  BF_Block_Destroy(&block);             //Αποδέσμευση της μνήμης στο buffer
  
  return 0;
}

HP_info* HP_OpenFile(char *fileName){
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
  HP_info* file_info = data;

  file_info[0].fileDesc = fd;
  
  //Πρέπει να έιναι heapfile
  if (strcmp(file_info[0].file_type,"Heapfile") != 0) return NULL;

  //ΣΗΜΕΙΩΣΗ: Tο πρωτο μπλοκ ιδανικα μενει στον buffer!
  //Ενημερώνουμε τo hp_info για την (μόνιμη) θέση του στο buffer
  file_info->owner_block_buffer_pos = block;
  
  //Δεν κάνουμε unpin! (Συμφωνα με εκφώνση - πρώτο bulletpoint)
  BF_Block_SetDirty(block);

  //Σχεδιαστική επιλογή:
  //Τελος επιστρεφουμε ενα δυναμικα allocated αντιγραφο των metadata 
  //(θα μπορουσαμε να επιστρεψουμε απλα το file_info,
  // αλλα θα υπηρχε ο κινδυνος ο χρηστης να το πειραξει)
  HP_info* info = malloc(sizeof(HP_info));
  *info = file_info[0];
  
  return info;
}


int HP_CloseFile( HP_info* header_info ){
  BF_Block* buffer_ptr = header_info->owner_block_buffer_pos;

  CALL_BF(BF_UnpinBlock(buffer_ptr));   //Είναι pinned απο το ανοιγμα του αρχείου
  BF_Block_Destroy(&buffer_ptr);
  CALL_BF(BF_CloseFile(header_info->fileDesc));
  free(header_info);

  return 0;
}

int HP_InsertEntry(HP_info* hp_info, Record record){
  //Χρήσιμες μεταβλητές
  int blockId;                                                  //Εκεί που θα μπει τελικά η εγγραφή
  int block_count;
  int fd = hp_info->fileDesc;
  CALL_BF(BF_GetBlockCounter(hp_info->fileDesc,&block_count));
  BF_Block* first_block = NULL;
  BF_Block* last_block = NULL;
  BF_Block* new_block = NULL;
  void* data_of_first = NULL;
  void* data_of_last = NULL;
  void* data_of_new = NULL;
  int last_pos = BF_BLOCK_SIZE / sizeof(HP_block_info) - 1;    //index στην τελευταια θεση του μπλοκ με μεγεθος hp_block_info

  first_block = hp_info->owner_block_buffer_pos;
  data_of_first = BF_Block_GetData(first_block);

  //Φορτώνω το τελευταίο μπλοκ 
  if(block_count > 1) {
    BF_Block_Init(&last_block);
    CALL_BF(BF_GetBlock(fd,hp_info->last_block_id,last_block));
    
  }else {
    //Αν count = 1, το #0 υπαρχει έτσι κι αλλιώς στον buffer
    last_block = hp_info->owner_block_buffer_pos;               // last_block == first_block 
  }
  data_of_last = BF_Block_GetData(last_block);


  HP_block_info* last_block_info = data_of_last;
  if(block_count == 1 || last_block_info[last_pos].num_of_records == hp_info->rec_capacity){ //Αν υπάρχει μόνο το block #0 στο αρχειο (block metadata) 
                                                                                             //ή δεν χωράει η εγγαρφή στο τελευταίο block
    
    //Φτιάχνω καινούριο
    BF_Block_Init(&new_block);                          
    CALL_BF(BF_AllocateBlock(fd,new_block));               
    CALL_BF(BF_GetBlockCounter(fd,&block_count));     // block_count++;
    data_of_new = BF_Block_GetData(new_block);
    
    
    //Γέμισμα του νεου μπλοκ 
    
    //εγγραφη 
    Record* rec = data_of_new;
    rec[0] = record;
    //block metadata
    HP_block_info block_info_instance;
    block_info_instance.num_of_records = 1;           //Μπήκε μόλις μια εγγραφή
    HP_block_info* b_info = data_of_new;
    b_info[last_pos] = block_info_instance;

    BF_Block_SetDirty(new_block);
    CALL_BF(BF_UnpinBlock(new_block));
    BF_Block_Destroy(&new_block);

    
    //Ενημέρωση των metadata (αρχείου)
    hp_info->last_block_id = block_count - 1;
    HP_info* metadata_block = data_of_first;
    metadata_block[0] = *hp_info;
                                        
  }else {
    //Χωράει στο τελευταίο μπλοκ κανονικά
    
    HP_block_info* block_info = data_of_last;
    Record* rec = data_of_last;
    
    int pos = block_info[last_pos].num_of_records;
    rec[pos] = record;

    block_info[last_pos].num_of_records++;

    BF_Block_SetDirty(last_block);      
  }
  
  
  //Αποδεσμεύουμε και το προηγούμενο εφοσον δεν ειναι το πρωτο
  if(last_block != first_block){                          
    CALL_BF(BF_UnpinBlock(last_block));    
    BF_Block_Destroy(&last_block);         
  } 

  return block_count - 1;     //blockId
}

int HP_GetAllEntries(HP_info* hp_info, int value){
  int block_count = 0;
  
  for(int i=1; i<=hp_info->last_block_id; i++){
    block_count++;
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(hp_info->fileDesc,i,block));
    void* data = BF_Block_GetData(block);

    HP_block_info* b_info = data;
    int last_pos = BF_BLOCK_SIZE / sizeof(HP_block_info) - 1;
    
    for(int j = 0; j < b_info[last_pos].num_of_records; j++) {
      Record* rec = data;

      if (rec[j].id == value){
        printRecord(rec[j]);
      }
    }
    BF_UnpinBlock(block);
    BF_Block_Destroy(&block);
  }

  return block_count;
}

