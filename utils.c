#include "utils.h"
#include <dirent.h>

#define MQUEUE 999

char *getChunkData(int mapperID) {

  //TODO open message queue
	key_t key;
  int msgid;
	if ((key = ftok("test/T1/F1.txt", MQUEUE)) == -1) {
		printf("ERROR: Creating key with ftok() in getChunkData");
    exit(1);
	}
	if ((msgid = msgget(key, 0666)) == -1) {
		printf("ERROR: Opening message queue in getChunkData");
    exit(1);
	}

  //TODO receive chunk from the master
  struct msgBuffer msg;
  if (msgrcv(msgid, &msg, (sizeof(msg.msgText)+1), 0, 0) == -1) {
		printf("ERROR: Recieving message in getChunkData\n");
    exit(1);
	}

  //TODO check for END message and send ACK to master and return NULL. 
  //Otherwise return pointer to the chunk data. 
	if ((strcmp(msg.msgText, "END") == 0)) {

		struct msgBuffer ackMessage;
		ackMessage.msgType = ACKTYPE;

		if (msgsnd(msgid, &ackMessage, strlen(ackMessage.msgText) + 1, 0) == -1) {
			printf("ERROR: Sending message in getChunkData");
      exit(1);
		}

		return NULL;
	}
	else {
    char* chunkData = malloc(sizeof(msg.msgText));
    strcpy(chunkData, msg.msgText);
    return chunkData;
  }
}


//return the next word as an output parameter
//return 1: indicates read a word
//retrun 0: indicates reach at the end of the stream
int getNextWord(int fd, char* buffer){
   char word[100];
   memset(word, '\0', 100);
   int i = 0;
   while(read(fd, &word[i], 1) == 1 ){
    if(word[i] == ' '|| word[i] == '\n' || word[i] == '\t'){
        strcpy(buffer, word);
        return 1;
    }
    if(word[i] == 0x0){
      break;
    }

    i++;
   }
   strcpy(buffer, word);
   return 0;
}


void sendChunkData(char *inputFile, int nMappers) {

  //TODO open the message queue
    key_t key;
    int msgid;
    if ((key = ftok("test/T1/F1.txt", MQUEUE)) == -1) {
		  printf("ERROR: Creating key using ftok() in sendChunkData");
      exit(1);
    }
  	if ((msgid = msgget (key, 0666 | IPC_CREAT)) == -1) {
		  printf("ERROR: Opening message queue in sendChunkData");
      exit(1);
  	}

  // TODO Construct chunks of at most 1024 bytes each and send each chunk to a mapper in a round
  // robin fashion. Read one word at a time(End of word could be  \n, space or ROF). If a chunk 
  // is less than 1024 bytes, concatennate the word to the buffer. 

    int fd;
    if ((fd = open(inputFile, O_RDONLY)) == -1) {
      printf("ERROR: Opening inputFile in sendChunkData");
      exit(1);
    }

    char word[50];
    char chunk[chunkSize];
    int currMapper = 1;
    struct msgBuffer* msg = malloc(sizeof(struct msgBuffer));

    while (getNextWord(fd, word) != 0) {

      if (strlen(chunk) + strlen(word) >= chunkSize) {

        //send the chunk to mapper i
        msg->msgType = currMapper;
        strcpy(msg->msgText, chunk);

        if (msgsnd(msgid, msg, (sizeof(msg->msgText)+1), 0) == -1) {
          printf("ERROR: Sending chunk in sendChunkData");
          exit(1);
        } 

        memset(chunk, '\0', chunkSize);
        strcpy(chunk, word);
        memset(word, '\0', 50);

        currMapper++;
        if (currMapper > nMappers)
          currMapper = 1;
      }
      else {
        //concatennate the word to the chunk buffer
        strcat(chunk, word);
      }
    }

    if ((strlen(chunk) + strlen(word)) > 0) {
      msg->msgType = currMapper;
      strcpy(msg->msgText, chunk);
      if (msgsnd(msgid, msg, (sizeof(msg->msgText)+1), 0) == -1) {
          printf("ERROR: Sending chunk in sendChunkData");
          exit(1);
        }
    } 

  //TODO inputFile read complete, send END message to mappers
  struct msgBuffer endmsg;
  strcpy(endmsg.msgText, "END");

  for (int i = 0; i < nMappers; i++) {
    if (msgsnd(msgid, &endmsg, (sizeof(endmsg.msgText)), 0) == -1) {
      printf("ERROR: Sending END message in sendChunkData");
      exit(1);
    }
  } 

  //TODO wait to receive ACK from all mappers for END notification
  struct msgBuffer msgReceiver;
  
  for (int i = 0; i < nMappers; i++) {
    if (msgrcv(msgid, &msgReceiver, (sizeof(msgReceiver.msgText)), ACKTYPE, 0) == -1) {
      printf("ERROR: Sending END message in sendChunkData");
      exit(1);
    }
  } 
  free(msg);
  msgctl(msgid,IPC_RMID, 0);
}


// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
	unsigned long hash = 0;
    int c;

    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash % reducers);
}

int getInterData(char *key, int reducerID) {
  //TODO open message queue 
  key_t keys;
	if ((keys = ftok(key, MQUEUE)) == -1) {
		printf("ERROR: Creating key with ftok() in getInterData");
    exit(0);
	}
	int msgid;
	if ((msgid = msgget(keys, 0666 | IPC_CREAT)) == -1) {
		printf("ERROR: Opening message queue in getInterData");
    exit(0);
	}

  //TODO receive data from the master
  struct msgBuffer msg;
  if (msgrcv(msgid, (void*) &msg, strlen(msg.msgText) + 1, reducerID, IPC_NOWAIT) == -1) {
		printf("ERROR: Recieving message in getChunkData");
    exit(0);
	}

  //TODO check for END message and send ACK to master and then return 0
  //Otherwise return 1
  if (msg.msgType == ENDTYPE) {
    if (msgsnd(msgid, (void*) &msg, (strlen(msg.msgText) + 1), 0) == -1) {
      printf("ERROR: Sending message in getInterData");
      exit(0);
    }
    return 0;
  }
  else
    return 1;
}

void shuffle(int nMappers, int nReducers) {

  //TODO open message queue
  int msgid;
  key_t key = ftok("test/T1/F1.txt", MQUEUE);
  if (key == -1) {
    printf("ERROR: Creating key using ftok() in shuffle");
    exit(1);
  }

  if ((msgid = msgget (key, 0666 | IPC_CREAT)) == -1) {
    printf("ERROR: Opening message queue in shuffle");
    exit(1);
  }

  //TODO traverse the directory of each Mapper and send the word filepath to the reducer
  //You should check dirent is . or .. or DS_Store,etc. If it is a regular
  //file, select the reducer using a hash function and send word filepath to
  //reducer 
  char filePath[50] = "./output/MapOut/Map_";
  char mappperNumber[3];
  DIR* dir;
  struct dirent* entry;
  struct msgBuffer msg;

  for (int i = 1; i <= nMappers; i++) {
    sprintf(mappperNumber, "%d", i);
    strcat(filePath, mappperNumber);
    dir = opendir(filePath);

    while ((entry = readdir(dir)) != NULL) {
      if (entry->d_type == DT_REG) {

        char wordFilePath[50];
        strcpy(wordFilePath, filePath);
        strcat(wordFilePath, "/");
        strcat(wordFilePath, entry->d_name);

        int reducerID = hashFunction(wordFilePath, nReducers);
        strcpy(msg.msgText, wordFilePath);
        msg.msgType = reducerID;

        if (msgsnd(msgid, (void*) &msg, strlen(msg.msgText) + 1, 0) == -1) {
          printf("ERROR: Sending message in shuffle");
          exit(1);
        }

      }
    }
  }

  //TODO inputFile read complete, send END message to reducers
  struct msgBuffer endmsg;
  strcat(endmsg.msgText, "END");
  endmsg.msgType = ENDTYPE;

  for (int i = 0; i < nReducers; i++) {
    if (msgsnd(msgid, (void*) &endmsg, (strlen(msg.msgText) + 1), 0) == -1) {
      printf("ERROR: Sending END message in shuffle");
      exit(1);
    }
  } 
  
  //TODO  wait for ACK from the reducers for END notification
  struct msgBuffer msgReceiver;
  int ackCounter = 0;
  
  while (ackCounter < nReducers) {
    
    if (msgrcv(msgid, &msgReceiver, sizeof(msgReceiver.msgText), ACKTYPE, 0) == -1) {
      printf("ERROR: Recieving ACK message in sendChunkData");
      exit(0);
    }
    if (msgReceiver.msgType == ACKTYPE) {
      ackCounter ++;
    }
  }

}

// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char) * chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		// read a single word at a time from chunk
		// printf("%d\n", i);
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}
