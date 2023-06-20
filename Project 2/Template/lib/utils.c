#include "utils.h"

char *getChunkData(int mapperID) {
  //TODO open message queue
   char* map;
   key_t key;
   sprintf(map, "%d", mapperID);
   if((key=ftok(map, 's') == -1)) {
	   printf("ERROR: Creating a key with ftok() in getChunkData");
   }
   int msgid;
   if((msgid=msgget(key, 0666 | IPC_CREAT))==-1){
	   printf("ERROR: Opening message queue in getChunkData");
   }


  //TODO receive chunk from the master
    char* message;
	if(msgrcv(msgid, &message, sizeof(message),1,0) == -1){
		printf("ERROR: Receiving message in getChunkData");
	}


  //TODO check for END message and send ACK to master and return NULL. 
  //Otherwise return pointer to the chunk data. 
	char* msg;
	if ((msg=getWord(message, &mapperID)) == "END"){
		if(msgsnd(msgid, "ACK", 4,0) == -1){
			printf("ERROR: Sending message in getChunkData");
		}
		return NULL;
	}
	else {
		return message;
	}

}

void sendChunkData(char *inputFile, int nMappers) {
  //TODO open the message queue
	char* map;
	key_t key = ftok(map, "%d", mapperID);
	mid = msgget(key, 0666 | IPC_CREAT);
	if(mid==-1){
		printf("ERROR: Opening message queue in sendChunkData");
	}

  //TODO Construct chunks of at most 1024 bytes each and send each chunk to a mapper in a round
  // robin fashion. Read one word at a time(End of word could be  \n, space or ROF). If a chunk 
  // is less than 1024 bytes, concatennate the word to the buffer.   
	int fd;
	if((fd = open(inputFile, O_RDONLY))==-1){
		printf("ERROR: Opening inputFile in sendChunkData");
	}

	char* buffer;
	char chunk[1024];
	int sum = 0;
	while(fscanf(fd,buffer) != EOF){
		
	}

  //TODO inputFile read complete, send END message to mappers
	

  //TODO wait to receive ACK from all mappers for END notification

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
      



    //TODO receive data from the master
     



    //TODO check for END message and send ACK to master and then return 0
    //Otherwise return 1


}

void shuffle(int nMappers, int nReducers) {

    //TODO open message queue
     


    //TODO traverse the directory of each Mapper and send the word filepath to the reducer
    //You should check dirent is . or .. or DS_Store,etc. If it is a regular
    //file, select the reducer using a hash function and send word filepath to
    //reducer 




    //TODO inputFile read complete, send END message to reducers

    


    
    //TODO  wait for ACK from the reducers for END notification
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
