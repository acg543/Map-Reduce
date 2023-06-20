#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <sys/msg.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <ctype.h>

#define chunkSize 1024
#define MSGSIZE 1100
#define ENDTYPE 1000
#define ACKTYPE 1100

struct msgBuffer {
    long msgType;
    char msgText[MSGSIZE];
};

//getNextWord():
//      usage:      retrieves the next word from an open file and places that word in buffer
//                  intended to be used by students in sendChunkData to save them time
//
//      inputs:     fd      --> Opened file descriptor
//                  buffer  --> char array/string to hold next word
//
//      returns:    1       --> if a word was read
//                  0       --> if end of stream was reached 
int getNextWord(int fd, char* buffer);

// mapper side
int validChar(char c);
// getWord usage - retrieves words from the chunk passed until it is fully traversed
// given a chunk of data chunkData, the call to getWord should look as below:
// int i = 0;
// char *buffer;
// while ((buffer = getWord(chunkData, &i)) != NULL){
// 			your code
// }
char *getWord(char *chunk, int *i);
char *getChunkData(int mapperID);
void sendChunkData(char *inputFile, int nMappers);


// reducer side
int hashFunction(char* key, int reducers);
int getInterData(char *key, int reducerID);
void shuffle(int nMappers, int nReducers);

// directory
void createOutputDir();
char *createMapDir(int mapperID);
void removeOutputDir();
void bookeepingCode();

#endif