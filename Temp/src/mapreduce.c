#include "mapreduce.h"

// execute executables using execvp
void execute(char **argv, int nProcesses){
	pid_t  pid;

	int i;
	for (i = 0; i < nProcesses; i++){
		pid = fork();
		if (pid < 0) {
			printf("ERROR: forking child process failed\n");
			exit(1);
		} else if (pid == 0) {
			char *processID = (char *) malloc(sizeof(char) * 5); // memory leak
			sprintf(processID, "%d", i+1);
			argv[1] = processID;
			if (execvp(*argv, argv) < 0) {
				printf("ERROR: exec failed\n");
				exit(1);
			}
		}
     }
}

int main(int argc, char *argv[]) {

	#ifdef TEST_INTERMEDIATE
		printf("WARNING: You are running mapreduce in a mode to test the intermediate submission\n");
		remove("test.txt"); //Try and remove test.txt [ignore error if not exist]
	#endif
	
	if(argc < 4) {
		printf("Less number of arguments.\n");
		printf("./mapreduce #mappers #reducers inputFile\n");
		fflush(stdout);
		exit(0);
	}

	int nMappers 	= strtol(argv[1], NULL, 10);
	int nReducers 	= strtol(argv[2], NULL, 10);

	if(nMappers < nReducers){
		printf("ERROR: Number of mappers should be greater than or equal to number of reducers...\n");
		fflush(stdout);
		exit(0);
	}

	if(nMappers == 0 || nReducers == 0){
		printf("ERROR: Mapper and Reducer count should be grater than zero...\n");
		fflush(stdout);
		exit(0);
	}
	
	char *inputFile = argv[3];

	if(access(inputFile, F_OK) == -1){
		printf("ERROR: %s not accessible\n", inputFile);
		fflush(stdout);
		exit(0);
	}

	bookeepingCode();

	int status;
	pid_t pid = fork();
	if(pid == 0){
		//send chunks of data to the mappers in RR fashion
		sendChunkData(inputFile, nMappers);
		exit(0);
	}
	sleep(1);

	// spawn mappers
	char *mapperArgv[] = {"./mapper", NULL, NULL};
	execute(mapperArgv, nMappers);

	// wait for all children to complete execution
    while (wait(&status) > 0){};

	
	#ifndef TEST_INTERMEDIATE
		// shuffle sends the word.txt files generated by mapper 
		// to reducer based on a hash function
		pid = fork();
		if(pid == 0){
			shuffle(nMappers, nReducers);
			exit(0);
		}
		sleep(1);

		// spawn reducers
		char *reducerArgv[] = {"./reducer", NULL, NULL};
		execute(reducerArgv, nReducers);

		// wait for all children to complete execution
		while (wait(&status) > 0){};

		return 0;
  	#endif
}