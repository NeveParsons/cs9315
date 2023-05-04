// COMP9315 22T1 Final Exam Q1
// Find longest tuple in a no-frills file

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "no-frills.h"

int main(int argc, char **argv)
{
	if (argc < 2) {
		printf("Usage: %s DataFile\n", argv[0]);
		exit(1);
	}
	int fd = open(argv[1],O_RDONLY);
	if (fd < 0) {
		printf("Can't open file %s\n", argv[1]);
		exit(1);
	}
	char longest[MAXTUPLEN];

	// Add variables and code here to find
	// the longest tuple in the data file
	
	FILE* fp = fopen(argv[1], "r");

	fseek(fp, 0L, SEEK_END);  
	int fileSize = ftell(fp);
	fseek(fp, 0, SEEK_SET);

	char tuples;

	if(fileSize == 0) {
		printf("<none>\n");
		return -1;
	}

	if(fileSize == PAGESIZE) {
		fread(&tuples, sizeof(char), 1, fp);
		fseek(fp, 0, SEEK_SET);
		if(tuples == 0) {
			printf("<none>\n");
			return -1;
		}
	}

	if(fileSize % PAGESIZE) {
		printf("<none>\n");
		return -1;
	}

	int maxTuple = 0;
	int currPage = 0;
	while(fread(&tuples, sizeof(char), 1, fp)) {
	  for(int i = 0; i < tuples; i++) {
		char terminator;
		int length = 0;
		char tmp[MAXTUPLEN];
		while(fread(&terminator, sizeof(char), 1, fp)) {
			tmp[length] = terminator;
			length++;
			if(terminator == '\0') {
				break;
			}
    	}
		if(maxTuple < length) {
			maxTuple = length;
			strcpy(longest, tmp);
		}
	  }
	  currPage++;
	  fseek(fp, PAGESIZE*currPage, SEEK_SET);
    }

	printf("%s\n",longest);
	return 0;
}
