// COMP9315 22T1 Final Exam Q1
// Count tuples in a no-frills file

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
	int ntuples = 0;

	FILE* fp = fopen(argv[1], "r");

	fseek(fp, 0L, SEEK_END);  
	int fileSize = ftell(fp);
	fseek(fp, 0, SEEK_SET);

	if(fileSize % PAGESIZE) {
		printf("-1\n");
		return -1;
	}
	if(fileSize < PAGESIZE) {
		printf("-1\n");
		return -1;
	}

	char tuples;
	while(fread(&tuples, sizeof(char), 1, fp)) {
	  ntuples = ntuples + tuples;
	  fseek(fp, PAGESIZE - sizeof(char), SEEK_CUR);
    }

	// Add variables and code here to work out
	// the total number of tuples

	printf("%d\n",ntuples);
	return 0;
}
