// COMP9315 22T1 Final Exam Q3
// Read tuples from stdin and store in no-frills file
// Start from empty file, add new pages as needed

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "no-frills.h"

int main(int argc, char **argv)
{
	if (argc < 3) {
		fprintf(stderr, "Usage: %s DataFile TupleFile\n", argv[0]);
		exit(1);
	}
	unsigned int mode = S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH;
	int fd = open(argv[1],O_RDWR|O_CREAT|O_TRUNC,mode);
	if (fd < 0) {
		fprintf(stderr, "Can't open data file.\n");
		exit(1);
	}
	FILE *input = fopen(argv[2],"r");
	if (input == NULL) {
		fprintf(stderr, "Can't open data file.\n");
		exit(1);
	}

	FILE *output = fopen(argv[1],"w");
	int tuples = 0;
	int currPage = 0; 
	

	char tuple[MAXTUPLEN];

	fwrite(&tuples, sizeof(char), 1, output);
	int currPageBytes = 1;

    while(fgets(tuple,MAXTUPLEN,input)){
		if(strlen(tuple) > (PAGESIZE - currPageBytes)) {
			for(int i = 0; i < (PAGESIZE - currPageBytes); i++) {
				fputc('\0', output);
			}
			fseek(output, currPage*PAGESIZE, SEEK_SET);
			fputc(tuples, output);
			currPage++;
			currPageBytes = 1;
			fseek(output, currPage*PAGESIZE, SEEK_SET);
			tuples = 0;
		}
		tuple[strlen(tuple)-1] = '\0';
		tuples++;
		fwrite(&tuple, sizeof(char)*strlen(tuple), 1, output);
		fputc('\0', output);
		currPageBytes = currPageBytes + strlen(tuple) + 1;
	}
	for(int i = 0; i < (PAGESIZE - currPageBytes); i++) {
		fputc('\0', output);
	}
	fseek(output, currPage*PAGESIZE, SEEK_SET);
	fputc(tuples, output);	

	// Add variables and code here to read tuples from
	// input and append them to the "no-frills" file

	return 0;
}
