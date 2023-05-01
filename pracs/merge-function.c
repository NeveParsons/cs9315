// to merge two sorted files

// get_page(rel, pid, buf) ... read specified page into buffer
// put_page(rel, pid, buf) ... write a page to disk, at position pid
// clear_page(rel, buf) ... make page have zero tuples
// sort_page(buf) ... in-memory sort of tuples in page
// nPages(rel),    nTuples(buf),    get_tuple(buf, tid)

r1 = openRel("r1","r"); buf1 = get_page(r1,0);
r2 = openRel("r2","r"); buf2 = get_page(r2,0);

pid1 = 0; tid1 = 0; pid2 = 0; tid2 = 0;

tup1 = get_tuple(buf1,0); tup2 = get_tuple(buf2,0);

//for two sorted files, using 3 buffers, with b1=5, b2=3

while (tup1 != NULL && tup2 != NULL) {
	if (tup1 < tup2) {
		insert_tuple(outbuf, tup1);
		if (tid1 >= nTuples(buf1)) // end of page
			pid1++;
			buf1 = get_page(r1,pid1);
			tid1 = -1;
		}
		tid1++;
		tup1 = get_tuple(buf1, tid1);
	}
	else {
		insert_tuple(outbuf, tup2);
		if (tid2 >= nTuples(buf2)) // end of page
			pid2++;
			buf2 = get_page(r2,pid2);
			tid2 = -1;
		}
		tid2++;
		tup2 = get_tuple(buf2, tid2);
	}
	if (nTuples(outbuf) >= capacity(outbuf)) {
		put_page(outbuf);
		clear(outbuf);
	}
}

//for one unsorted file

while (tup1 != NULL) {
	insert_tuple(outbuf, tup1);
	if (tid1 >= nTuples(buf1)) // end of page
		pid1++;
		buf1 = get_page(r1,pid1);
		tid1 = -1;
	}
	tid1++;
	tup1 = get_tuple(buf1, tid1);
	if (nTuples(outbuf) >= capacity(outbuf)) {
		put_page(outbuf);
		clear(outbuf);
	}
}

while (tup2 != NULL) {
	insert_tuple(outbuf, tup2);
	if (tid2 >= nTuples(buf2)) // end of page
		pid2++;
		buf2 = get_page(r2,pid2);
		tid2 = -1;
	}
	tid2++;
	tup2 = get_tuple(buf2, tid2);
	if (nTuples(outbuf) >= capacity(outbuf)) {
		put_page(outbuf);
		clear(outbuf);
	}
}