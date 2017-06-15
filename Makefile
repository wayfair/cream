# Michael Coates : mcoates@wayfair.com : @outerwear
#
# Makefile for Wayfair Cache Team Tools
#
# To compile with debug objects use 'make debug'

CC = gcc
CFLAGS = -Wall
ODIR= obj
SDIR = src

all: $(ODIR) dump prefix

debug: CFLAGS += -g -D DEBUG
debug: dump

$(ODIR):
	mkdir -p $(ODIR)

lzf_d.o:
	$(CC) $(CFLAGS) -c $(SDIR)/lzf_d.c -o $(ODIR)/lzf_d.o

dumpread.o:
	$(CC) $(CFLAGS) -c $(SDIR)/dumpread.c -o $(ODIR)/dumpread.o

prefix.o:
	$(CC) $(CFLAGS) -c $(SDIR)/prefix.c -o $(ODIR)/prefix.o

prefix: prefix.o
	$(CC) $(CFLAGS) $(ODIR)/prefix.o -o prefix

dump: lzf_d.o dumpread.o
	$(CC) $(CFLAGS) $(ODIR)/lzf_d.o $(ODIR)/dumpread.o -o dumpread

.PHONY : clean
clean:
	-rm dumpread
	-rm prefix
	-rm -rf $(ODIR)/*.o
