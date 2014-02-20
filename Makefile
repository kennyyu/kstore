# Project directory layout
#
# Makefile
# server.c
# client.c
# include/
#   *.h
# src/
#   *.c
# obj/
#   *.o
INCDIR = include
OBJDIR = obj
SRCDIR = src

CC = gcc
CFLAGS = -I$(INCDIR) -Wall -Werror -O1 -ggdb -std=gnu99 -m32
LIBS = -lm -lpthread

DEPS = $(wildcard $(INCDIR)/*.h)
SRCS = $(wildcard $(SRCDIR)/*.c)
OBJS = $(addprefix $(OBJDIR)/,$(notdir $(SRCS:.c=.o)))

$(OBJDIR)/%.o: $(SRCDIR)/%.c $(DEPS)
	mkdir -p $(OBJDIR)
	$(CC) -c -o $@ $< $(CFLAGS)

all: server client

server: $(OBJS) server.o
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

client: $(OBJS) client.o
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

clean:
	rm -rf $(OBJDIR) *~ core client server *.o

.PHONY: clean