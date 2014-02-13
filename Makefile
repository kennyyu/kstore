# Project directory layout
#
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
CFLAGS = -I$(INCDIR) -Wall -Werror -O1 -ggdb -std=c99
LIBS = -lm -lpthread

DEPS = $(wildcard $(INCDIR)/*.h)
SRCS = $(wildcard $(SRCDIR)/*.c)
OBJS = $(addprefix $(OBJDIR)/,$(notdir $(SRCS:.c=.o)))

$(OBJDIR)/%.o: $(SRCDIR)/%.c $(DEPS)
	mkdir -p $(OBJDIR)
	$(CC) -c -o $@ $< $(CFLAGS)

all: server client

server: server.o $(OBJS)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

client: client.o $(OBJS)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

clean:
	rm -rf $(OBJDIR) *~ core client server *.o

.PHONY: clean