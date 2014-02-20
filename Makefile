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

SERVER = server
CLIENT = client
COMMON = common

SERVER_INCDIR = $(SRCDIR)/$(SERVER)/$(INCDIR)
CLIENT_INCDIR = $(SRCDIR)/$(CLIENT)/$(INCDIR)
COMMON_INCDIR = $(SRCDIR)/$(COMMON)/$(INCDIR)

SERVER_SRCDIR = $(SRCDIR)/$(SERVER)
CLIENT_SRCDIR = $(SRCDIR)/$(CLIENT)
COMMON_SRCDIR = $(SRCDIR)/$(COMMON)

SERVER_OBJDIR = $(OBJDIR)/$(SERVER)
CLIENT_OBJDIR = $(OBJDIR)/$(CLIENT)
COMMON_OBJDIR = $(OBJDIR)/$(COMMON)

SERVER_DEPS = $(wildcard $(SERVER_INCDIR)/*.h)
SERVER_SRCS = $(wildcard $(SERVER_SRCDIR)/*.c)
SERVER_OBJS = $(addprefix $(SERVER_OBJDIR)/,$(notdir $(SERVER_SRCS:.c=.o)))

CLIENT_DEPS = $(wildcard $(CLIENT_INCDIR)/*.h)
CLIENT_SRCS = $(wildcard $(CLIENT_SRCDIR)/*.c)
CLIENT_OBJS = $(addprefix $(CLIENT_OBJDIR)/,$(notdir $(CLIENT_SRCS:.c=.o)))

COMMON_DEPS = $(wildcard $(COMMON_INCDIR)/*.h)
COMMON_SRCS = $(wildcard $(COMMON_SRCDIR)/*.c)
COMMON_OBJS = $(addprefix $(COMMON_OBJDIR)/,$(notdir $(COMMON_SRCS:.c=.o)))

CC = gcc
CFLAGS = -Wall -Werror -O1 -ggdb -std=gnu99 -m32
LIBS = -lm -lpthread

#DEPS = $(wildcard $(INCDIR)/*.h)
#SRCS = $(wildcard $(SRCDIR)/*.c)
#OBJS = $(addprefix $(OBJDIR)/,$(notdir $(SRCS:.c=.o)))

$(COMMON_OBJDIR)/%.o: $(COMMON_SRCDIR)/%.c $(COMMON_DEPS)
	mkdir -p $(COMMON_OBJDIR)
	$(CC) -c -o $@ $< $(CFLAGS) -I$(COMMON_INCDIR)

$(SERVER_OBJDIR)/%.o: $(SERVER_SRCDIR)/%.c $(SERVER_DEPS) $(COMMON_OBJS) $(COMMON_DEPS)
	mkdir -p $(SERVER_OBJDIR)
	$(CC) -c -o $@ $< $(CFLAGS) -I$(SERVER_INCDIR) -I$(COMMON_INCDIR)

$(CLIENT_OBJDIR)/%.o: $(CLIENT_SRCDIR)/%.c $(CLIENT_DEPS) $(COMMON_OBJS) $(COMMON_DEPS)
	mkdir -p $(CLIENT_OBJDIR)
	$(CC) -c -o $@ $< $(CFLAGS) -I$(CLIENT_INCDIR) -I$(COMMON_INCDIR)

#$(OBJDIR)/%.o: $(SRCDIR)/%.c $(DEPS)
#	mkdir -p $(OBJDIR)
#	$(CC) -c -o $@ $< $(CFLAGS)

all: server client

server: $(COMMON_OBJS) $(SERVER_OBJS) server.o
	$(CC) -o $@ $^ $(CFLAGS) -I$(SERVER_INCDIR) -I$(COMMON_INCDIR) $(LIBS)

client: $(COMMON_OBJS) $(CLIENT_OBJS) client.o
	$(CC) -o $@ $^ $(CFLAGS) -I$(CLIENT_INCDIR) -I$(COMMON_INCDIR) $(LIBS)

clean:
	rm -rf $(OBJDIR) *~ core client server *.o

.PHONY: clean