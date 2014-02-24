# Project directory layout
#
# Makefile
# server.c
# client.c
# src/
#   client/
#     include/
#       *.h
#     *.c
#   server/
#     include/
#       *.h
#     *.c
#   common/
#     include/
#       *.h
#     *.c
#   *.h
# obj/
#   client/
#   server/
#   common/
# test/
#   *.c
# testbin/
#   *_test
INCDIR = include
OBJDIR = obj
SRCDIR = src

SERVER = server
CLIENT = client
COMMON = common

TEST_SRCDIR = test
TEST_OBJDIR = testbin

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

TEST_SRCS = $(wildcard $(TEST_SRCDIR)/*.c)
TEST_OBJS = $(addprefix $(TEST_OBJDIR)/,$(notdir $(TEST_SRCS:.c=.o)))
TEST_BINS = $(addprefix $(TEST_OBJDIR)/,$(notdir $(TEST_SRCS:.c=)))

CC = gcc
CFLAGS = -Wall -Werror -ggdb -std=gnu99 -m32
LIBS = -lm -lpthread

$(TEST_OBJDIR)/%_test: $(TEST_OBJDIR)/%_test.o $(COMMON_OBJS) $(SERVER_OBJS) $(CLIENT_OBJS)
	$(CC) -o $@ $^ $(CFLAGS) -I$(COMMON_INCDIR) -I$(SERVER_INCDIR) -I$(CLIENT_INCDIR) $(LIBS)

$(TEST_OBJDIR)/%.o: $(TEST_SRCDIR)/%.c $(COMMON_DEPS) $(SERVER_DEPS) $(CLIENT_DEPS)
	@mkdir -p $(TEST_OBJDIR)
	$(CC) -c -o $@ $< $(CFLAGS) -I$(COMMON_INCDIR) -I$(SERVER_INCDIR) -I$(CLIENT_INCDIR)

$(COMMON_OBJDIR)/%.o: $(COMMON_SRCDIR)/%.c $(COMMON_DEPS)
	@mkdir -p $(COMMON_OBJDIR)
	$(CC) -c -o $@ $< $(CFLAGS) -I$(COMMON_INCDIR)

$(SERVER_OBJDIR)/%.o: $(SERVER_SRCDIR)/%.c $(SERVER_DEPS) $(COMMON_OBJS) $(COMMON_DEPS)
	@mkdir -p $(SERVER_OBJDIR)
	$(CC) -c -o $@ $< $(CFLAGS) -I$(SERVER_INCDIR) -I$(COMMON_INCDIR)

$(CLIENT_OBJDIR)/%.o: $(CLIENT_SRCDIR)/%.c $(CLIENT_DEPS) $(COMMON_OBJS) $(COMMON_DEPS)
	@mkdir -p $(CLIENT_OBJDIR)
	$(CC) -c -o $@ $< $(CFLAGS) -I$(CLIENT_INCDIR) -I$(COMMON_INCDIR)

all: server client

test: server client $(TEST_BINS)
	@echo
	@echo ">>> STARTING TESTS"
	@for t in `ls $(TEST_OBJDIR)/*_test`; do \
		echo "=== running $$t... ==="; \
		./$$t; \
		echo "=== finished $$t ==="; \
	done
	@echo ">>> TESTS DONE"

server: $(COMMON_OBJS) $(SERVER_OBJS) server.o
	$(CC) -o $@ $^ $(CFLAGS) -I$(SERVER_INCDIR) -I$(COMMON_INCDIR) $(LIBS)

client: $(COMMON_OBJS) $(CLIENT_OBJS) client.o
	$(CC) -o $@ $^ $(CFLAGS) -I$(CLIENT_INCDIR) -I$(COMMON_INCDIR) $(LIBS)

clean:
	rm -rf $(OBJDIR) *~ core client server *.o $(TEST_OBJDIR)

.PHONY: clean