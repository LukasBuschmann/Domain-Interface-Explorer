CC ?= cc
CFLAGS ?= -O3 -std=c11 -Wall -Wextra -Wpedantic
LDFLAGS ?=
LDLIBS ?= -lm -pthread

TARGET := interface_distance
SOURCE := interface_distance.c

.PHONY: all
all: $(TARGET)

$(TARGET): $(SOURCE)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(SOURCE) $(LDLIBS)
