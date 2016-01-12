PG_CPPFLAGS = --std=c99
MODULE_big = tkd
OBJS =  tkd.o PQueue.o
DATA_built = tkd.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

PQueue.o: PQueue.c PQueue.h
