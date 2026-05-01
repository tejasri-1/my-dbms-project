# contrib/auto_index_advisor/Makefile

MODULE_big = auto_index_advisor
OBJS = \
	$(WIN32RES) \
	auto_index_advisor.o

EXTENSION = auto_index_advisor
DATA = auto_index_advisor--1.0.sql auto_index_advisor--1.1.sql auto_index_advisor--1.0--1.1.sql
PGFILEDESC = "auto_index_advisor - background worker for online index-advisor observation"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/auto_index_advisor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
