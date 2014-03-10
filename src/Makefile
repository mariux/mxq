
PREFIX     = /usr
EPREFIX    = ${PREFIX}
SBINDIR    = ${EPREFIX}/sbin
BINDIR     = ${EPREFIX}/bin
LIBDIR     = ${EPREFIX}/lib
LIBEXECDIR = ${EPREFIX}/libexec
DATADIR    = ${PREFIX}/share
MANDIR     = ${DATADIR}/man
SYSCONFDIR = ${PREFIX}/etc


ifeq ($(USER),root)
    USER_SUBMIT  = $(shell id --user  nobody)
    GROUP_SUBMIT = $(shell id --group nobody)

    USER_LIST    = $(shell id --user  nobody)
    GROUP_LIST   = $(shell id --group nobody)
    
    USER_EXEC    = $(shell id --user  root)
    GROUP_EXEC   = $(shell id --group root)
    
    SUID_MODE    = 6755
else
    USER_SUBMIT  = $(shell id --user)
    GROUP_SUBMIT = $(shell id --group)
    
    USER_EXEC    = $(shell id --user)
    GROUP_EXEC   = $(shell id --group)

    SUID_MODE    = 0755
endif

# set sysconfdir /etc if prefix /usr
ifeq (${PREFIX},/usr)
    SYSCONFDIR = /etc
endif

# strip /mxq from LIBEXECDIR if set
ifeq ($(notdir ${LIBEXECDIR}),mxq)
    override LIBEXECDIR := $(patsubst %/,%,$(dir ${LIBEXECDIR}))
endif

DESTDIR=

quiet-command = $(if ${V},${1},$(if ${2},@echo ${2} && ${1}, @${1}))
quiet-install = $(call quiet-command,install -m ${1} ${2} ${3},"INSTALL [${1}] ${3}")
quiet-installdir = $(call quiet-command,install -m ${1} -d ${2},"MKDIR [${1}] ${2}")
quiet-installforuser = $(call quiet-command,install -m ${1} -o ${2} -g ${3} ${4} ${5},"INSTALL (${2}:${3}) [${1}] ${5}")

OBJECTS_MXQ_EXEC += mxq_exec.o
OBJECTS_MXQ_EXEC += bee_getopt.o
OBJECTS_MXQ_EXEC += mxq_mysql.o
OBJECTS_MXQ_EXEC += mxq_util.o

OBJECTS_MXQ_SUBMIT += mxq_submit.o
OBJECTS_MXQ_SUBMIT += bee_getopt.o
OBJECTS_MXQ_SUBMIT += mxq_mysql.o
OBJECTS_MXQ_SUBMIT += mxq_util.o

OBJECTS_MXQ_LIST += mxq_list.o
OBJECTS_MXQ_LIST += bee_getopt.o
OBJECTS_MXQ_LIST += mxq_mysql.o
OBJECTS_MXQ_LIST += mxq_util.o

LIBS_MYSQL += $(shell mysql_config --libs)

LIBS_MXQ_EXEC += $(LIBS_MYSQL)

LIBS_MXQ_SUBMIT += $(LIBS_MYSQL)

LIBS_MXQ_LIST += $(LIBS_MYSQL)

CFLAGS_MYSQL += $(shell mysql_config --cflags)

CFLAGS += -DMXQ_MYSQL_DEFAULT_FILE=\"${SYSCONFDIR}/mxq/mysql.cnf\"
CFLAGS += $(CFLAGS_MYSQL)
CFLAGS += -g

.PHONY: macheteinfach
macheteinfach: all

.PHONY: all
all: mxq_exec
all: mxq_submit
all: mxq_list

.PHONY: clean
clean:
	rm -f $(OBJECTS_MXQ_EXEC) 
	rm -f $(OBJECTS_MXQ_SUBMIT)
	rm -f $(OBJECTS_MXQ_LIST)

.PHONY: install
install: mxq_exec mxq_submit mxq_list
	$(call quiet-installdir,0755,${DESTDIR}${BINDIR})
	$(call quiet-installdir,0755,${DESTDIR}${SYSCONFDIR}/mxq)
	$(call quiet-installforuser,0755,$(USER_EXEC),$(GROUP_EXEC),mxq_exec,${DESTDIR}${BINDIR}/mxq_exec)
	$(call quiet-installforuser,$(SUID_MODE),$(USER_SUBMIT),$(GROUP_SUBMIT),mxq_submit,${DESTDIR}${BINDIR}/mxq_submit)
	$(call quiet-installforuser,$(SUID_MODE),$(USER_LIST),$(GROUP_LIST),mxq_list,${DESTDIR}${BINDIR}/mxq_list)

mxq_exec: $(OBJECTS_MXQ_EXEC)
	$(call quiet-command,gcc -o $@ $(OBJECTS_MXQ_EXEC) $(LIBS_MXQ_EXEC), "LD    $@")

mxq_submit: $(OBJECTS_MXQ_SUBMIT)
	$(call quiet-command,gcc -o $@ $(OBJECTS_MXQ_SUBMIT) $(LIBS_MXQ_SUBMIT), "LD    $@")

mxq_list: $(OBJECTS_MXQ_LIST)
	$(call quiet-command,gcc -o $@ $(OBJECTS_MXQ_LIST) $(LIBS_MXQ_LIST), "LD    $@")

%.o: %.c Makefile
	$(call quiet-command,${CC} ${CFLAGS} -o $@ -c $<,"CC    $@")

bee_getopt.o: bee_getopt.h

mxq_mysql.o: mxq_mysql.h
mxq_mysql.o: mxq.h

mxq_util.o: mxq_util.h
mxq_util.o: mxq.h

mxq_exec.o: mxq_mysql.h
mxq_exec.o: mxq_util.h
mxq_exec.o: mxq.h
mxq_exec.o: bee_getopt.h

mxq_submit.o: mxq_mysql.h
mxq_submit.o: mxq_util.h
mxq_submit.o: mxq.h
mxq_submit.o: bee_getopt.h

mxq_list.o: mxq.h

