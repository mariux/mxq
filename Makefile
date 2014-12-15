
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

    USER_LIST    = $(shell id --user)
    GROUP_LIST   = $(shell id --group)

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

MXQ_MYSQL_DEFAULT_FILE = ${SYSCONFDIR}/mxq/mysql.cnf
MXQ_INITIAL_PATH = /sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin

CFLAGS_MXQ_MYSQL_DEFAULT_FILE = -DMXQ_MYSQL_DEFAULT_FILE=\"$(MXQ_MYSQL_DEFAULT_FILE)\"
CFLAGS_MXQ_INITIAL_PATH = -DMXQ_INITIAL_PATH=\"$(MXQ_INITIAL_PATH)\"

MYSQL_CONFIG = mysql_config

OS_RELEASE = $(shell ./os-release)

# special defaults for mariux64
ifeq (${OS_RELEASE}, mariux64)
   MXQ_INITIAL_PATH := ${MXQ_INITIAL_PATH}:/usr/local/package/bin
endif

CFLAGS_MYSQL += $(shell $(MYSQL_CONFIG) --cflags)
LDLIBS_MYSQL += $(shell $(MYSQL_CONFIG) --libs)

CFLAGS += -g
CFLAGS += -Wall
CFLAGS += -Wno-unused-variable

########################################################################

quiet-command = $(if ${V},${1},$(if ${2},@echo ${2} && ${1}, @${1}))
quiet-install = $(call quiet-command,install -m ${1} ${2} ${3},"INSTALL [${1}] ${3}")
quiet-installdir = $(call quiet-command,install -m ${1} -d ${2},"MKDIR [${1}] ${2}")
quiet-installforuser = $(call quiet-command,install -m ${1} -o ${2} -g ${3} ${4} ${5},"INSTALL (${2}:${3}) [${1}] ${5}")

########################################################################

%.o: %.c Makefile
	$(call quiet-command,${CC} ${CFLAGS} -o $@ -c $<,"CC    $@")

%: %.c

%: %.o
	$(call quiet-command,${CC} -o $@ $^ $(LDFLAGS) $(LDLIBS), "LINK  $@")

########################################################################

.PHONY: all
all: build

.PHONY: build

.PHONY: test
test:
	@for i in $^ ; do \
		echo "TEST  $$i" ; \
		./$$i ; \
	done



########################################################################

.PHONY: clean
clean:
	@for i in $(CLEAN) ; do \
	    if [ -e "$$i" ] ; then \
			if [ "$(V)" = 1 ] ; then \
				echo "rm -f $$i" ; \
			else \
				echo "CLEAN $$i" ; \
			fi ; \
			rm -f $$i ; \
        fi \
    done

########################################################################

.PHONY: fix
fix:
	@for i in *.c *.h Makefile mysql/create_tables ; do \
	    if grep -q -m 1 -E '\s+$$' $$i ; then \
	        echo "FIX   $$i" ; \
	        sed -i $$i -e 's/\s*$$//g' ; \
	    fi \
	done

########################################################################

.PHONY: install
install::
	$(call quiet-installdir,0755,${DESTDIR}${BINDIR})
	$(call quiet-installdir,0755,${DESTDIR}${SBINDIR})
	$(call quiet-installdir,0755,${DESTDIR}${SYSCONFDIR}/mxq)

########################################################################

### mx_util.h ----------------------------------------------------------

mx_util.h += mx_util.h

### mx_flock.h ---------------------------------------------------------

mx_flock.h += mx_flock.h

### mxq.h --------------------------------------------------------------

mxq.h += mxq.h
mxq.h += $(mxq_group.h)
mxq.h += $(mxq_job.h)

### mxq_mysql.h --------------------------------------------------------

mxq_mysql.h += mxq_mysql.h
mxq_mysql.h += $(mxq_util.h)

### mxq_util.h ---------------------------------------------------------

mxq_util.h += mxq_util.h
mxq_util.h += $(mxq.h)

### mxq_group.h --------------------------------------------------------

mxq_group.h += mxq_group.h

### mxq_job.h ---------------------------------------------------------

mxq_job.h += mxq_job.h

### mxqd.h ------------------------------------------------------------

mxqd.h += mxqd.h

### bee_getopt.h ------------------------------------------------------

bee_getopt.h += bee_getopt.h

########################################################################

### bee_getopt.o -------------------------------------------------------

bee_getopt.o: $(bee_getopt.h)

clean: CLEAN += bee_getopt.o

### mx_util.o ----------------------------------------------------------

clean: CLEAN += mx_util.o

### mx_flock.o -------------------------------------------------------

mx_flock.o: $(mx_flock.h)

clean: CLEAN += mx_flock.o

### mxq_mysql.o --------------------------------------------------------

mxq_mysql.o: $(mxq_mysql.h)
mxq_mysql.o: $(mxq_util.h)
mxq_mysql.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxq_mysql.o

### mxq_exec.o ---------------------------------------------------------

mxq_exec.o: $(mxq_mysql.h)
mxq_exec.o: $(mxq_util.h)
mxq_exec.o: $(bee_getopt.h)

clean: CLEAN += mxq_exec.o

### mxq_submit.o -------------------------------------------------------

mxq_submit.o: $(mxq_mysql.h)
mxq_submit.o: $(mxq_util.h)
mxq_submit.o: $(bee_getopt.h)

clean: CLEAN += mxq_submit.o

### mxq_list.o ---------------------------------------------------------

mxq_list.o: $(mxq_mysql.h)
mxq_list.o: $(mxq_util.h)
mxq_list.o: $(bee_getopt.h)

clean: CLEAN += mxq_list.o

### mxqdump.o ---------------------------------------------------

mxqdump.o: $(mxq_util.h)
mxqdump.o: $(mxq_mysql.h)
mxqdump.o: CFLAGS += $(CFLAGS_MYSQL)
mxqdump.o: CFLAGS += $(CFLAGS_MXQ_MYSQL_DEFAULT_FILE)

clean: CLEAN += mxqdump.o

### mxq_job_dump.o -----------------------------------------------------

mxq_job_dump.o: $(mxq_util.h)
mxq_job_dump.o: $(mxq_mysql.h)

clean: CLEAN += mxq_job_dump.o

### mxq_util.o ---------------------------------------------------------

mxq_util.o: $(mxq_util.h)
mxq_util.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxq_util.o

### mxq_group.o --------------------------------------------------------

mxq_group.o: $(mxq_group.h)
mxq_group.o: $(mxq_mysql.h)
mxq_group.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxq_group.o

### mxq_job.o ----------------------------------------------------------

mxq_job.o: $(mx_util.h)
mxq_job.o: $(mxq.h)
mxq_job.o: $(mxq_job.h)
mxq_job.o: $(mxq_group.h)
mxq_job.o: $(mxq_mysql.h)
mxq_job.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxq_job.o

### mxqd.o -------------------------------------------------------------

mxqd.o: $(bee_getopt.h)
mxqd.o: $(mx_flock.h)
mxqd.o: $(mx_util.h)
mxqd.o: $(mxqd.h)
mxqd.o: $(mxq_group.h)
mxqd.o: $(mxq_job.h)
mxqd.o: $(mxq_mysql.h)
mxqd.o: CFLAGS += $(CFLAGS_MYSQL)
mxqd.o: CFLAGS += $(CFLAGS_MXQ_MYSQL_DEFAULT_FILE)
mxqd.o: CFLAGS += $(CFLAGS_MXQ_INITIAL_PATH)
mxqd.o: CFLAGS += -Wno-unused-but-set-variable

clean: CLEAN += mxqd.o

### mxqsub.o -------------------------------------------------------

mxqsub.o: $(bee_getopt.h)
mxqsub.o: $(mx_util.h)
mxqsub.o: $(mxq_mysql.h)
mxqsub.o: CFLAGS += $(CFLAGS_MYSQL)
mxqsub.o: CFLAGS += $(CFLAGS_MXQ_MYSQL_DEFAULT_FILE)

clean: CLEAN += mxqsub.o

########################################################################

### mxqd ------------------------------------------------------------

mxqd: mx_flock.o
mxqd: mx_util.o
mxqd: bee_getopt.o
mxqd: mxq_group.o
mxqd: mxq_job.o
mxqd: mxq_util.o
mxqd: mxq_mysql.o
mxqd: LDLIBS += $(LDLIBS_MYSQL)

build: mxqd

clean: CLEAN += mxqd

install:: mxqd
	$(call quiet-installforuser,0755,$(USER_EXEC),$(GROUP_EXEC),mxqd,${DESTDIR}${SBINDIR}/mxqd)

### mxq_exec -----------------------------------------------------------

mxq_exec: bee_getopt.o
mxq_exec: mxq_mysql.o
mxq_exec: mxq_util.o
mxq_exec: LDLIBS += $(LDLIBS_MYSQL)

#build: mxq_exec
clean: CLEAN += mxq_exec

#install:: mxq_exec
#	$(call quiet-installforuser,0755,$(USER_EXEC),$(GROUP_EXEC),mxq_exec,${DESTDIR}${BINDIR}/mxq_exec)

### mxq_submit ---------------------------------------------------------

mxq_submit: bee_getopt.o
mxq_submit: mxq_mysql.o
mxq_submit: mxq_util.o
mxq_submit: LDLIBS += $(LDLIBS_MYSQL)

#build: mxq_submit

clean: CLEAN += mxq_submit

#install:: mxq_submit
#	$(call quiet-installforuser,$(SUID_MODE),$(USER_SUBMIT),$(GROUP_SUBMIT),mxq_submit,${DESTDIR}${BINDIR}/mxq_submit)

### mxqsub ------------------------------------------------------------

mxqsub: bee_getopt.o
mxqsub: mxq_mysql.o
mxqsub: mxq_util.o
mxqsub: LDLIBS += $(LDLIBS_MYSQL)

build: mxqsub

clean: CLEAN += mxqsub

install:: mxqsub
	$(call quiet-installforuser,$(SUID_MODE),$(USER_SUBMIT),$(GROUP_SUBMIT),mxqsub,${DESTDIR}${BINDIR}/mxqsub)

### mxq_list -----------------------------------------------------------

mxq_list: bee_getopt.o
mxq_list: mxq_mysql.o
mxq_list: mxq_util.o
mxq_list: LDLIBS += $(LDLIBS_MYSQL)

#build: mxq_list

clean: CLEAN += mxq_list

#install:: mxq_list
#	$(call quiet-installforuser,$(SUID_MODE),$(USER_LIST),$(GROUP_LIST),mxq_list,${DESTDIR}${BINDIR}/mxq_list)

### mxqdump -----------------------------------------------------

mxqdump: mxq_group.o
mxqdump: mxq_mysql.o
mxqdump: mxq_util.o
mxqdump: LDLIBS += $(LDLIBS_MYSQL)

build: mxqdump

clean: CLEAN += mxqdump

install:: mxqdump
	$(call quiet-installforuser,$(SUID_MODE),$(USER_SUBMIT),$(GROUP_SUBMIT),mxqdump,${DESTDIR}${BINDIR}/mxqdump)

### mxq_job_dump -------------------------------------------------------

mxq_job_dump: mxq_mysql.o
mxq_job_dump: mxq_util.o
mxq_job_dump: LDLIBS += $(LDLIBS_MYSQL)

build: mxq_job_dump

clean: CLEAN += mxq_job_dump

########################################################################

test_mx_util.o: $(mx_util.h)
clean: CLEAN += test_mx_util.o

test_mx_util: mx_util.o
clean: CLEAN += test_mx_util

test: test_mx_util

