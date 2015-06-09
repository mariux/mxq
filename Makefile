
MXQ_VERSION_MAJOR = 0
MXQ_VERSION_MINOR = 5
MXQ_VERSION_PATCH = 1
MXQ_VERSION_EXTRA = "beta"

MXQ_VERSION = ${MXQ_VERSION_MAJOR}.${MXQ_VERSION_MINOR}.${MXQ_VERSION_PATCH}
MXQ_VERSIONFULL = "MXQ v${MXQ_VERSION} (${MXQ_VERSION_EXTRA})"
MXQ_VERSIONDATE = 2013-2015

##############################################################################

PREFIX     = /usr
EPREFIX    = ${PREFIX}
SBINDIR    = ${EPREFIX}/sbin
BINDIR     = ${EPREFIX}/bin
LIBDIR     = ${EPREFIX}/lib
LIBEXECDIR = ${EPREFIX}/libexec
DATADIR    = ${PREFIX}/share
MANDIR     = ${DATADIR}/man
SYSCONFDIR = ${PREFIX}/etc

DESTDIR=

##############################################################################

UNPRIV_USER = nobody

##############################################################################

### set sysconfdir /etc if prefix /usr || /usr/local
ifneq (, $(filter /usr /usr/local, ${PREFIX}))
    SYSCONFDIR = /etc
endif

##############################################################################

### strip /mxq from LIBEXECDIR if set
ifeq ($(notdir ${LIBEXECDIR}),mxq)
    override LIBEXECDIR := $(patsubst %/,%,$(dir ${LIBEXECDIR}))
endif

##############################################################################

MXQ_MYSQL_DEFAULT_FILE  = ${SYSCONFDIR}/mxq/mysql.cnf
MXQ_MYSQL_DEFAULT_GROUP = mxqclient

MXQ_INITIAL_PATH = /sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin

CFLAGS_MXQ_MYSQL_DEFAULT_FILE  = -DMXQ_MYSQL_DEFAULT_FILE=\"$(MXQ_MYSQL_DEFAULT_FILE)\"
CFLAGS_MXQ_MYSQL_DEFAULT_GROUP = -DMXQ_MYSQL_DEFAULT_GROUP=\"$(MXQ_MYSQL_DEFAULT_GROUP)\"
CFLAGS_MXQ_INITIAL_PATH        = -DMXQ_INITIAL_PATH=\"$(MXQ_INITIAL_PATH)\"

MYSQL_CONFIG = mysql_config

OS_RELEASE = $(shell ./os-release)

# special defaults for mariux64
ifeq (${OS_RELEASE}, mariux64)
   MXQ_INITIAL_PATH := ${MXQ_INITIAL_PATH}:/usr/local/package/bin

endif

##############################################################################

UID_SERVER := $(shell id --user  $(USER))
GID_SERVER := $(shell id --group $(USER))

ifeq ($(USER),root)
    UID_CLIENT := $(shell id --user  ${UNPRIV_USER})
    GID_CLIENT := $(shell id --group ${UNPRIV_USER})

    SUID_MODE    = 6755
else
    UID_CLIENT := $(shell id --user  $(USER))
    GID_CLIENT := $(shell id --group $(USER))

    SUID_MODE    = 0755
endif

##############################################################################

CFLAGS_MYSQL := $(shell $(MYSQL_CONFIG) --cflags)
LDLIBS_MYSQL := $(shell $(MYSQL_CONFIG) --libs)

CFLAGS_MYSQL += ${CFLAGS_MXQ_MYSQL_DEFAULT_FILE}
CFLAGS_MYSQL += ${CFLAGS_MXQ_MYSQL_DEFAULT_GROUP}

CFLAGS += -g
CFLAGS += -Wall
CFLAGS += -Wno-unused-variable
CFLAGS += -Wno-unused-function
CFLAGS += -DMXQ_VERSION=\"${MXQ_VERSION}\"
CFLAGS += -DMXQ_VERSIONFULL=\"${MXQ_VERSIONFULL}\"
CFLAGS += -DMXQ_VERSIONDATE=\"${MXQ_VERSIONDATE}\"
CFLAGS += -DMXQ_VERSIONEXTRA=\"${MXQ_VERSIONEXTRA}\"

########################################################################

quiet-command = $(if ${V},${1},$(if ${2},@echo ${2} && ${1}, @${1}))
quiet-install = $(call quiet-command,install -m ${1} ${2} ${3},"INSTALL ${3} [mode=${1}]")
quiet-installdir = $(call quiet-command,install -m ${1} -d ${2},"  MKDIR ${2} [mode=${1}]")
quiet-installforuser = $(call quiet-command,install -m ${1} -o ${2} -g ${3} ${4} ${5},"INSTALL ${5} (user=${2} group=${3}) [mode=${1}]")

########################################################################

%.o: %.c Makefile
	$(call quiet-command,${CC} ${CFLAGS} -o $@ -c $<,"     CC $@")

%: %.c

%: %.o
	$(call quiet-command,${CC} -o $@ $^ $(LDFLAGS) $(LDLIBS), "   LINK $@")

########################################################################

.PHONY: all
.PHONY: build

all: build

########################################################################

.PHONY: test
test:
	@for i in $^ ; do \
		echo "   TEST $$i" ; \
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
				echo "  CLEAN $$i" ; \
			fi ; \
			rm -f $$i ; \
        fi \
    done

########################################################################

.PHONY: fix
fix:
	@for i in *.c *.h Makefile mysql/create_tables mxqdctl-hostconfig.sh ; do \
	    if grep -q -m 1 -E '\s+$$' $$i ; then \
	        echo "FIX   $$i" ; \
	        sed -i $$i -e 's/\s*$$//g' ; \
	    fi \
	done

########################################################################

.PHONY: install
install:: build
	$(call quiet-installdir,0755,${DESTDIR}${BINDIR})
	$(call quiet-installdir,0755,${DESTDIR}${SBINDIR})
	$(call quiet-installdir,0755,${DESTDIR}${SYSCONFDIR}/mxq)

########################################################################

### mx_log.h ----------------------------------------------------------

mx_log.h += mx_log.h

### mx_util.h ----------------------------------------------------------

mx_util.h += mx_util.h

### mx_flock.h ---------------------------------------------------------

mx_flock.h += mx_flock.h

### mx_mysql.h ---------------------------------------------------------

mx_mysql.h += mx_mysql.h
mx_mysql.h += $(mx_util.h)

### mxq.h --------------------------------------------------------------

mx_mxq.h += mx_mxq.h

### mxq_mysql.h --------------------------------------------------------

mxq_mysql.h += mxq_mysql.h
mxq_mysql.h += $(mxq_util.h)

### mxq_util.h ---------------------------------------------------------

mxq_util.h += mxq_util.h
mxq_util.h += $(mx_log.h)

### mxq_group.h --------------------------------------------------------

mxq_group.h += mxq_group.h

### mxq_job.h ---------------------------------------------------------

mxq_job.h += mxq_job.h
mxq_job.h += mxq_group.h

### mxqd.h ------------------------------------------------------------

mxqd.h += mxqd.h

### mx_getopt.h ------------------------------------------------------

mx_getopt.h += mx_getopt.h

########################################################################

### mx_getopt.o -------------------------------------------------------

mx_getopt.o: $(mx_getopt.h)

clean: CLEAN += mx_getopt.o

### mx_log.o ----------------------------------------------------------

mx_log.o: $(mx_log.h)

clean: CLEAN += mx_log.o

### mx_util.o ----------------------------------------------------------

mx_util.o: $(mx_log.h)

clean: CLEAN += mx_util.o

### mx_flock.o -------------------------------------------------------

mx_flock.o: $(mx_flock.h)

clean: CLEAN += mx_flock.o

### mx_mysql.o ---------------------------------------------------------

mx_mysql.o: $(mx_mysql.h)
mx_mysql.o: $(mx_util.h)
mx_mysql.o: $(mx_log.h)
mx_mysql.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mx_mysql.o

### mxq_log.o ----------------------------------------------------------

mxq_log.o: $(mx_log.h)

clean: CLEAN += mxq_log.o

### mxq_mysql.o --------------------------------------------------------

mxq_mysql.o: $(mx_log.h)
mxq_mysql.o: $(mxq_mysql.h)
mxq_mysql.o: $(mxq_util.h)
mxq_mysql.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxq_mysql.o

### mxqdump.o ---------------------------------------------------

mxqdump.o: $(mx_log.h)
mxqdump.o: $(mxq_util.h)
mxqdump.o: $(mxq_mysql.h)
mxqdump.o: $(mx_getopt.h)
mxqdump.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxqdump.o

### mxqkill.o ---------------------------------------------------

mxqkill.o: $(mx_log.h)
mxqkill.o: $(mx_util.h)
mxqkill.o: $(mx_mysql.h)
mxqkill.o: $(mx_getopt.h)
mxqkill.o: $(mxq.h)
mxqkill.o: $(mxq_group.h)
mxqkill.o: $(mxq_job.h)
mxqkill.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxqkill.o

### mxq_util.o ---------------------------------------------------------

mxq_util.o: $(mx_log.h)
mxq_util.o: $(mxq_util.h)
mxq_util.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxq_util.o

### mxq_group.o --------------------------------------------------------

mxq_group.o: $(mx_log.h)
mxq_group.o: $(mxq_group.h)
mxq_group.o: $(mxq_mysql.h)
mxq_group.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxq_group.o

### mxq_job.o ----------------------------------------------------------

mxq_job.o: $(mx_util.h)
mxq_job.o: $(mx_log.h)
mxq_job.o: $(mxq_job.h)
mxq_job.o: $(mxq_group.h)
mxq_job.o: $(mxq_mysql.h)
mxq_job.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxq_job.o

### mxqd.o -------------------------------------------------------------

mxqd.o: $(mx_getopt.h)
mxqd.o: $(mx_flock.h)
mxqd.o: $(mx_util.h)
mxqd.o: $(mx_log.h)
mxqd.o: $(mxqd.h)
mxqd.o: $(mxq_group.h)
mxqd.o: $(mxq_job.h)
mxqd.o: $(mxq_mysql.h)
mxqd.o: CFLAGS += $(CFLAGS_MYSQL)
mxqd.o: CFLAGS += $(CFLAGS_MXQ_INITIAL_PATH)
mxqd.o: CFLAGS += -Wno-unused-but-set-variable

clean: CLEAN += mxqd.o

### mxqsub.o -------------------------------------------------------

mxqsub.o: $(mx_getopt.h)
mxqsub.o: $(mx_util.h)
mxqsub.o: $(mx_log.h)
mxqsub.o: $(mx_mysql.h)
mxqsub.o: $(mxq.h)
mxqsub.o: $(mxq_group.h)
mxqsub.o: $(mxq_job.h)
mxqsub.o: $(mxq_util.h)
mxqsub.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxqsub.o

########################################################################

### mxqd ------------------------------------------------------------

mxqd: mx_flock.o
mxqd: mx_util.o
mxqd: mx_log.o
mxqd: mxq_log.o
mxqd: mx_getopt.o
mxqd: mxq_group.o
mxqd: mxq_job.o
mxqd: mxq_util.o
mxqd: mxq_mysql.o
mxqd: mx_mysql.o
mxqd: LDLIBS += $(LDLIBS_MYSQL)

build: mxqd

clean: CLEAN += mxqd

install:: mxqd
	$(call quiet-installforuser,0755,$(UID_SERVER),$(GID_SERVER),mxqd,${DESTDIR}${SBINDIR}/mxqd)

### mxqsub ------------------------------------------------------------

mxqsub: mx_getopt.o
mxqsub: mxq_util.o
mxqsub: mx_util.o
mxqsub: mx_log.o
mxqsub: mx_mysql.o
mxqsub: LDLIBS += $(LDLIBS_MYSQL)

build: mxqsub

clean: CLEAN += mxqsub

install:: mxqsub
	$(call quiet-installforuser,$(SUID_MODE),$(UID_CLIENT),$(GID_CLIENT),mxqsub,${DESTDIR}${BINDIR}/mxqsub)

### mxqdump -----------------------------------------------------

mxqdump: mx_log.o
mxqdump: mx_mysql.o
mxqdump: mxq_group.o
mxqdump: mxq_job.o
mxqdump: mxq_mysql.o
mxqdump: mxq_util.o
mxqdump: mx_util.o
mxqdump: mx_getopt.o
mxqdump: LDLIBS += $(LDLIBS_MYSQL)
mxqdump: CFLAGS += -Wunused-function

build: mxqdump

clean: CLEAN += mxqdump

install:: mxqdump
	$(call quiet-installforuser,$(SUID_MODE),$(UID_CLIENT),$(GID_CLIENT),mxqdump,${DESTDIR}${BINDIR}/mxqdump)

### mxqkill -----------------------------------------------------

mxqkill: mx_log.o
mxqkill: mx_mysql.o
mxqkill: mx_util.o
mxqkill: mx_getopt.o
mxqkill: LDLIBS += $(LDLIBS_MYSQL)

build: mxqkill

clean: CLEAN += mxqkill

install:: mxqkill
	$(call quiet-installforuser,$(SUID_MODE),$(UID_CLIENT),$(GID_CLIENT),mxqkill,${DESTDIR}${BINDIR}/mxqkill)

########################################################################

install:: mxqdctl-hostconfig.sh
	$(call quiet-install,0755,mxqdctl-hostconfig.sh,${DESTDIR}${SBINDIR}/mxqdctl-hostconfig)

########################################################################

test_mx_util.o: $(mx_util.h)
clean: CLEAN += test_mx_util.o

test_mx_util: mx_util.o
test_mx_util: mx_log.o
clean: CLEAN += test_mx_util

test: test_mx_util

test_mx_log.o: $(mx_log.h)
clean: CLEAN += test_mx_log.o

test_mx_log: mx_log.o
clean: CLEAN += test_mx_log

test: test_mx_log

test_mx_mysql.o: $(mx_mysql.h)
clean: CLEAN += test_mx_mysql.o

test_mx_mysql: mx_mysql.o
test_mx_mysql: mx_log.o
test_mx_mysql: mx_util.o
test_mx_mysql: LDLIBS += $(LDLIBS_MYSQL)
clean: CLEAN += test_mx_mysql
