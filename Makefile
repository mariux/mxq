MXQ_VERSION_MAJOR = 0
MXQ_VERSION_MINOR = 13
MXQ_VERSION_PATCH = 0
MXQ_VERSION_EXTRA = "beta"
MXQ_VERSIONDATE = 2013-2015

MXQ_VERSION_GIT := $(shell git describe --long 2>/dev/null)

MXQ_VERSION = ${MXQ_VERSION_MAJOR}.${MXQ_VERSION_MINOR}.${MXQ_VERSION_PATCH}
ifeq (${MXQ_VERSION_GIT},)
    MXQ_VERSIONFULL = "MXQ v${MXQ_VERSION} (${MXQ_VERSION_EXTRA})"
else
    MXQ_VERSIONFULL = "MXQ v${MXQ_VERSION} (${MXQ_VERSION_EXTRA}) [${MXQ_VERSION_GIT}]"
endif


########################################################################

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

########################################################################

UNPRIV_USER = nobody

HTTP_PORT  = 3000
HTTP_USER  = nobody
HTTP_GROUP = nogroup

########################################################################

### set sysconfdir /etc if prefix /usr || /usr/local
ifneq (, $(filter /usr /usr/local, ${PREFIX}))
    SYSCONFDIR = /etc
endif

########################################################################

### strip /mxq from LIBEXECDIR if set
ifeq ($(notdir ${LIBEXECDIR}),mxq)
    override LIBEXECDIR := $(patsubst %/,%,$(dir ${LIBEXECDIR}))
endif

CGIDIR = ${LIBEXECDIR}/mxq/cgi

########################################################################

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

########################################################################

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

########################################################################

CFLAGS_MYSQL := $(shell $(MYSQL_CONFIG) --cflags)
LDLIBS_MYSQL := $(shell $(MYSQL_CONFIG) --libs)

CFLAGS_MYSQL += ${CFLAGS_MXQ_MYSQL_DEFAULT_FILE}
CFLAGS_MYSQL += ${CFLAGS_MXQ_MYSQL_DEFAULT_GROUP}
CFLAGS_MYSQL += -DMX_MYSQL_FAIL_WAIT_DEFAULT=5

CFLAGS += -g
CFLAGS += -Wall
CFLAGS += -DMXQ_VERSION=\"${MXQ_VERSION}\"
CFLAGS += -DMXQ_VERSIONFULL=\"${MXQ_VERSIONFULL}\"
CFLAGS += -DMXQ_VERSIONDATE=\"${MXQ_VERSIONDATE}\"
CFLAGS += -DMXQ_VERSIONEXTRA=\"${MXQ_VERSIONEXTRA}\"
CFLAGS += $(EXTRA_CFLAGS)

########################################################################

quiet-command = $(if ${V},${1},$(if ${2},@echo ${2} && ${1}, @${1}))
quiet-install = $(call quiet-command,install -m ${1} ${2} ${3},"INSTALL ${3} [mode=${1}]")
quiet-installdir = $(call quiet-command,install -m ${1} -d ${2},"  MKDIR ${2} [mode=${1}]")
quiet-installforuser = $(call quiet-command,install -m ${1} -o ${2} -g ${3} ${4} ${5},"INSTALL ${5} (user=${2} group=${3}) [mode=${1}]")

########################################################################

sed-rules = -e 's,@PREFIX@,${PREFIX},g' \
            -e 's,@EPREFIX@,${EPREFIX},g' \
            -e 's,@BINDIR@,${BINDIR},g' \
            -e 's,@SBINDIR@,${SBINDIR},g' \
            -e 's,@LIBDIR@,${LIBDIR},g' \
            -e 's,@SYSCONFDIR@,${SYSCONFDIR},g' \
            -e 's,@DEFCONFDIR@,${DEFCONFDIR},g' \
            -e 's,@LIBEXECDIR@,${LIBEXECDIR},g' \
            -e 's,@BEE_VERSION@,${BEE_VERSION},g' \
            -e 's,@DATADIR@,${DATADIR},g' \
            -e 's,@MXQ_VERSION@,${MXQ_VERSION},g' \
            -e 's,@MXQ_MYSQL_DEFAULT_FILE@,${MXQ_MYSQL_DEFAULT_FILE},g' \
            -e 's,@CGIDIR@,${CGIDIR},g' \
            -e 's,@HTTP_USER@,${HTTP_USER},g' \
            -e 's,@HTTP_GROUP@,${HTTP_GROUP},g' \
            -e 's,@HTTP_PORT@,${HTTP_PORT},g' \


########################################################################

%.o: %.c Makefile
	$(call quiet-command,${CC} ${CFLAGS} -o $@ -c $<,"     CC $@")

%: %.c

%: %.o
	$(call quiet-command,${CC} -o $@ $^ $(LDFLAGS) $(LDLIBS), "   LINK $@")

%: %.in Makefile
	$(call quiet-command,sed ${sed-rules} $< >$@, "    GEN $@")

########################################################################

.SECONDARY:

MAN1DIR := ${MANDIR}/man1

fix: FIX += manpages/*.xml

manpages/%: manpages/%.xml
	$(call quiet-command,xmlto --stringparam man.output.quietly=1 man $^ -o manpages, "  XMLTO $@")

%: manpages/% Makefile
	$(call quiet-command,sed ${sed-rules} $< >$@, "    GEN $@")

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

.PHONY: mrproper
mrproper: clean

.PHONY: clean
mrproper clean:
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

fix: FIX += *.c
fix: FIX += *.h
fix: FIX += Makefile
fix: FIX += mysql/*.sql

fix:
	@for i in $(FIX) ; do \
	    if grep -q -m 1 -E '\s+$$' $$i ; then \
	        echo "FIX   $$i" ; \
	        sed -i $$i -e 's/\s*$$//g' ; \
	    fi \
	done

########################################################################

.PHONY: install

install:: build

install::
	$(call quiet-installdir,0755,${DESTDIR}${BINDIR})
	$(call quiet-installdir,0755,${DESTDIR}${SBINDIR})
	$(call quiet-installdir,0755,${DESTDIR}${SYSCONFDIR}/mxq)
	$(call quiet-installdir,0755,${DESTDIR}${MAN1DIR})
	$(call quiet-installdir,0755,${DESTDIR}${CGIDIR})

########################################################################

### mx_log.h -----------------------------------------------------------

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

### mxq_group.h --------------------------------------------------------

mxq_group.h += mxq_group.h

### mxq_job.h ----------------------------------------------------------

mxq_job.h += mxq_job.h
mxq_job.h += mxq_group.h

### mxqd.h -------------------------------------------------------------

mxqd.h += mxqd.h

### mx_getopt.h --------------------------------------------------------

mx_getopt.h += mx_getopt.h

########################################################################

### mx_getopt.o --------------------------------------------------------

mx_getopt.o: $(mx_getopt.h)

clean: CLEAN += mx_getopt.o

### mx_log.o -----------------------------------------------------------

mx_log.o: $(mx_log.h)

clean: CLEAN += mx_log.o

### mx_util.o ----------------------------------------------------------

mx_util.o: $(mx_log.h)

clean: CLEAN += mx_util.o

### mx_flock.o ---------------------------------------------------------

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

### mxqdump.o ----------------------------------------------------------

mxqdump.o: $(mx_log.h)
mxqdump.o: $(mx_util.h)
mxqdump.o: $(mx_mysql.h)
mxqdump.o: $(mx_getopt.h)
mxqdump.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxqdump.o

### mxqkill.o ----------------------------------------------------------

mxqkill.o: $(mx_log.h)
mxqkill.o: $(mx_util.h)
mxqkill.o: $(mx_mysql.h)
mxqkill.o: $(mx_getopt.h)
mxqkill.o: $(mxq.h)
mxqkill.o: $(mxq_group.h)
mxqkill.o: $(mxq_job.h)
mxqkill.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxqkill.o

### mxq_group.o --------------------------------------------------------

mxq_group.o: $(mx_log.h)
mxq_group.o: $(mxq_group.h)
mxq_group.o: $(mx_mysql.h)
mxq_group.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxq_group.o

### mxq_job.o ----------------------------------------------------------

mxq_job.o: $(mx_util.h)
mxq_job.o: $(mx_log.h)
mxq_job.o: $(mxq_job.h)
mxq_job.o: $(mxq_group.h)
mxq_job.o: $(mx_mysql.h)
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
mxqd.o: $(mx_mysql.h)
mxqd.o: CFLAGS += $(CFLAGS_MYSQL)
mxqd.o: CFLAGS += $(CFLAGS_MXQ_INITIAL_PATH)
mxqd.o: CFLAGS += -Wno-unused-but-set-variable

clean: CLEAN += mxqd.o

### mxqsub.o -----------------------------------------------------------

mxqsub.o: $(mx_getopt.h)
mxqsub.o: $(mx_util.h)
mxqsub.o: $(mx_log.h)
mxqsub.o: $(mx_mysql.h)
mxqsub.o: $(mxq.h)
mxqsub.o: $(mxq_group.h)
mxqsub.o: $(mxq_job.h)
mxqsub.o: $(mx_util.h)
mxqsub.o: CFLAGS += $(CFLAGS_MYSQL)

clean: CLEAN += mxqsub.o

########################################################################

### mxqd ---------------------------------------------------------------

mxqd: mx_flock.o
mxqd: mx_util.o
mxqd: mx_log.o
mxqd: mxq_log.o
mxqd: mx_getopt.o
mxqd: mxq_group.o
mxqd: mxq_job.o
mxqd: mx_mysql.o
mxqd: LDLIBS += $(LDLIBS_MYSQL)

build: mxqd

clean: CLEAN += mxqd

install:: mxqd
	$(call quiet-installforuser,0755,$(UID_SERVER),$(GID_SERVER),mxqd,${DESTDIR}${SBINDIR}/mxqd)

### mxqsub -------------------------------------------------------------

mxqsub: mx_getopt.o
mxqsub: mx_util.o
mxqsub: mx_log.o
mxqsub: mx_mysql.o
mxqsub: LDLIBS += $(LDLIBS_MYSQL)

build: mxqsub

clean: CLEAN += mxqsub

install:: mxqsub
	$(call quiet-installforuser,$(SUID_MODE),$(UID_CLIENT),$(GID_CLIENT),mxqsub,${DESTDIR}${BINDIR}/mxqsub)

### mxqdump ------------------------------------------------------------

mxqdump: mx_log.o
mxqdump: mx_mysql.o
mxqdump: mxq_group.o
mxqdump: mxq_job.o
mxqdump: mx_util.o
mxqdump: mx_getopt.o
mxqdump: LDLIBS += $(LDLIBS_MYSQL)
mxqdump: CFLAGS += -Wunused-function

build: mxqdump

clean: CLEAN += mxqdump

install:: mxqdump
	$(call quiet-installforuser,$(SUID_MODE),$(UID_CLIENT),$(GID_CLIENT),mxqdump,${DESTDIR}${BINDIR}/mxqdump)

### mxqkill ------------------------------------------------------------

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

fix: FIX += mxqdctl-hostconfig.sh

install:: mxqdctl-hostconfig.sh
	$(call quiet-install,0755,mxqdctl-hostconfig.sh,${DESTDIR}${SBINDIR}/mxqdctl-hostconfig)

########################################################################

build: mxqsub.1

mrproper: CLEAN += manpages/mxqsub.1
clean: CLEAN += mxqsub.1
#install:: mxqsub.1
#	$(call quiet-install,0644,mxqsub.1,${DESTDIR}${MAN1DIR}/mxqsub.1)

########################################################################

build: web/pages/mxq/mxq
build: web/lighttpd.conf

fix: FIX += web/pages/mxq/mxq.in
fix: FIX += web/lighttpd.conf.in

clean: CLEAN += web/pages/mxq/mxq
clean: CLEAN += web/lighttpd.conf

install:: web/pages/mxq/mxq
	$(call quiet-install,0755,$^,${DESTDIR}${CGIDIR}/mxq)

install:: web/lighttpd.conf
	$(call quiet-install,0644,$^,${DESTDIR}${LIBEXECDIR}/mxq/lighttpd.conf)

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
