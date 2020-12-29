GO_LIB_FILES=context.go error.go const.go time.go log.go exec.go threads.go fixture.go github.go gha.go json.go
GO_BIN_FILES=cmd/dadsgha/dadsgha.go
GO_TEST_FILES=context_test.go threads_test.go
GO_LIBTEST_FILES=test/time.go
GO_BIN_CMDS=github.com/LF-Engineering/da-ds-gha/sources/cmd/dadsgha
#for race CGO_ENABLED=1
#GO_ENV=CGO_ENABLED=1
GO_ENV=CGO_ENABLED=0
#GO_BUILD=go build -ldflags '-s -w' -race
GO_BUILD=go build -ldflags '-s -w'
GO_INSTALL=go install -ldflags '-s'
GO_FMT=gofmt -s -w
GO_LINT=golint -set_exit_status
GO_VET=go vet
GO_IMPORTS=goimports -w
GO_USEDEXPORTS=usedexports
GO_ERRCHECK=errcheck -asserts -ignore '[FS]?[Pp]rint*' -ignoretests
GO_TEST=go test
BINARIES=dadsgha
STRIP=strip

all: check ${BINARIES}

dadsgha: cmd/dadsgha/dadsgha.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o dadsgha cmd/dadsgha/dadsgha.go

fmt: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	./for_each_go_file.sh "${GO_FMT}"

lint: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	./for_each_go_file.sh "${GO_LINT}"

vet: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	./vet_files.sh "${GO_VET}"

imports: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	./for_each_go_file.sh "${GO_IMPORTS}"

usedexports: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	${GO_USEDEXPORTS} ./...

errcheck: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	${GO_ERRCHECK} ./...

test:
	${GO_TEST} ${GO_TEST_FILES}

check: fmt lint imports vet usedexports errcheck

install: check ${BINARIES}
	${GO_ENV} ${GO_INSTALL} ${GO_BIN_CMDS}

strip: ${BINARIES}
	${STRIP} ${BINARIES}

clean:
	rm -f ${BINARIES}

.PHONY: test
