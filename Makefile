TARGET = bin/metl
GITCOMMIT = `git rev-parse --short HEAD`
GITTAG = unknown
#GITTAG = `git describe --tags --abbrev=0 | sed 's/^v//' | sed 's/\+.*$$//'`
GITBRANCH = `git rev-parse --abbrev-ref HEAD`

.PHONEY: get-deps clean all install run 

get-deps:
	source gvp
	gmp install

all: 
	#clean test build install

test:
	go vet
	go test -covermode=count ./...

clean:
	rm -rf bin/
	go clean

devrun:	build
	$(TARGET) $(c)

build: clean
	go build -a -ldflags "\
	 -X command.GitTag $(GITTAG)\
	 -X command.GitBranch $(GITBRANCH)\
	 -X command.GitCommit $(GITCOMMIT)"\
	 -o $(TARGET) .

install:
	mkdir /etc/metl
	mv $(TARGET) /usr/local/bin/metl

