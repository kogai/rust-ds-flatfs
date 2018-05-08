.PHONY: install
install:
	go get github.com/ipfs/go-ds-flatfs
	go get github.com/rogpeppe/godef
	go get golang.org/x/tools/cmd/godoc
	go get -v github.com/uudashr/gopkgs/cmd/gopkgs
	go get -v github.com/sqs/goreturns
	go get -v github.com/ramya-rao-a/go-outline
	go get -v github.com/nsf/gocode
	go get -v golang.org/x/tools/cmd/guru
