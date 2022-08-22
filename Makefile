VERSION=0.1.1

all: sqlc

sqlc: dep
	go build

dep:
	go get

tools:
	go install github.com/mitchellh/gox@latest
	go get -u github.com/tcnksm/ghr

ver:
	echo version $(VERSION)

gittag:
	git tag v$(VERSION)
	git push --tags origin master

clean:
	rm -rf dist

dist:
	mkdir -p dist

gox:
	CGO_ENABLED=0 gox -osarch="!darwin/386" -ldflags="-s -w" -output="dist/{{.Dir}}_{{.OS}}_{{.Arch}}"

draft:
	ghr -draft v$(VERSION) dist/



