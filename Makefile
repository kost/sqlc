VERSION=0.1.1
GIT_COMMIT = `git rev-parse HEAD | cut -c1-7`
BUILD_OPTIONS = -ldflags "-X main.Version=$(VERSION) -X main.CommitID=$(GIT_COMMIT)"
STATIC_OPTIONS = -ldflags "-extldflags='-static' -X main.Version=$(VERSION) -X main.CommitID=$(GIT_COMMIT)"

all: sqlc

sqlc: dep
	go build ${BUILD_OPTIONS}

dep:
	go get

update:
	go get -u ./...

tools:
	go install github.com/mitchellh/gox@latest
	go install github.com/tcnksm/ghr@latest

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
	CGO_ENABLED=0 gox -osarch="!darwin/386" -ldflags="-s -w -X main.Version=$(VERSION) -X main.CommitID=$(GIT_COMMIT)" -output="dist/{{.Dir}}_{{.OS}}_{{.Arch}}"

draft:
	ghr -draft v$(VERSION) dist/



