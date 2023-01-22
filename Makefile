test:
	go clean -testcache && go test -v ./...

inspect:
	od -A d -b db
