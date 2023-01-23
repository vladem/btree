test:
	go clean -testcache && go test -v ./...

inspect:
	hexdump -e '"%_ad:\t"' -e '16/1 "%03u "' -e '"\n"' db

