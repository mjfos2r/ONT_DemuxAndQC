IMAGE_NAME = BbCallGenotypes
VERSION := $(shell cat ._VERSION)

all: | tag 

check:
	find . -name '.venv' -prune -o -name '.git' -prune -o -regex  '.*/*.wdl' -print0 | xargs -0 miniwdl check
	find . -name '.venv' -prune -o -name '.git' -prune -o -regex  '.*\.\(ya?ml\)' -print0 | xargs -0 yamllint -d relaxed 

tag:
	git tag -s v$(VERSION) -m "Release version $(VERSION)"
	git push origin tag v$(VERSION)
