#!/bin/sh
# tagpr proposes a patch release when the release pull request has no
# label. Propose the next minor instead; editing the pull request's title
# still picks any other version.
set -eu

IFS=. read -r major minor _ <<END
${TAGPR_CURRENT_VERSION#v}
END
echo "$major.$((minor + 1)).0" > VERSION
