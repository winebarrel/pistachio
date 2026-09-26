#!/bin/sh
# Rename the [Unreleased] section of CHANGELOG.md to the version tagpr is
# about to release. tagpr rebuilds the release pull request from main on
# every run, so the section is still [Unreleased] each time this runs.
set -eu

version=${TAGPR_NEXT_VERSION#v}
date=$(TZ=Asia/Tokyo date +%Y-%m-%d)

sed -i "s/^## \[Unreleased\]\$/## [$version] - $date/" CHANGELOG.md
