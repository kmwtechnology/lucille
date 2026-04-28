# The following script is used in hugo.yml for the github workflow which builds and deploys our documentation site.

# The script will run in production mode if a production flag is added to it, like this: versionsworkflow.sh production
# It runs in local mode by default, which you can see in the first few lines of the script. Further information can be found
# on local mode in the following comments:

# Local mode which will generate the user-facing versioned documentation site locally. When you build
# the site locally without running this script, you will only get the most updated docs from the
# working directory. This script allows you to see what the doc site will look like in production.

# Keep in mind that the latest version of the docs here will not reflect your changes to the doc site content
# folder in the working directory, because it pulls from the latest release. Your changes will not appear until
# the next release.

# Also keep in mind that this script will alter the hugo.toml file.
# MAKE SURE TO REVERT THE hugo.toml BEFORE COMMITTING IF YOU RUN THIS SCRIPT.

# Pre-run cleanup if running in local mode, not necessary for production
mode=${1:-"local"}
if [ $mode = "local" ]; then
  cd ../../doc/site
  rm -rf content/en/docs-*
  git checkout HEAD -- hugo.toml
  url="http://localhost:1313/docs"
elif [ $mode = "production" ]; then
  cd ${GITHUB_WORKSPACE}/doc/site
  url="https://kmwtechnology.github.io/lucille/docs"
else
  echo "Invalid mode specified: $mode"
  exit 1
fi

# Minimum version to expose on the docs site. Tags older than this are
# skipped entirely, they get no docs-<tag> folder and no dropdown entry.
min_version="0.5.10"

# Helper which returns success if $1 a tag is at or above $min_version. Strips leading v for v0.2.4
tag_at_least_min() {
  # local scopes the variable to this function only, 1#v removes a v if it's there
  local t="${1#v}"
  [ "$(printf '%s\n%s\n' "$min_version" "$t" | sort -V | head -n1)" = "$min_version" ]
}

# Filtered list of tags at or above min_version
tags=$(git tag --list | while IFS= read -r t; do
  tag_at_least_min "$t" && printf '%s\n' "$t"
done)

# Descending (newest-first) list used for the dropdown selector order
tags_descending=$(echo "$tags" | sort -rV)

# Get the latest tag; its docs will replace the working directory's docs/
latest_tag=$(echo "$tags_descending" | grep -v '^v' | head -1)
echo "Latest tag: $latest_tag"

# Use "git archive" to extract docs from each tag into a temp directory, then
# copy them into place.

# IFS= means preserve any whitespace, read -r says to read the tag exactly as is (ignoring backslashes)
while IFS= read -r tag; do
  echo "Processing tag: $tag"

  # Check if docs exist in this tag
  git ls-tree "$tag" content/en/docs >/dev/null 2>&1 || { echo "Skipping $tag - docs not found"; continue; }

  # Make temporary directory for git archive to use, then archive the tag into it
  tmpdir=$(mktemp -d)
  git archive "$tag" content/en/docs | tar -x -C "$tmpdir"

  if [ "$tag" = "$latest_tag" ]; then
    # Latest tag replaces the main docs/ folder
    rm -rf content/en/docs
    cp -r "$tmpdir/content/en/docs" content/en/docs
  else
    # Older tags go into their own docs-{tag} folder
    cp -r "$tmpdir/content/en/docs" "content/en/docs-$tag"

    # Update _index.md to specify title and link title
    cat > "content/en/docs-$tag/_index.md" << EOF
---
title: Documentation $tag
linkTitle: Docs-$tag
weight: 20
cascade:
  type: docs
  exclude_search: true
---
EOF
  fi
  rm -rf "$tmpdir"
done <<< "$tags"

# Write the version params and versions block to a temp file.
# Tags are listed in reverse version order (newest first) so the dropdown
# reads top-to-bottom from newest to oldest.
{
  printf 'version = "%s (latest)"\n' "$latest_tag"
  printf 'url_latest_version = "/docs/"\n'
  printf '\n'

  # Add the latest version first, it points to the main /docs/ path
  printf '[[params.versions]]\n'
  printf '  version = "%s (latest)"\n' "$latest_tag"
  printf '  url = "%s"\n' "$url"
  printf '\n'

  while IFS= read -r tag; do
    # Skip the latest tag — already added above
    if [ "$tag" = "$latest_tag" ]; then continue; fi
    printf "[[params.versions]]\n"
    printf '  version = "%s"\n' "$tag"
    printf '  url = "%s-%s/"\n' "$url" "$tag"
    printf '\n'
  done <<< "$tags_descending"
} > /tmp/versions_block.txt

# Insert the versions block immediately before [params.ui] in hugo.toml.
# This placement matters: [[params.versions]] is an array-of-tables, and any
# plain key/value pairs appearing after it are parsed as properties of the
# last version entry rather than of [params] itself, which silently breaks
# things like offlineSearch, algolia_docsearch, github_repo. Anchoring on
# [params.ui] puts the versions block after all top-level [params] keys and
# right before the next sub-table header.
# Also strip any pre-existing top-level `version =` / `url_latest_version =`
# lines so we don't end up with duplicate keys (indented ones inside
# [[params.versions]] blocks are left alone).

awk '
  /^# PLACEHOLDER/{while((getline line < "/tmp/versions_block.txt") > 0) print line; next}
  /^version = /{next}
  /^url_latest_version = /{next}
  {print}
' hugo.toml > hugo.toml.tmp && mv hugo.toml.tmp hugo.toml

# We can skip the cleanup if deploying to prod
if [ $mode = "local" ]; then
  hugo build
  rm -rf content/en/docs-*
  git checkout HEAD -- content/en/docs
fi