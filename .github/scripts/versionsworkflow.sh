# This script prepares a versioned doc site in doc/site-preview, illustrating what the site will look like when deployed.
#
# What it does:
#   * Snapshots the current working tree's content/en/docs into content/en/docs-pre-release
#     for the "pre-release" dropdown entry (kept up to date with main).
#   * Uses git to pull content/en/docs from each tagged release at or above $min_version:
#     the newest tag replaces content/en/docs (the site's "latest" entry), each earlier
#     tag goes into its own content/en/docs-<tag> folder.
#   * Rewrites hugo.toml's [[params.versions]] block to match.
#
# Two modes:
#   * "production" (used by .github/workflows/hugo.yml): operates directly on
#     ${GITHUB_WORKSPACE}/doc/site in the CI checkout.
#   * "local" (default): mirrors doc/site -> doc/site-preview/ and does everything in the
#     mirror. doc/site itself is never modified, so you can edit it freely and commit
#     normally without the generated versioned content polluting your diff.
#     To preview: re-run this script, then `cd doc/site-preview && hugo server`.

mode=${1:-"local"}
if [ $mode = "local" ]; then
  # Build a doc/site-preview/ mirror as a sibling to doc/site and run everything there.
  # rsync excludes node_modules and public/ from the copy: node_modules is then
  # symlinked back (hugo only reads from it), and public/ would just be regenerated
  # by hugo anyway.

  # cd into directory where this script is and store the pwd in script_dir
  script_dir="$(cd "$(dirname "$0")" && pwd)"
  # cd into doc site and save pwd
  src_site="$(cd "$script_dir/../../doc/site" && pwd)"
  # mark location for site-preview
  preview_site="$(dirname "$src_site")/site-preview"
  echo "Mirroring $src_site -> $preview_site"
  # remove anything there previously
  rm -rf "$preview_site"
  # make new site-preview
  mkdir -p "$preview_site"
  # exclude node_modules and public folder from copy based on src (hugo doesn't write public to disk)
  rsync -a --exclude='/node_modules' --exclude='/public' "$src_site/" "$preview_site/"
  if [ -d "$src_site/node_modules" ]; then
    ln -s "$src_site/node_modules" "$preview_site/node_modules"
  fi
  cd "$preview_site"
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

# Filtered list of tags at or above min_version. sort -V puts tags in version
# order, then awk emits everything from min_version onward.
tags=$(git tag --list | sort -V | awk -v min="$min_version" '$0 == min { found=1 } found')

# Descending (newest-first) list used for the dropdown selector order
tags_descending=$(echo "$tags" | sort -rV)

# Get the latest tag; its docs will replace the working directory's docs/
latest_tag=$(echo "$tags_descending" | head -1)
echo "Latest tag: $latest_tag"

# Grab the doc site from the working branch for pre-release (up to date with main)
# Needs to be done before the tag loop because it wipes content/en/docs
pre_release_url="${url}-pre-release/"
echo "Creating pre-release docs snapshot from working tree"
rm -rf content/en/docs-pre-release
cp -r content/en/docs content/en/docs-pre-release
cat > content/en/docs-pre-release/_index.md << EOF
---
title: Documentation pre-release
linkTitle: Docs-pre-release
weight: 20
cascade:
  type: docs
---
EOF
# Append everything after the metadata from the working branches top level docs _index.md so the pre-release landing
# page keeps the same overview content
awk 'found{print} /^---$/{n++; if(n==2) found=1}' content/en/docs/_index.md >> content/en/docs-pre-release/_index.md

# Use "git archive" to extract docs from each tag into a temp directory, then
# copy them into place.

# IFS= means preserve any whitespace, read -r says to read the tag exactly as is (ignoring backslashes)
while IFS= read -r tag; do
  echo "Processing tag: $tag"

  # Check if docs exist in this tag. Use -C "$src_site" so git resolves
  # pathspecs from the real doc/site location; in local mode CWD is the
  # preview mirror, which isn't part of the tracked tree.
  git -C "$src_site" ls-tree "$tag" content/en/docs >/dev/null 2>&1 || { echo "Skipping $tag - docs not found"; continue; }

  # Make temporary directory for git archive to use, then archive the tag into it
  tmpdir=$(mktemp -d)
  git -C "$src_site" archive "$tag" content/en/docs | tar -x -C "$tmpdir"

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
  printf 'url_latest_version = "%s/"\n' "$url"
  printf '\n'

  # Pre-release entry at the top of the dropdown
  printf '[[params.versions]]\n'
  printf '  version = "pre-release"\n'
  printf '  url = "%s"\n' "$pre_release_url"
  printf '\n'

  # Add the latest release version next, it points to the main /docs/ path
  printf '[[params.versions]]\n'
  printf '  version = "%s (latest)"\n' "$latest_tag"
  printf '  url = "%s/"\n' "$url"
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
  /^# VERSION_GENERATION_SCRIPT_PLACEHOLDER/{while((getline line < "/tmp/versions_block.txt") > 0) print line; next}
  /^version = /{next}
  /^url_latest_version = /{next}
  {print}
' hugo.toml > hugo.toml.tmp && mv hugo.toml.tmp hugo.toml