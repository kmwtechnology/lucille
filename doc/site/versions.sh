# Clean up version folders and hugo.toml from any previous run
rm -rf content/en/docs-*
git checkout HEAD -- hugo.toml

# Get the latest tag so we can skip it, its docs are already the main /docs/.
latest_tag=$(git tag --list --sort=-version:refname | grep -v '^v' | head -1)
echo "Latest tag: $latest_tag"

# Use "git archive" to extract docs from each tag into a temp directory, then
# copy them into place. This avoids "git checkout" which would overwrite the
# current docs folder and leave behind extra files from older tags.
while IFS= read -r tag; do
  echo "Processing tag: $tag"

  # Skip the latest tag, its docs are already served at the main /docs/ path
  if [ "$tag" = "$latest_tag" ]; then echo "Skipping $tag - latest version"; continue; fi

  # Check if docs exist in this tag
  git ls-tree "$tag" content/en/docs >/dev/null 2>&1 || { echo "Skipping $tag - docs not found"; continue; }

  # Use git archive to extract just the docs folder from the tag into a temp dir
  tmpdir=$(mktemp -d)
  git archive "$tag" content/en/docs | tar -x -C "$tmpdir"
  cp -r "$tmpdir/content/en/docs" "content/en/docs-$tag"
  rm -rf "$tmpdir"

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
done <<< "$(git tag --list)"

# Write versions block directly to a temp file instead of building it in a
# variable. Bash's $(...) command substitution strips trailing newlines, which
# is why the previous approach jammed all the [[params.versions]] entries
# together on one line.
#
# Tags are listed in reverse version order (newest first) so the dropdown
# reads top-to-bottom from newest to oldest.
{
  # Add the latest version first, it points to the main /docs/ path
  printf '[[params.versions]]\n'
  printf '  version = "Lucille Latest"\n'
  printf '  url = "http://localhost:1313/docs/"\n'
  printf '\n'

  while IFS= read -r tag; do
    # Skip the latest tag — already added above
    if [ "$tag" = "$latest_tag" ]; then continue; fi
    printf '[[params.versions]]\n'
    printf '  version = "%s"\n' "$tag"
    printf '  url = "http://localhost:1313/docs-%s/"\n' "$tag"
    printf '\n'
  done <<< "$(git tag --list --sort=-version:refname)"
} > /tmp/versions_block.txt

awk '/# versions injected at build time/{while((getline line < "/tmp/versions_block.txt") > 0) print line; next} {print}' hugo.toml > hugo.toml.tmp && mv hugo.toml.tmp hugo.toml

echo "Tags found:"
git tag
echo "Content folder:"
ls content/en/
echo "hugo.toml versions section:"
cat hugo.toml
