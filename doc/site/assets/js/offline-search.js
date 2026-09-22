// Adapted code from https://github.com/google/docsy/blob/v0.14.0/assets/js/offline-search.js
// Main changes from original code:
// - buildSnippet() centers the result snippet on the actual match and highlights it, instead of a static excerpt
// - only highlights matches that are exact or contain the raw query word or its stem (excludes fuzzy noise, see queryStems)
// - appends "?q=" to result links so hooks/body-end.html can highlight + scroll to the match on the destination page

(function ($) {
  'use strict';

  $(document).ready(function () {
    const $searchInput = $('.td-search input');

    //
    // Register handler
    //

    $searchInput.on('change', (event) => {
      render($(event.target));

      // Hide keyboard on mobile browser
      $searchInput.blur();
    });

    // Prevent reloading page by enter key on sidebar search.
    $searchInput.closest('form').on('submit', () => {
      return false;
    });

    //
    // Lunr
    //

    let idx = null; // Lunr index
    const resultDetails = new Map(); // Will hold the data for the search results (titles and summaries)

    // Set up for an Ajax call to request the JSON data file that is created by Hugo's build process
    $.ajax($searchInput.data('offline-search-index-json-src')).then((data) => {
      idx = lunr(function () {
        this.ref('ref');
        this.metadataWhitelist = ['position']; // Match position needed for snippet generation

        // Searchable fields, matching the dict built in assets/json/offline-search-index.json
        this.field('title', { boost: 20 });
        this.field('description', { boost: 4 });
        this.field('identifiers', { boost: 3 });
        this.field('section', { boost: 1 });
        this.field('body', { boost: 1 });
        this.field('code', { boost: 0.25 });

        data.forEach((doc) => {
          this.add(doc);

          resultDetails.set(doc.ref, {
            title: doc.title,
            excerpt: doc.excerpt,
            body: doc.body,
            code: doc.code,
          });
        });
      });

      $searchInput.trigger('change');
    });

    // Edit distance for the fuzzy clause, tiered by term length
    const fuzzyDistance = (len) => (len < 4 ? 0 : len < 7 ? 1 : 2);

    // Fields a snippet can come from, in priority order
    // identifiers are excluded because they hold space-split camelCase names that do not appear on screen
    const SNIPPET_FIELDS = ['body', 'code'];

    // Where a snippet may start and end, per field. Code has no sentences or line breaks so it is cut at whitespace
    const SNIPPET_BOUNDARY = { body: /[.!?;]\s|\n/, code: /\s/ };

    // Build a snippet around the first relevant match of a query term, trying each
    // snippet field in order and falling back to the static excerpt
    // A relevant match is an exact match of a query stem, or contains the raw query word/stem
    // Fuzzy-only matches are discarded and snippets are cut at sentence/line boundaries 80 characters before and after the match
    // If no boundary is found, snippet starts from the first match
    // Every relevant occurrence within the snippet window is highlighted
    function buildSnippet(doc, r, queryStems) {
      // Query text to look for inside a matched span, best highlight first
      // Prefer the raw word, fall back to the stem. The index is stemmed and the wildcard
      // clauses match on it, so a span like "SolrIndexerTest" holds "index" but not "indexers"
      function matchTexts(term) {
        for (const [stem, raw] of queryStems) {
          if (term === stem || term.includes(raw) || term.includes(stem)) {
            return raw === stem ? [raw] : [raw, stem];
          }
        }
        return null;
      }

      function hitsFor(text, term, position) {
        const candidates = matchTexts(term);
        if (!candidates) return [];
        const hits = [];
        position.forEach(([s, len]) => {
          const span = text.slice(s, s + len).toLowerCase();
          for (const candidate of candidates) {
            const offset = span.indexOf(candidate);
            if (offset !== -1) {
              hits.push([s + offset, candidate.length]);
              break;
            }
          }
        });
        return hits;
      }

      function snippetForField(field) {
        const text = doc[field];
        if (!text) return null;

        for (const term of Object.keys(r.matchData.metadata)) {
          const match = r.matchData.metadata[term][field];
          if (!match || !match.position || !match.position.length) continue;
          const termHits = hitsFor(text, term, match.position);
          if (!termHits.length) continue;
          const [start] = termHits[0];

          const boundary = SNIPPET_BOUNDARY[field];
          const windowStart = Math.max(0, start - 80);
          const windowEnd = start + 80;
          const startMatch = [...text.slice(windowStart, start).matchAll(new RegExp(boundary, 'g'))].at(-1);
          const endMatch = boundary.exec(text.slice(start, windowEnd));
          // Start past the boundary; end keeps its punctuation but drops the trailing space.
          const snippetStart = startMatch ? windowStart + startMatch.index + startMatch[0].length : start;
          const snippetEnd = endMatch ? start + endMatch.index + endMatch[0].length - 1 : windowEnd;

          const hits = [];
          for (const t of Object.keys(r.matchData.metadata)) {
            const m = r.matchData.metadata[t][field];
            if (m && m.position) {
              hitsFor(text, t, m.position).forEach(([s, len]) => {
                if (s + len > snippetStart && s < snippetEnd) hits.push([s, len]);
              });
            }
          }
          hits.sort((a, b) => a[0] - b[0]);

          // Constructed per attempt, so a field that yields no hits leaves nothing behind.
          const $p = $('<p>');
          if (field === 'code') $p.addClass('td-offline-search-results__code');
          let cursor = snippetStart;
          hits.forEach(([s, len]) => {
            const hitStart = Math.max(s, cursor);
            const hitEnd = Math.min(s + len, snippetEnd);
            $p.append(document.createTextNode(text.slice(cursor, hitStart)));
            $p.append($('<mark>').text(text.slice(hitStart, hitEnd)));
            cursor = hitEnd;
          });
          $p.append(document.createTextNode(text.slice(cursor, snippetEnd)));
          return $p;
        }
        return null;
      }

      for (const field of SNIPPET_FIELDS) {
        const $snippet = snippetForField(field);
        if ($snippet) return $snippet;
      }
      return $('<p>').text(doc.excerpt);
    }

    const render = ($targetSearchInput) => {
      //
      // Dispose existing popover
      //

      {
        let popover = bootstrap.Popover.getInstance($targetSearchInput[0]);
        if (popover !== null) {
          popover.dispose();
        }
      }

      //
      // Search
      //

      if (idx === null) {
        return;
      }

      const searchQuery = $targetSearchInput.val();
      if (searchQuery === '') {
        return;
      }

      const queryStems = new Map();
      const results = idx
        .query((q) => {
          const tokens = lunr.tokenizer(searchQuery.toLowerCase());
          tokens.forEach((token) => {
            const queryString = token.toString();
            const stem = lunr.stemmer(new lunr.Token(queryString)).toString();
            queryStems.set(stem, queryString);
            q.term(queryString, {
              boost: 100,
            });
            // Prefix and substring are weighted separately since a prefix match tends to
            // be closer to what was typed ("connect" -> "connector"), while a substring
            // can land mid-word ("run" -> "truncate")
            // Both take the stem since lunr skips the pipeline on any clause with a wildcard
            q.term(stem, {
              wildcard: lunr.Query.wildcard.TRAILING,
              boost: 20,
              usePipeline: false,
            });
            q.term(stem, {
              wildcard:
                lunr.Query.wildcard.LEADING | lunr.Query.wildcard.TRAILING,
              boost: 5,
              usePipeline: false,
            });
            const editDistance = fuzzyDistance(queryString.length);
            if (editDistance > 0) {
              q.term(queryString, { editDistance });
            }
          });
        })
        .slice(0, $targetSearchInput.data('offline-search-max-results'));

      //
      // Make result html
      //

      const $html = $('<div>');

      $html.append(
        $('<div>')
          .css({
            display: 'flex',
            justifyContent: 'space-between',
            marginBottom: '1em',
          })
          .append(
            $('<span>').text('Search results').css({ fontWeight: 'bold' })
          )
          .append(
            $('<span>').addClass('td-offline-search-results__close-button')
          )
      );

      const $searchResultBody = $('<div>').css({
        maxHeight: `calc(100vh - ${
          $targetSearchInput.offset().top - $(window).scrollTop() + 180
        }px)`,
        overflowY: 'auto',
      });
      $html.append($searchResultBody);

      if (results.length === 0) {
        $searchResultBody.append(
          $('<p>').text(`No results found for query "${searchQuery}"`)
        );
      } else {
        results.forEach((r) => {
          const doc = resultDetails.get(r.ref);
          const href =
            $searchInput.data('offline-search-base-href') +
            r.ref.replace(/^\//, '') +
            '?q=' + encodeURIComponent(searchQuery); // read by hooks/body-end.html to highlight the match on the destination page

          const $entry = $('<div>').addClass('mt-4');

          $entry.append(
            $('<small>').addClass('d-block text-body-secondary').text(r.ref)
          );

          $entry.append(
            $('<a>')
              .addClass('d-block')
              .css({
                fontSize: '1.2rem',
              })
              .attr('href', href)
              .text(doc.title)
          );

          $entry.append(buildSnippet(doc, r, queryStems));

          $searchResultBody.append($entry);
        });
      }

      $targetSearchInput.one('shown.bs.popover', () => {
        $('.td-offline-search-results__close-button').on('click', () => {
          $targetSearchInput.val('');
          $targetSearchInput.trigger('change');
        });
      });

      const popover = new bootstrap.Popover($targetSearchInput, {
        content: $html[0],
        html: true,
        customClass: 'td-offline-search-results',
        placement: 'bottom',
      });
      popover.show();
    };
  });
})(jQuery);