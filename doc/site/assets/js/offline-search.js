// Adapted from code by Matt Walters https://www.mattwalters.net/posts/2018-03-28-hugo-and-lunr/

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
        this.metadataWhitelist = ['position'];

        // If you added more searchable fields to the search index, list them here.
        // Here you can specify searchable fields to the search index - e.g. individual toxonomies for you project
        // With "boost" you can add weighting for specific (default weighting without boost: 1)
        this.field('title', { boost: 5 });
        this.field('categories', { boost: 3 });
        this.field('tags', { boost: 3 });
        // this.field('projects', { boost: 3 }); // example for an individual toxonomy called projects
        this.field('description', { boost: 2 });
        this.field('body');

        data.forEach((doc) => {
          this.add(doc);

          resultDetails.set(doc.ref, {
            title: doc.title,
            excerpt: doc.excerpt,
            body: doc.body,
          });
        });
      });

      $searchInput.trigger('change');
    });

    // Build a snippet around the first relevant match of a query term in the document body.
    // A relevant match is either an exact match of a query stem or contains the raw query word.
    // Snippet is cut off at sentence/line boundaries 80 characters before and after the match.
    // If no boundary is found, snippet starts from the first match.
    // Every relevant occurrence within the snippet window is highlighted.
    function buildSnippet(doc, r, queryStems) {
      function findRaw(term) {
        for (const [stem, raw] of queryStems) {
          if (term === stem || term.includes(raw)) return raw;
        }
        return null;
      }

      function hitsFor(term, position) {
        const raw = findRaw(term);
        if (!raw) return [];
        const hits = [];
        position.forEach(([s, len]) => {
          const offset = doc.body.slice(s, s + len).toLowerCase().indexOf(raw);
          if (offset !== -1) hits.push([s + offset, raw.length]);
        });
        return hits;
      }

      const $p = $('<p>');
      for (const term of Object.keys(r.matchData.metadata)) {
        const match = r.matchData.metadata[term].body;
        if (!match || !match.position || !match.position.length) continue;
        const termHits = hitsFor(term, match.position);
        if (!termHits.length) continue;
        const [start] = termHits[0];

        const windowStart = Math.max(0, start - 80);
        const windowEnd = start + 80;
        const startMatch = [...doc.body.slice(windowStart, start).matchAll(/[.!?;]\s|\n/g)].at(-1);
        const endMatch = /[.!?;]\s|\n/.exec(doc.body.slice(start, windowEnd));
        const snippetStart = startMatch ? windowStart + startMatch.index + (startMatch[0] === '\n' ? 1 : 2) : start;
        const snippetEnd = endMatch ? start + endMatch.index + (endMatch[0] === '\n' ? 0 : 1) : windowEnd;

        const hits = [];
        for (const t of Object.keys(r.matchData.metadata)) {
          const m = r.matchData.metadata[t].body;
          if (m && m.position) {
            hitsFor(t, m.position).forEach(([s, len]) => {
              if (s + len > snippetStart && s < snippetEnd) hits.push([s, len]);
            });
          }
        }
        hits.sort((a, b) => a[0] - b[0]);

        let cursor = snippetStart;
        hits.forEach(([s, len]) => {
          const hitStart = Math.max(s, cursor);
          const hitEnd = Math.min(s + len, snippetEnd);
          $p.append(document.createTextNode(doc.body.slice(cursor, hitStart)));
          $p.append($('<mark>').text(doc.body.slice(hitStart, hitEnd)));
          cursor = hitEnd;
        });
        $p.append(document.createTextNode(doc.body.slice(cursor, snippetEnd)));
        return $p;
      }
      return $p.text(doc.excerpt);
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
            queryStems.set(lunr.stemmer(new lunr.Token(queryString)).toString(), queryString);
            q.term(queryString, {
              boost: 100,
            });
            q.term(queryString, {
              wildcard:
                lunr.Query.wildcard.LEADING | lunr.Query.wildcard.TRAILING,
              boost: 10,
            });
            q.term(queryString, {
              editDistance: 2,
            });
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
            '?q=' + encodeURIComponent(searchQuery);

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