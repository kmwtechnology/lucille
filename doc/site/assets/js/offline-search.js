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

    // Build a snippet around the first match of a term in the document body, highlighting the matched term.
    // Snippet is cut off at sentence boundaries 80 characters before and after the match.
    // If no boundary is found, snippet starts from the first match
    function buildSnippet(doc, r) {
      const $p = $('<p>');
      for (const term of Object.keys(r.matchData.metadata)) {
        const match = r.matchData.metadata[term].body;
        if (match && match.position && match.position.length) {
          const [start, length] = match.position[0];
          const from = Math.max(0, start - 80);
          const to = start + 80;
          const pre = doc.body.slice(from, start).lastIndexOf('. ');
          const linePre = doc.body.slice(from, start).lastIndexOf('\n');
          const post = /[.!?]\s|\n/.exec(doc.body.slice(start, to));
          const snippetStart = pre === -1 && linePre === -1 ? start : Math.max(pre === -1 ? from : from + pre + 2, linePre === -1 ? from : from + linePre + 1);
          const snippetEnd = post ? start + post.index + (post[0] === '\n' ? 0 : 1) : to;
          $p.append(document.createTextNode(doc.body.slice(snippetStart, start)));
          $p.append($('<mark>').text(doc.body.slice(start, start + length)));
          $p.append(document.createTextNode(doc.body.slice(start + length, snippetEnd)));
          return $p;
        }
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

      const results = idx
        .query((q) => {
          const tokens = lunr.tokenizer(searchQuery.toLowerCase());
          tokens.forEach((token) => {
            const queryString = token.toString();
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

          $entry.append(buildSnippet(doc, r));

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