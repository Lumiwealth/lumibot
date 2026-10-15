/**
 * Harden Sphinx/Furo docs navigation for search results and sidebar.
 *
 * Users reported search-result clicks updating only the URL hash while
 * staying on search.html, and sidebar toggles feeling dead.
 * Sphinx marks the current toctree page as href="#", which is a silent no-op
 * besides changing the hash. This script:
 * - Forces search-result activations to assign a fully resolved document URL
 * - Rewrites current-page href="#" links to the real document path
 * - Closes the mobile nav/toc overlays when a real sidebar link is followed
 * - Raises expander label z-index via companion CSS so toggles stay clickable
 */
(function () {
  "use strict";

  function contentRootBase() {
    var root = document.documentElement.getAttribute("data-content_root") || "./";
    try {
      return new URL(root, window.location.href);
    } catch (error) {
      return new URL("./", window.location.href);
    }
  }

  function resolveDocsUrl(href) {
    return new URL(href, contentRootBase()).href;
  }

  function rewriteSearchResultLinks(container) {
    var root = container || document.getElementById("search-results");
    if (!root) {
      return;
    }
    root.querySelectorAll("a[href]").forEach(function (link) {
      var raw = link.getAttribute("href");
      if (!raw || raw.charAt(0) === "#") {
        return;
      }
      var absolute = resolveDocsUrl(raw);
      if (link.getAttribute("href") !== absolute) {
        link.setAttribute("href", absolute);
      }
    });
  }

  function rewriteCurrentPageHashLinks() {
    var path = window.location.pathname || "/";
    var parts = path.split("/");
    var leaf = parts[parts.length - 1] || "";
    if (!leaf || leaf === "/") {
      leaf = "index.html";
    }
    document
      .querySelectorAll(
        "a.current.reference.internal[href='#'], a.reference.internal.current[href='#']"
      )
      .forEach(function (link) {
        link.setAttribute("href", leaf);
      });
  }

  function closeMobileOverlays() {
    var navigation = document.getElementById("__navigation");
    if (navigation && navigation.checked) {
      navigation.checked = false;
    }
    var toc = document.getElementById("__toc");
    if (toc && toc.checked) {
      toc.checked = false;
    }
  }

  document.addEventListener(
    "click",
    function (event) {
      var target = event.target;
      if (!target || typeof target.closest !== "function") {
        return;
      }

      var sidebarLink = target.closest(".sidebar-tree a[href]");
      if (sidebarLink) {
        var sidebarHref = sidebarLink.getAttribute("href") || "";
        if (sidebarHref && sidebarHref.charAt(0) !== "#") {
          closeMobileOverlays();
        }
      }

      var searchLink = target.closest("#search-results a[href]");
      if (!searchLink) {
        return;
      }
      var raw = searchLink.getAttribute("href");
      if (!raw || raw.charAt(0) === "#") {
        return;
      }
      // Leave ordinary primary clicks on a fully resolved URL so search.html
      // cannot stay stuck with only a hash change. Preserve modified clicks
      // (new tab / new window) so the rewritten href still opens normally.
      if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
        return;
      }
      event.preventDefault();
      window.location.assign(resolveDocsUrl(raw));
    },
    true
  );

  function init() {
    rewriteCurrentPageHashLinks();
    rewriteSearchResultLinks();
    var results = document.getElementById("search-results");
    if (results && typeof MutationObserver !== "undefined") {
      var observer = new MutationObserver(function () {
        rewriteSearchResultLinks(results);
      });
      observer.observe(results, { childList: true, subtree: true });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  // Expose for contract tests (Node VM).
  window.__lumibotDocsNavFix = {
    resolveDocsUrl: resolveDocsUrl,
    rewriteSearchResultLinks: rewriteSearchResultLinks,
    rewriteCurrentPageHashLinks: rewriteCurrentPageHashLinks,
    closeMobileOverlays: closeMobileOverlays
  };
})();
