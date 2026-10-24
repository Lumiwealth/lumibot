"""Contract tests for documentation search/sidebar navigation hardening.

Runs docs_nav_fix.js in a Node VM (no browser, no network). Covers the
Regression: search-result clicks must leave search.html,
and current-page href="#" links must resolve to a real document path.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path


NODE_PROGRAM = r"""
const fs = require('fs');
const vm = require('vm');
const source = fs.readFileSync(process.argv[1], 'utf8');

function makeDoc(url, {searchHref = null, contentRoot = './'} = {}) {
  const location = new URL(url);
  const assignments = [];
  const listeners = [];

  function el(tag, attrs = {}) {
    const node = {
      tagName: String(tag).toUpperCase(),
      attrs: Object.assign({}, attrs),
      children: [],
      parentElement: null,
      className: attrs.class || '',
      checked: !!attrs.checked,
      getAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attrs, name) ? this.attrs[name] : null; },
      setAttribute(name, value) { this.attrs[name] = String(value); },
      closest(selector) {
        let cur = this;
        while (cur) {
          if (selector === '#search-results' && cur.id === 'search-results') return cur;
          if (selector === '#search-results a[href]' && cur.tagName === 'A' && cur.inSearch) return cur;
          if (selector === '.sidebar-tree a[href]' && cur.tagName === 'A' && cur.inSidebar) return cur;
          cur = cur.parentElement;
        }
        return null;
      },
      querySelectorAll(selector) {
        const out = [];
        const walk = (n) => {
          if (!n) return;
          if (selector === 'a[href]' && n.tagName === 'A' && n.getAttribute('href') != null) out.push(n);
          (n.children || []).forEach(walk);
        };
        walk(this);
        return out;
      },
    };
    if (attrs.id) node.id = attrs.id;
    if (Object.prototype.hasOwnProperty.call(attrs, 'href')) {
      Object.defineProperty(node, 'href', {
        get() {
          try { return new URL(this.getAttribute('href'), location.href).href; }
          catch (e) { return this.getAttribute('href'); }
        },
        set(v) { this.setAttribute('href', v); },
      });
    }
    return node;
  }

  const searchResults = el('div', {id: 'search-results'});
  searchResults.id = 'search-results';
  let searchAnchor = null;
  if (searchHref) {
    searchAnchor = el('a', {href: searchHref});
    searchAnchor.inSearch = true;
    searchAnchor.parentElement = searchResults;
    searchResults.children.push(searchAnchor);
  }

  const current = el('a', {href: '#', class: 'current reference internal'});
  current.className = 'current reference internal';
  current.inSidebar = true;

  const navToggle = el('input', {id: '__navigation'});
  navToggle.id = '__navigation';
  navToggle.checked = true;
  const tocToggle = el('input', {id: '__toc'});
  tocToggle.id = '__toc';
  tocToggle.checked = false;

  const document = {
    documentElement: {
      getAttribute: (name) => (name === 'data-content_root' ? contentRoot : null),
    },
    readyState: 'complete',
    getElementById: (id) => {
      if (id === 'search-results') return searchResults;
      if (id === '__navigation') return navToggle;
      if (id === '__toc') return tocToggle;
      return null;
    },
    querySelectorAll: (selector) => {
      if (selector.includes('[href=\'#\']') || selector.includes('[href="#"]')) {
        return current.getAttribute('href') === '#' ? [current] : [];
      }
      return [];
    },
    addEventListener: (type, cb, opts) => listeners.push({type, cb, opts}),
  };

  const windowObj = {
    location: {
      href: location.href,
      pathname: location.pathname,
      assign: (href) => assignments.push(href),
    },
  };

  const context = vm.createContext({
    window: windowObj,
    document,
    URL,
    MutationObserver: undefined,
  });
  vm.runInContext(source, context);
  return {
    api: context.window.__lumibotDocsNavFix,
    searchResults,
    searchAnchor,
    current,
    navToggle,
    listeners,
    assignments,
  };
}

const searchPage = makeDoc('https://lumibot.lumiwealth.com/search.html?q=create_order', {
  searchHref: 'strategy_methods.orders/lumibot.strategies.strategy.Strategy.create_order.html',
  contentRoot: './',
});
searchPage.api.rewriteSearchResultLinks(searchPage.searchResults);
const rewritten = searchPage.searchResults.children[0].getAttribute('href');

const clickListener = searchPage.listeners.find((l) => l.type === 'click');
const evt = {
  target: searchPage.searchAnchor,
  prevented: false,
  button: 0,
  metaKey: false,
  ctrlKey: false,
  shiftKey: false,
  altKey: false,
  defaultPrevented: false,
  preventDefault() { this.prevented = true; this.defaultPrevented = true; },
};
clickListener.cb(evt);

const nested = makeDoc(
  'https://lumibot.lumiwealth.com/strategy_methods.datetime/lumibot.strategies.strategy.Strategy.get_datetime.html',
  {contentRoot: '../'}
);
nested.api.rewriteCurrentPageHashLinks();

const resolvedNested = nested.api.resolveDocsUrl(
  '../strategy_methods.orders/lumibot.strategies.strategy.Strategy.submit_order.html'
);

const mobile = makeDoc('https://lumibot.lumiwealth.com/search.html', {contentRoot: './'});
mobile.navToggle.checked = true;
mobile.api.closeMobileOverlays();

console.log(JSON.stringify({
  rewritten,
  assignments: searchPage.assignments,
  prevented: evt.prevented,
  currentHref: nested.current.getAttribute('href'),
  resolvedNested,
  navClosed: mobile.navToggle.checked === false,
  clickCapture: !!(clickListener && clickListener.opts === true),
}));
"""


def test_docs_nav_fix_search_and_current_page_links():
    node = shutil.which("node")
    assert node, "Node.js is required for the documentation navigation contract test"
    source = Path(__file__).resolve().parents[1] / "docsrc/_html/docs_nav_fix.js"
    assert source.is_file()

    result = subprocess.run(
        [node, "-e", NODE_PROGRAM, str(source)],
        text=True,
        capture_output=True,
        check=True,
        timeout=15,
    )
    data = json.loads(result.stdout)

    assert data["rewritten"].endswith(
        "/strategy_methods.orders/lumibot.strategies.strategy.Strategy.create_order.html"
    ), data["rewritten"]
    assert data["assignments"], "search result click must assign a navigation URL"
    assert data["assignments"][0].endswith(
        "/strategy_methods.orders/lumibot.strategies.strategy.Strategy.create_order.html"
    ), data["assignments"]
    assert not data["assignments"][0].rstrip("/").endswith("search.html")
    assert data["prevented"] is True
    assert data["currentHref"] == (
        "lumibot.strategies.strategy.Strategy.get_datetime.html"
    ), data["currentHref"]
    assert data["resolvedNested"].endswith(
        "/strategy_methods.orders/lumibot.strategies.strategy.Strategy.submit_order.html"
    ), data["resolvedNested"]
    assert data["navClosed"] is True
    assert data["clickCapture"] is True


def test_docs_nav_fix_is_registered_in_sphinx_conf():
    conf = Path(__file__).resolve().parents[1] / "docsrc" / "conf.py"
    text = conf.read_text(encoding="utf-8")
    assert "docs_nav_fix.js" in text
    assert "posthog.js" in text
