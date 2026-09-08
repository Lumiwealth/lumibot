(function () {
  "use strict";

  var hostname = window.location.hostname;
  var isProductionDocs =
    hostname === "lumibot.lumiwealth.com" ||
    hostname === "www.lumibot.lumiwealth.com";

  if (!isProductionDocs) {
    return;
  }

  var posthogKey = "phc_BjxXrbWQHXV7UnjhtkUuMGaVB3iMg4Tq68KGcAZxwAE";
  var posthogHost = "https://us.i.posthog.com";

  function getParam(name) {
    return new URLSearchParams(window.location.search).get(name) || "";
  }

  function baseProperties() {
    return {
      site: "lumibot_docs",
      docs_host: window.location.hostname,
      page_path: window.location.pathname,
      page_title: document.title,
      referrer: document.referrer || "",
      utm_source: getParam("utm_source"),
      utm_medium: getParam("utm_medium"),
      utm_campaign: getParam("utm_campaign"),
      utm_content: getParam("utm_content"),
      utm_term: getParam("utm_term")
    };
  }

  function capture(name, properties) {
    if (!window.posthog || typeof window.posthog.capture !== "function") {
      return;
    }
    window.posthog.capture(name, Object.assign(baseProperties(), properties || {}));
  }

  window.__lumibotDocsPostHogLoaded = window.__lumibotDocsPostHogLoaded || false;
  if (window.__lumibotDocsPostHogLoaded) {
    return;
  }
  window.__lumibotDocsPostHogLoaded = true;

  var script = document.createElement("script");
  script.async = true;
  script.src = "https://us-assets.i.posthog.com/static/array.js";
  script.onload = function () {
    if (!window.posthog || typeof window.posthog.init !== "function") {
      return;
    }

    window.posthog.init(posthogKey, {
      api_host: posthogHost,
      person_profiles: "identified_only",
      capture_pageview: true,
      capture_pageleave: true,
      autocapture: true,
      persistence: "localStorage+cookie",
      loaded: function (posthog) {
        posthog.register({
          site: "lumibot_docs",
          docs_host: window.location.hostname
        });
        capture("lumibot_docs_pageview");
      }
    });
  };
  document.head.appendChild(script);

  document.addEventListener("click", function (event) {
    var link = event.target && event.target.closest ? event.target.closest("a[href]") : null;
    if (!link) {
      return;
    }

    var href = link.href || "";
    if (!href) {
      return;
    }

    var route = "";
    if (link.closest(".lumibot-start-routes")) {
      if (link.hash === "#first-python-backtest") route = "python_backtest";
      else if (link.pathname.endsWith("/agents_quickstart.html")) route = "ai_agent";
      else if (link.pathname.endsWith("/agents_example_ai_iron_condor.html")) route = "options";
    } else if (link.closest(".lumibot-partnership-route")) {
      route = "partnership";
    }
    if (route) {
      capture("lumibot_docs_start_click", {
        route: route,
        link_path: link.pathname || "",
        link_hash: link.hash || ""
      });
    }
    if (link.protocol === "mailto:") {
      if (window.location.pathname.endsWith("/PARTNERSHIPS.html")) {
        capture("lumibot_docs_partnership_contact_click", { route: "email" });
      }
      return;
    }

    if (link.hostname === "botspot.trade" || link.hostname.endsWith(".botspot.trade")) {
      capture("lumibot_docs_botspot_click", {
        link_text: (link.textContent || "").trim().slice(0, 120),
        link_href: href,
        link_path: link.pathname || ""
      });
      return;
    }

    if (link.hostname && link.hostname !== window.location.hostname) {
      capture("lumibot_docs_outbound_click", {
        link_text: (link.textContent || "").trim().slice(0, 120),
        link_href: href,
        link_host: link.hostname
      });
    }
  });
})();
