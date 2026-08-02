import assert from "node:assert/strict";
import test from "node:test";

import { buildChart, errorChart, parseLastPage } from "../src/index.mjs";

test("parseLastPage reads the final GitHub pagination link", () => {
  const header = [
    '<https://api.github.com/repositories/1/stargazers?per_page=100&page=2>; rel="next"',
    '<https://api.github.com/repositories/1/stargazers?per_page=100&page=19>; rel="last"',
  ].join(", ");

  assert.equal(parseLastPage(header), 19);
  assert.equal(parseLastPage(null), 1);
});

test("buildChart renders current live data in both themes", () => {
  const history = {
    repository: "Lumiwealth/lumibot",
    repositoryUrl: "https://github.com/Lumiwealth/lumibot",
    createdAt: "2021-01-01T00:00:00Z",
    generatedAt: "2026-07-15T05:00:00Z",
    totalStars: 1812,
    starredAt: [
      Date.parse("2021-01-02T00:00:00Z"),
      Date.parse("2022-01-02T00:00:00Z"),
      Date.parse("2026-07-14T00:00:00Z"),
    ],
  };

  for (const theme of ["light", "dark"]) {
    const svg = buildChart(history, theme);
    assert.match(svg, /^<svg /);
    assert.match(svg, /LumiBot Project Growth/);
    assert.match(svg, /Live GitHub star history/);
    assert.match(svg, /1\.8k/);
    assert.match(svg, /Lumiwealth\/lumibot/);
    assert.doesNotMatch(svg, /undefined|NaN/);
  }
});

test("errorChart truncates before XML escaping", () => {
  const message = `${"x".repeat(119)}"more text`;
  const svg = errorChart(message, "light");

  assert.match(svg, new RegExp(`${"x".repeat(119)}&quot;</text>`));
  assert.doesNotMatch(svg, /&q<\/text>/);
});
