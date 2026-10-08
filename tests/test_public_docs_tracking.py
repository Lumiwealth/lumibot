"""Execute the browser analytics script in a deterministic JavaScript VM.

No browser, network, production events, or model calls are used.
"""
import json
import shutil
import subprocess
from pathlib import Path


def test_docs_route_events_and_outbound_classification():
    node = shutil.which("node")
    assert node, "Node.js is required for the documentation JavaScript contract test"
    source = Path(__file__).resolve().parents[1] / "docsrc/_html/posthog.js"
    program = r'''
const fs = require('fs');
const vm = require('vm');
const source = fs.readFileSync(process.argv[1], 'utf8');
function setup(url) {
  const events = [], listeners = [];
  const location = new URL(url);
  const window = {location, posthog: {
    capture: (name, properties) => events.push({name, properties}),
    register: () => {}, init: (_key, config) => config.loaded(window.posthog)
  }};
  const document = {title: 'LumiBot', referrer: '',
    createElement: () => ({}), head: {appendChild: script => script.onload()},
    addEventListener: (_type, callback) => listeners.push(callback)};
  const context = vm.createContext({window, document, URLSearchParams});
  vm.runInContext(source, context);
  vm.runInContext(source, context);
  function click(href, group) {
    const link = new URL(href, location);
    link.textContent = 'Example';
    link.closest = selector => selector === group ? {} : null;
    listeners.forEach(callback => callback({target: {closest: () => link}}));
  }
  return {events, listeners, click};
}
const home = setup('https://lumibot.lumiwealth.com/index.html?utm_source=qa');
home.click('#first-python-backtest', '.lumibot-start-routes');
home.click('agents_quickstart.html', '.lumibot-start-routes');
home.click('agents_example_ai_iron_condor.html', '.lumibot-start-routes');
home.click('PARTNERSHIPS.html', '.lumibot-partnership-route');
home.click('agents_quickstart.html', null);
home.click('agents_examples.html', '.lumibot-start-routes');
home.click('examples.html', '.lumibot-start-routes');
home.click('https://botspot.trade/challenges?utm_content=learn_with_rob', null);
home.click('https://botspot.trade/marketplace/strategy/example', null);
home.click('https://botspot.trade.evil.example/path', null);
home.click('https://example.org/?next=botspot.trade', null);
const partner = setup('https://lumibot.lumiwealth.com/PARTNERSHIPS.html');
partner.click('mailto:team@example.org?subject=Hello', null);
const local = setup('http://localhost:8765/index.html');
local.click('agents_quickstart.html', '.lumibot-start-routes');
console.log(JSON.stringify({home: home.events, listeners: home.listeners.length,
  partner: partner.events, local: local.events}));
'''
    result = subprocess.run(
        [node, "-e", program, str(source)], text=True, capture_output=True,
        check=True, timeout=15,
    )
    data = json.loads(result.stdout)
    assert data["listeners"] == 1
    routes = [e["properties"]["route"] for e in data["home"] if e["name"] == "lumibot_docs_start_click"]
    assert routes == ["python_backtest", "ai_agent", "options", "partnership", "ai_examples", "python_examples"]
    destinations = [
        (e["properties"]["link_path"], e["properties"]["link_hash"])
        for e in data["home"] if e["name"] == "lumibot_docs_start_click"
    ]
    assert destinations == [
        ("/index.html", "#first-python-backtest"),
        ("/agents_quickstart.html", ""),
        ("/agents_example_ai_iron_condor.html", ""),
        ("/PARTNERSHIPS.html", ""),
        ("/agents_examples.html", ""),
        ("/examples.html", ""),
    ]
    names = [event["name"] for event in data["home"]]
    assert names.count("lumibot_docs_pageview") == 1
    # The new education CTA reuses the existing event with a distinct destination.
    assert names.count("lumibot_docs_botspot_click") == 2
    education = [e for e in data["home"] if e["name"] == "lumibot_docs_botspot_click"
                 and e["properties"].get("destination") == "challenge"]
    assert len(education) == 1
    assert education[0]["properties"]["link_utm_content"] == "learn_with_rob"
    assert names.count("lumibot_docs_outbound_click") == 2
    contact = [e for e in data["partner"] if e["name"] == "lumibot_docs_partnership_contact_click"]
    assert len(contact) == 1
    assert contact[0]["properties"]["route"] == "email"
    assert "link_href" not in contact[0]["properties"]
    assert data["local"] == []


def test_docs_tracking_filters_browser_extension_exceptions():
    node = shutil.which("node")
    assert node, "Node.js is required for the documentation JavaScript contract test"
    source = Path(__file__).resolve().parents[1] / "docsrc/_html/posthog.js"
    program = r'''
const assert = require('assert');
const fs = require('fs');
const vm = require('vm');
let config;
const window = {
  location: new URL('https://lumibot.lumiwealth.com/index.html'),
  posthog: {init: (_key, options) => { config = options; }}
};
const document = {
  createElement: () => ({}), head: {appendChild: script => script.onload()},
  addEventListener: () => {}
};
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'),
  vm.createContext({window, document, URLSearchParams}));
assert.strictEqual(typeof config.before_send, 'function');
function exception(value, frames) {
  return {event: '$exception', properties: {
    $exception_list: [{value, stacktrace: {frames}}]
  }};
}
const safariMessage = 'No Listener: tabs:outgoing.message.ready';
const genericMessage = 'TypeError: Cannot read properties of undefined';
const extensionFrame = {filename: 'chrome-extension://example/content.js', in_app: false};
const firstPartyFrame = {filename: 'https://lumibot.lumiwealth.com/_static/foo.js'};
const dropped = [
  exception(safariMessage, [
    {filename: 'webkit-masked-url://hidden/', in_app: false},
    {filename: 'webkit-masked-url://hidden/', in_app: false}
  ]),
  exception(genericMessage, [extensionFrame, extensionFrame]),
  exception(genericMessage, [
    {filename: 'moz-extension://example/a.js'}, {filename: 'moz-extension://example/b.js'}
  ]),
  exception(genericMessage, [{filename: 'SAFARI-WEB-EXTENSION://example/a.js'}]),
  exception(genericMessage, [{filename: 'safari-extension://example/a.js'}]),
  exception(genericMessage, [{filename: '', abs_path: 'CHROME-EXTENSION://example/a.js'}]),
  exception(genericMessage, [{url: 'moz-extension://example/a.js'}, {}]),
  exception(genericMessage, [extensionFrame, {filename: ''}]),
  exception('Extension context invalidated.', []),
  exception(safariMessage, undefined),
  exception(safariMessage, [{filename: ''}, {}]),
  {event: '$exception', properties: {$exception_message: safariMessage}},
  {event: '$exception', properties: {$exception_list: [
    {value: genericMessage}, {value: safariMessage}
  ]}}
];
for (const message of [
  'chrome.runtime failed', 'browser.runtime failed', 'runtime.sendMessage failed',
  'runtime.connect failed', 'tabs:outgoing failed', 'Receiving end does not exist',
  'chrome-extension://example/a.js', 'moz-extension://example/a.js',
  'safari-web-extension://example/a.js', 'webkit-masked-url://hidden/'
]) {
  dropped.push(exception(message, []));
}
const kept = [
  exception(safariMessage, [extensionFrame, firstPartyFrame]),
  exception('Extension context invalidated.', [firstPartyFrame]),
  exception(safariMessage, [{filename: 'https://us-assets.i.posthog.com/static/array.js'}]),
  exception(genericMessage, []),
  exception(genericMessage, [{filename: ''}, {}]),
  exception('The extension failed to load', []),
  exception(safariMessage, [{filename: extensionFrame.filename, abs_path: firstPartyFrame.filename}]),
  {event: '$exception', properties: {$exception_list: [
    {value: safariMessage, stacktrace: {frames: [extensionFrame]}},
    {stacktrace: {frames: [{url: firstPartyFrame.filename}]}}
  ]}},
  {event: '$pageview', properties: {$exception_message: safariMessage}},
  {event: '$exception'},
  {event: '$exception', properties: {$exception_list: [null, {}]}},
  undefined,
  null
];
// Unexpected payload access errors must preserve the original event.
const malformed = {event: '$exception'};
Object.defineProperty(malformed, 'properties', {get() { throw new Error('bad payload'); }});
kept.push(malformed);
for (const event of dropped) assert.strictEqual(config.before_send(event), null);
for (const event of kept) assert.strictEqual(config.before_send(event), event);
'''
    subprocess.run(
        [node, "-e", program, str(source)], text=True, capture_output=True,
        check=True, timeout=15,
    )
