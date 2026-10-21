// SDD 3b.6: a page that only reads. Every piece of text that came from the base goes in through textContent, never as markup.
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var NS = 'http://www.w3.org/2000/svg';
  var state = { base: null, last: 0, snapshot: null, selected: null, timer: null, nodes: [], edges: [] };

  function el(tag, props, children) {
    var e = document.createElement(tag);
    Object.keys(props || {}).forEach(function (k) { e[k] = props[k]; });
    (children || []).forEach(function (c) { e.appendChild(typeof c === 'string' ? document.createTextNode(c) : c); });
    return e;
  }
  function say(text) { $('status').textContent = text || ''; }
  function api(path, asText) {
    var op = $('operator').value.trim();
    try { localStorage.setItem('stint-operator', op); } catch (e) { /* storage may be blocked */ }
    return fetch(path, { headers: { 'X-Stint-Operator': op } }).then(function (r) {
      if (r.status === 403) { throw new Error('Not allowed'); }
      if (!r.ok) { throw new Error('Request failed (' + r.status + ')'); }
      return asText ? r.text() : r.json();
    });
  }
  function url(suffix) { return '/graph/' + encodeURIComponent(state.base.namespace) + '/' + encodeURIComponent(state.base.base) + '/' + suffix; }
  function hue(type) { var h = 0; for (var i = 0; i < type.length; i++) { h = (h * 31 + type.charCodeAt(i)) % 360; } return h; }

  // --- bases and tabs -------------------------------------------------------------------------------------------
  function loadBases() {
    say('Loading...');
    api('/graph/bases').then(function (bases) {
      var select = $('base');
      select.textContent = '';
      bases.forEach(function (b, i) { select.appendChild(el('option', { value: String(i), textContent: b.namespace + ' / ' + b.base })); });
      state.bases = bases;
      state.base = bases[0] || null;
      say(bases.length ? '' : 'No base for this operator');
      if (state.base) { refreshAll(); }
    }).catch(function (e) { say(e.message); });
  }
  $('base').addEventListener('change', function () { state.base = state.bases[Number($('base').value)]; refreshAll(); });
  $('reload').addEventListener('click', function () { if (state.base) { refreshAll(); } else { loadBases(); } });
  document.querySelectorAll('nav button').forEach(function (b) {
    b.addEventListener('click', function () {
      document.querySelectorAll('nav button').forEach(function (x) { x.classList.toggle('on', x === b); });
      ['graph', 'changes', 'metrics', 'duplicates'].forEach(function (t) { $(t).hidden = (t !== b.dataset.tab); });
      if (b.dataset.tab === 'changes') { loadChanges(); } else if (b.dataset.tab === 'metrics') { loadMetrics(); }
      else if (b.dataset.tab === 'duplicates') { loadDuplicates(); }
    });
  });
  $('export').addEventListener('click', function () {
    if (!state.base) { return; }
    api(url('export.graphml'), true).then(function (xml) {
      var a = el('a', { href: URL.createObjectURL(new Blob([xml], { type: 'application/graphml+xml' })), download: 'graph.graphml' });
      document.body.appendChild(a); a.click(); a.remove();
    }).catch(function (e) { say(e.message); });
  });

  function refreshAll() {
    stop();
    api(url('view')).then(function (v) {
      state.last = v.lastSequence;
      $('slider').max = String(v.lastSequence);
      $('slider').value = String(v.lastSequence);
      showSnapshot(v.lastSequence);
    }).catch(function (e) { say(e.message); });
  }

  // --- the graph: a snapshot at a point of the log ---------------------------------------------------------------
  function showSnapshot(seq) {
    api(url('snapshot?seq=' + seq)).then(function (s) {
      state.snapshot = s;
      $('seq').textContent = 'entry ' + s.sequence + (s.at ? ' (' + s.at + ')' : '');
      var chains = $('chain'), keep = chains.value;
      chains.textContent = '';
      chains.appendChild(el('option', { value: '', textContent: '(none)' }));
      Object.keys(s.nodesByChain).sort().forEach(function (c) { chains.appendChild(el('option', { value: c, textContent: c + ' (' + s.nodesByChain[c] + ')' })); });
      chains.value = keep;
      $('note').textContent = s.aggregated
        ? 'Too many nodes to draw (' + Object.values(s.nodesByType).reduce(function (a, b) { return a + b; }, 0) + '): only counts by type are shown: ' + JSON.stringify(s.nodesByType)
        : 'A name appears only for what still exists and is not restricted; the log never held one.' + (s.erased.length ? ' ' + s.erased.length + ' erased id(s).' : '');
      layout(s);
      draw();
    }).catch(function (e) { say(e.message); });
  }
  $('slider').addEventListener('input', function () { stop(); showSnapshot(Number($('slider').value)); });
  $('chain').addEventListener('change', draw);
  function stop() { if (state.timer) { clearInterval(state.timer); state.timer = null; } $('play').textContent = 'Play'; }
  $('play').addEventListener('click', function () {
    if (state.timer) { stop(); return; }
    $('play').textContent = 'Pause';
    var at = Number($('slider').value) >= state.last ? 0 : Number($('slider').value);
    state.timer = setInterval(function () {
      at += Math.max(1, Math.ceil(state.last / 60));
      if (at >= state.last) { at = state.last; stop(); }
      $('slider').value = String(at);
      showSnapshot(at);
    }, 700);
  });

  // a small force layout, run for a fixed number of steps; positions are kept by id so the picture does not jump between steps
  var pos = {};
  function layout(s) {
    var nodes = s.nodes, W = 800, H = 560, i, j;
    nodes.forEach(function (n) { if (!pos[n.id]) { pos[n.id] = { x: W / 2 + (Math.random() - .5) * 200, y: H / 2 + (Math.random() - .5) * 200 }; } });
    var index = {}; nodes.forEach(function (n) { index[n.id] = n; });
    for (var step = 0; step < 120; step++) {
      var fx = {}, fy = {};
      nodes.forEach(function (n) { fx[n.id] = (W / 2 - pos[n.id].x) * 0.01; fy[n.id] = (H / 2 - pos[n.id].y) * 0.01; });
      for (i = 0; i < nodes.length; i++) {
        for (j = i + 1; j < nodes.length; j++) {
          var a = pos[nodes[i].id], b = pos[nodes[j].id], dx = a.x - b.x, dy = a.y - b.y, d2 = dx * dx + dy * dy + 1, f = 1800 / d2;
          fx[nodes[i].id] += dx * f; fy[nodes[i].id] += dy * f; fx[nodes[j].id] -= dx * f; fy[nodes[j].id] -= dy * f;
        }
      }
      s.edges.forEach(function (e) {
        var a = pos[e.from], b = pos[e.to]; if (!a || !b) { return; }
        var dx = b.x - a.x, dy = b.y - a.y, d = Math.sqrt(dx * dx + dy * dy) + 1, f = (d - 90) * 0.02;
        fx[e.from] += dx / d * f * d * 0.1; fy[e.from] += dy / d * f * d * 0.1; fx[e.to] -= dx / d * f * d * 0.1; fy[e.to] -= dy / d * f * d * 0.1;
      });
      nodes.forEach(function (n) {
        var p = pos[n.id]; p.x = Math.max(20, Math.min(W - 20, p.x + Math.max(-8, Math.min(8, fx[n.id]))));
        p.y = Math.max(20, Math.min(H - 20, p.y + Math.max(-8, Math.min(8, fy[n.id]))));
      });
    }
  }
  function draw() {
    var s = state.snapshot, svg = $('canvas'), chain = $('chain').value;
    svg.textContent = '';
    svg.setAttribute('viewBox', '0 0 800 560');
    if (!s || s.aggregated) { return; }
    s.edges.forEach(function (e) {
      var a = pos[e.from], b = pos[e.to]; if (!a || !b) { return; }
      var line = document.createElementNS(NS, 'line');
      line.setAttribute('x1', a.x); line.setAttribute('y1', a.y); line.setAttribute('x2', b.x); line.setAttribute('y2', b.y);
      if (chain && e.chainId === chain) { line.setAttribute('class', 'hot'); }
      svg.appendChild(line);
    });
    s.nodes.forEach(function (n) {
      var p = pos[n.id], c = document.createElementNS(NS, 'circle'), cls = [];
      c.setAttribute('cx', p.x); c.setAttribute('cy', p.y); c.setAttribute('r', n.level === 'RESTRICTED' ? 7 : 9);
      c.setAttribute('fill', 'hsl(' + hue(n.type || '') + ',65%,55%)');
      if (n.level === 'RESTRICTED') { cls.push('restricted'); }
      if (chain && n.chainId !== chain) { cls.push('dim'); }
      if (state.selected === n.id) { cls.push('sel'); }
      c.setAttribute('class', cls.join(' '));
      c.addEventListener('click', function () { state.selected = n.id; detail(n); draw(); });
      var title = document.createElementNS(NS, 'title'); title.textContent = n.type + (n.label ? ': ' + n.label : ' (masked)');
      c.appendChild(title);
      svg.appendChild(c);
      if (n.label) {
        var t = document.createElementNS(NS, 'text'); t.setAttribute('x', p.x + 11); t.setAttribute('y', p.y + 4); t.textContent = n.label;
        svg.appendChild(t);
      }
    });
  }
  function detail(n) {
    var box = $('detail'), rows = [['id', n.id], ['type', n.type], ['level', n.level], ['name', n.label || (n.level === 'RESTRICTED' ? '(masked)' : '(gone or unknown)')],
      ['added', 'entry ' + n.addedSequence + ' at ' + n.addedAt], ['chain', n.chainId || ''], ['instance', n.instanceId || ''],
      ['definition', n.definition || ''], ['schema', n.schemaVersion || ''], ['document', n.documentId || '']];
    box.textContent = '';
    var dl = el('dl');
    rows.forEach(function (r) { dl.appendChild(el('dt', { textContent: r[0] })); dl.appendChild(el('dd', { textContent: r[1] })); });
    box.appendChild(dl);
  }

  // --- the timeline -----------------------------------------------------------------------------------------------
  function loadChanges() {
    var q = ['chain', 'instance', 'type', 'document'].map(function (k) { var v = $('f-' + k).value.trim(); return v ? k + '=' + encodeURIComponent(v) : ''; })
      .filter(Boolean).join('&');
    api(url('changes?limit=500' + (q ? '&' + q : ''))).then(function (rows) {
      var t = $('changes-table'); t.textContent = '';
      var head = el('tr'); ['#', 'at', 'op', 'kind', 'type', 'target', 'level', 'chain', 'instance', 'document', 'reason'].forEach(function (h) { head.appendChild(el('th', { textContent: h })); });
      t.appendChild(head);
      rows.forEach(function (c) {
        var tr = el('tr');
        [c.sequence, c.at, c.op, c.kind, c.elementType, c.targetId, c.level, c.chainId, c.instanceId, c.documentId, c.reason]
          .forEach(function (v) { tr.appendChild(el('td', { textContent: v == null ? '' : String(v) })); });
        t.appendChild(tr);
      });
    }).catch(function (e) { say(e.message); });
  }
  $('f-apply').addEventListener('click', loadChanges);

  // --- metrics ------------------------------------------------------------------------------------------------------
  function table(rows, heads) {
    var t = el('table'), h = el('tr');
    heads.forEach(function (x) { h.appendChild(el('th', { textContent: x })); });
    t.appendChild(h);
    rows.forEach(function (r) { var tr = el('tr'); r.forEach(function (v) { tr.appendChild(el('td', { textContent: String(v) })); }); t.appendChild(tr); });
    return t;
  }
  function loadMetrics() {
    api(url('metrics')).then(function (m) {
      var box = $('metrics'); box.textContent = '';
      var cards = el('div', { className: 'cards' });
      [['nodes', m.nodes], ['edges', m.edges], ['passages', m.passages], ['links', m.links], ['restricted', Math.round(m.restrictedRatio * 100) + '%'],
       ['erasures', m.erasures], ['expirations', m.expirations], ['reindexes', m.reindexes], ['duplicate nodes', m.duplicateNodes]]
        .forEach(function (c) { cards.appendChild(el('div', { className: 'card' }, [c[0], el('b', { textContent: String(c[1]) })])); });
      cards.appendChild(el('div', { className: 'card ' + m.alert }, ['state', el('b', { textContent: m.alert + (m.reasons.length ? ' (' + m.reasons.join(', ') + ')' : '') })]));
      box.appendChild(cards);
      box.appendChild(chart(m.series));
      box.appendChild(el('h3', { textContent: 'Nodes by type' }));
      box.appendChild(table(Object.keys(m.nodesByType).sort().map(function (k) { return [k, m.nodesByType[k]]; }), ['type', 'count']));
      box.appendChild(el('h3', { textContent: 'Added by chain' }));
      box.appendChild(table(Object.keys(m.addedByChain).sort().map(function (k) { return [k, m.addedByChain[k]]; }), ['chain', 'added']));
      box.appendChild(el('h3', { textContent: 'Added by definition' }));
      box.appendChild(table(Object.keys(m.addedByDefinition).sort().map(function (k) { return [k, m.addedByDefinition[k]]; }), ['definition', 'added']));
    }).catch(function (e) { say(e.message); });
  }
  function chart(series) {
    var svg = document.createElementNS(NS, 'svg'), W = 640, H = 160;
    svg.setAttribute('viewBox', '0 0 ' + W + ' ' + H); svg.setAttribute('width', '100%'); svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', 'Nodes and edges by day');
    if (!series.length) { return svg; }
    var max = Math.max.apply(null, series.map(function (p) { return Math.max(p.nodes, p.edges, 1); }));
    [['nodes', 'var(--accent)'], ['edges', 'var(--warn)']].forEach(function (s) {
      var pts = series.map(function (p, i) { return (series.length === 1 ? W / 2 : 10 + i * (W - 20) / (series.length - 1)) + ',' + (H - 12 - p[s[0]] / max * (H - 28)); }).join(' ');
      var line = document.createElementNS(NS, 'polyline');
      line.setAttribute('points', pts); line.setAttribute('fill', 'none'); line.setAttribute('stroke', s[1]); line.setAttribute('stroke-width', '2');
      svg.appendChild(line);
    });
    var cap = document.createElementNS(NS, 'text'); cap.setAttribute('x', 10); cap.setAttribute('y', 12);
    cap.textContent = 'nodes (blue), edges (orange) by day; ' + series[0].day + ' to ' + series[series.length - 1].day;
    svg.appendChild(cap);
    return svg;
  }

  // --- duplicates --------------------------------------------------------------------------------------------------
  function loadDuplicates() {
    api(url('duplicates')).then(function (rows) {
      var box = $('duplicates'); box.textContent = '';
      box.appendChild(el('p', { className: 'muted', textContent: 'Pairs that may be one thing written two ways. Nothing here merges: fixing it is a re-ingestion with the glossary.' }));
      box.appendChild(rows.length ? table(rows.map(function (r) {
        return [r.type, r.aLabel + ' (' + r.aId + ', ' + r.aEvidence + ' doc)', r.bLabel + ' (' + r.bId + ', ' + r.bEvidence + ' doc)', r.criterion];
      }), ['type', 'one', 'other', 'why']) : el('p', { textContent: 'None found.' }));
    }).catch(function (e) { say(e.message); });
  }

  try { $('operator').value = localStorage.getItem('stint-operator') || ''; } catch (e) { /* storage may be blocked */ }
  $('operator').addEventListener('change', loadBases);
  loadBases();
}());
