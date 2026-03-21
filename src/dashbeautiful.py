import html


def build_dashboard_html(title: str, source_path: str, payload_json: str) -> str:
    title_html = html.escape(str(title or ""))
    source_html = html.escape(str(source_path or ""))

    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>{title_html}</title>
  <style>
    :root {{
      --bg: #f4f8ef;
      --card: #ffffff;
      --ink: #1f2933;
      --muted: #5f6c7b;
      --accent: #0b8a76;
      --line: #d7e2d0;
      --chip: #edf6f3;
    }}
    * {{ box-sizing: border-box; }}
    html, body {{ width: 100%; overflow-x: hidden; }}
    body {{
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(1200px 700px at -20% -20%, #d3f2e2 0%, rgba(211,242,226,0) 60%),
        radial-gradient(900px 600px at 120% 0%, #ffd9c8 0%, rgba(255,217,200,0) 62%),
        var(--bg);
      font-family: \"Trebuchet MS\", \"Segoe UI\", sans-serif;
    }}
    .top {{
      padding: 14px 18px;
      position: sticky;
      top: 0;
      z-index: 20;
      backdrop-filter: blur(8px);
      background: rgba(244, 248, 239, 0.9);
      border-bottom: 1px solid var(--line);
      display: flex;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
    }}
    h1 {{ margin: 0; font-size: 1.16rem; letter-spacing: 0.02em; }}
    .sub {{ color: var(--muted); margin-top: 3px; font-size: 0.85rem; }}
    .layout {{
      width: min(1460px, 100%);
      margin: 0 auto;
      padding: 14px;
      display: grid;
      gap: 12px;
      max-width: 100%;
    }}
    .top-grid {{
      display: grid;
      gap: 12px;
      grid-template-columns: minmax(0, 1fr);
      align-items: start;
      min-width: 0;
    }}
    .cards {{ display: grid; gap: 10px; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); }}
    .card {{ background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 10px; box-shadow: 0 5px 14px rgba(22,45,29,0.06); }}
    .card .k {{ font-size: 1.1rem; font-weight: 700; }}
    .card .l {{ color: var(--muted); font-size: 0.82rem; margin-top: 2px; }}
    .panel {{ background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 10px; min-width: 0; }}
    .panel h2 {{ margin: 0 0 8px 0; font-size: 0.96rem; }}
    .section-head {{ display: flex; justify-content: space-between; align-items: center; gap: 8px; margin-bottom: 8px; }}
    .section-head h2 {{ margin: 0; }}
    .collapse-btn {{
      border: 1px solid #8eb7ad;
      background: #f7fffd;
      color: #0f5f52;
      border-radius: 8px;
      padding: 5px 9px;
      font-size: 0.78rem;
      cursor: pointer;
      white-space: nowrap;
    }}
    .panel-body.is-collapsed {{ display: none; }}
    .controls {{ display: grid; gap: 8px; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); }}
    input, select {{ width: 100%; border: 1px solid #b8c9b0; border-radius: 8px; padding: 8px 10px; background: #fff; }}
    .checks {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 8px; font-size: 0.88rem; }}
    .checks label {{ display: inline-flex; gap: 6px; align-items: center; }}
    .tiny {{ font-size: 0.78rem; }}
    .muted {{ color: var(--muted); }}
    .chart-grid {{ display: grid; gap: 10px; grid-template-columns: repeat(2, minmax(0, 1fr)); min-width: 0; }}
    .bars {{ display: grid; gap: 6px; }}
    .bar-row {{ display: grid; gap: 6px; grid-template-columns: minmax(80px, 1fr) 2fr auto; align-items: center; font-size: 0.82rem; }}
    .bar-label {{ white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .bar-wrap {{ height: 10px; border-radius: 999px; background: #edf2ea; overflow: hidden; }}
    .bar-fill {{ height: 100%; background: linear-gradient(90deg, var(--accent), #43b8a3); }}
    .count {{ color: var(--muted); font-variant-numeric: tabular-nums; }}

    .cols {{ display: grid; gap: 6px; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); margin-top: 8px; }}
    .cols label {{ font-size: 0.82rem; display: inline-flex; gap: 6px; align-items: center; }}

    .table-wrap {{
      width: 100%;
      max-width: 100%;
      max-height: 58vh;
      overflow-x: auto;
      overflow-y: auto;
      overscroll-behavior-x: contain;
      -webkit-overflow-scrolling: touch;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
    }}
    body.tight-table .table-wrap {{ max-height: 70vh; }}
    body.ultra-tight-table .table-wrap {{ max-height: 80vh; }}
    table {{
      width: max-content;
      min-width: 2200px;
      border-collapse: collapse;
      font-size: 0.82rem;
    }}
    th, td {{ border-bottom: 1px solid #e4ecdf; padding: 6px 6px; text-align: left; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: #f9fcf7; z-index: 1; white-space: nowrap; }}
    .pill {{ display: inline-block; padding: 1px 7px; border-radius: 999px; background: var(--chip); border: 1px solid #c9e6de; font-size: 0.74rem; margin: 0 4px 4px 0; }}
    .col-tb_key {{ width: 240px; min-width: 660px; max-width: 660px; }}
    .cell-inline {{ display: inline-flex; align-items: center; gap: 6px; max-width: 100%; }}
    .cell-clip {{
      display: inline-block;
      max-width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      vertical-align: top;
    }}
    .cell-more {{
      border: 1px solid #9dbdb3;
      background: #f3faf8;
      color: #0f5f52;
      border-radius: 7px;
      font-size: 0.72rem;
      padding: 2px 7px;
      line-height: 1.25;
      flex: 0 0 auto;
      cursor: pointer;
    }}

    .pager {{ margin-top: 8px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
    .page-buttons {{ display: inline-flex; gap: 4px; align-items: center; flex-wrap: wrap; }}
    .page-btn {{
      min-width: 34px;
      text-align: center;
      padding: 5px 7px;
    }}
    .page-btn.active {{
      background: #0f5f52;
      color: #fff;
      border-color: #0f5f52;
    }}
    .ellipsis {{ color: var(--muted); padding: 0 4px; }}
    .rows-wrap {{ display: inline-flex; align-items: center; gap: 6px; }}
    .rows-wrap select {{ width: auto; min-width: 104px; padding: 6px 8px; }}
    button {{ border: 1px solid #8eb7ad; background: #f7fffd; color: #0f5f52; border-radius: 8px; padding: 6px 10px; cursor: pointer; }}
    button:disabled {{ opacity: 0.55; cursor: default; }}

    .help-toggle {{ white-space: nowrap; }}
    .overlay {{ position: fixed; inset: 0; background: rgba(9, 18, 14, 0.38); z-index: 39; display: none; }}
    .sidebar {{
      position: fixed;
      right: 0;
      top: 0;
      height: 100vh;
      width: min(760px, 96vw);
      transform: translateX(100%);
      transition: transform 0.25s ease;
      background: #fbfef9;
      border-left: 1px solid var(--line);
      z-index: 40;
      display: grid;
      grid-template-rows: auto 1fr;
    }}
    .sidebar.open {{ transform: translateX(0); }}
    .overlay.open {{ display: block; }}
    .side-head {{ padding: 12px; border-bottom: 1px solid var(--line); display: flex; justify-content: space-between; align-items: center; gap: 8px; }}
    .side-body {{ overflow: auto; padding: 12px; display: grid; gap: 10px; }}
    .term {{ border: 1px solid #e4ecdf; border-radius: 10px; padding: 8px; background: #fff; }}
    .term b {{ display: block; margin-bottom: 4px; }}
    .mono {{ font-family: Consolas, Menlo, monospace; font-size: 0.8rem; }}
    .guide-table {{ width: 100%; min-width: 0; border-collapse: collapse; margin-top: 6px; font-size: 0.78rem; }}
    .guide-table th, .guide-table td {{ border: 1px solid #e3eadf; padding: 5px; vertical-align: top; }}
    .guide-table th {{ background: #f2f7ef; position: static; }}
    .bool-badge {{
      display: inline-block;
      min-width: 52px;
      text-align: center;
      padding: 2px 8px;
      border-radius: 999px;
      font-weight: 700;
      letter-spacing: 0.01em;
      font-size: 0.74rem;
      border: 1px solid;
    }}
    .bool-true {{ background: #e7f8ee; color: #146c43; border-color: #8fd6ac; }}
    .bool-false {{ background: #fff1ef; color: #a13a2b; border-color: #e8b2a9; }}
    .th-wrap {{ display: inline-flex; align-items: center; gap: 6px; }}
    .th-label {{
      border: 0;
      background: transparent;
      color: inherit;
      font: inherit;
      font-weight: 700;
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 0;
      margin: 0;
      cursor: pointer;
    }}
    .sort-indicator {{
      display: inline-block;
      min-width: 10px;
      color: #5a7f74;
      font-size: 0.7rem;
      line-height: 1;
      transform: translateY(-0.5px);
    }}
    .th-help {{
      display: inline-flex;
      width: 16px;
      height: 16px;
      border-radius: 999px;
      align-items: center;
      justify-content: center;
      border: 1px solid #9dc2b8;
      color: #0e6256;
      background: #eef9f5;
      font-size: 0.72rem;
      cursor: help;
      user-select: none;
    }}
    .col-tip {{
      position: fixed;
      z-index: 60;
      max-width: min(480px, 92vw);
      background: #ffffff;
      color: #163025;
      border: 1px solid #cfe2d8;
      border-radius: 10px;
      box-shadow: 0 12px 30px rgba(14, 40, 30, 0.2);
      padding: 8px 10px;
      font-size: 0.78rem;
      line-height: 1.35;
      display: none;
      pointer-events: none;
    }}
    .col-tip b {{ display: block; margin-bottom: 4px; }}
    .cell-modal {{
      position: fixed;
      inset: 0;
      background: rgba(10, 21, 17, 0.42);
      z-index: 80;
      display: none;
      align-items: center;
      justify-content: center;
      padding: 14px;
    }}
    .cell-modal.open {{ display: flex; }}
    .cell-modal-card {{
      width: min(860px, 96vw);
      max-height: 82vh;
      overflow: auto;
      border-radius: 12px;
      border: 1px solid #cfe2d8;
      background: #fff;
      box-shadow: 0 16px 38px rgba(11, 30, 23, 0.28);
      padding: 12px;
      display: grid;
      gap: 8px;
    }}
    .cell-modal-head {{ display: flex; justify-content: space-between; align-items: center; gap: 8px; }}
    .cell-modal-title {{ font-weight: 700; color: #173126; }}
    .cell-modal-text {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: Consolas, Menlo, monospace;
      font-size: 0.8rem;
      line-height: 1.4;
      color: #153126;
    }}

    @media (max-width: 980px) {{
      .top-grid {{ grid-template-columns: 1fr; }}
      .chart-grid {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 700px) {{
      .bar-row {{ grid-template-columns: 1fr; }}
      .table-wrap {{ max-height: 50vh; }}
    }}
  </style>
</head>
<body>
  <header class=\"top\">
    <div>
      <h1>{title_html}</h1>
      <div class=\"sub\">Source: {source_html}</div>
    </div>
    <button id=\"openGlossary\" class=\"help-toggle\">Glossary & Column Guide</button>
  </header>

  <main class=\"layout\">
    <section class=\"top-grid\">
      <article class=\"panel\">
        <div class=\"section-head\">
          <h2>KPIs</h2>
          <button type=\"button\" class=\"collapse-btn\" data-target=\"kpiBody\" aria-expanded=\"true\">Hide</button>
        </div>
        <div class=\"panel-body\" id=\"kpiBody\">
          <section class=\"cards\" id=\"kpis\"></section>
        </div>
      </article>
    </section>

    <section class=\"panel\">
      <div class=\"section-head\">
        <h2>Charts</h2>
        <button type=\"button\" class=\"collapse-btn\" data-target=\"chartBody\" aria-expanded=\"true\">Hide</button>
      </div>
      <div class=\"panel-body\" id=\"chartBody\">
        <section class=\"chart-grid\">
          <article class=\"panel\"><h2>Origin Class</h2><div class=\"bars\" id=\"originChart\"></div></article>
          <article class=\"panel\"><h2>Filter Status</h2><div class=\"bars\" id=\"statusChart\"></div></article>
          <article class=\"panel\"><h2>Top Assigned Flags</h2><div class=\"bars\" id=\"flagChart\"></div></article>
          <article class=\"panel\"><h2>Token Length Buckets</h2><div class=\"bars\" id=\"lenChart\"></div></article>
        </section>
      </div>
    </section>

    <section class=\"panel\">
      <h2>Filters</h2>
      <div class=\"controls\">
        <input id=\"q\" placeholder=\"Search token, source base, lineage...\" />
        <select id=\"origin\"><option value=\"\">All origin_class</option></select>
        <select id=\"filterStatus\"><option value=\"\">All filter_status</option><option value=\"__EMPTY__\">(empty)</option></select>
        <select id=\"filterMatchType\"><option value=\"\">All filter_match_type</option><option value=\"__EMPTY__\">(empty)</option></select>
        <select id=\"sourceGame\"><option value=\"\">All source games</option></select>
      </div>
      <div class=\"checks\">
        <label><input type=\"checkbox\" id=\"cCorpus\" /> corpus form only</label>
        <label><input type=\"checkbox\" id=\"cGhost\" /> ghost form only</label>
        <label><input type=\"checkbox\" id=\"cCasing\" /> casing inferred only</label>
        <label><input type=\"checkbox\" id=\"cTb\" /> in TB only</label>
      </div>
      <div class=\"tiny muted\" style=\"margin-top:8px;\">Tip: choose <b>(empty)</b> for filter_status/filter_match_type to isolate rows with no direct audit match.</div>
      <div class=\"cols\" id=\"colToggles\"></div>
    </section>

    <section class=\"panel\">
      <h2>Source Table</h2>
      <div class=\"table-wrap\">
        <table>
          <thead>
            <tr id=\"theadRow\"></tr>
          </thead>
          <tbody id=\"tbody\"></tbody>
        </table>
      </div>
      <div class=\"pager\">
        <span class=\"rows-wrap\">
          <label for=\"pageSize\" class=\"tiny muted\">Rows:</label>
          <select id=\"pageSize\"></select>
        </span>
        <button id=\"prev\">Prev</button>
        <span id=\"pageButtons\" class=\"page-buttons\"></span>
        <button id=\"next\">Next</button>
        <span id=\"pagerInfo\"></span>
      </div>
    </section>
  </main>

  <div id=\"overlay\" class=\"overlay\"></div>
  <aside id=\"sidebar\" class=\"sidebar\">
    <div class=\"side-head\">
      <strong>Glossary & Column Guide</strong>
      <button id=\"closeGlossary\">Close</button>
    </div>
    <div class=\"side-body\">
      <div class=\"term\"><b>Main Glossary</b>
        <div><span class=\"mono\">.dic</span>: Hunspell base entries, optionally with flags (example: <span class=\"mono\">camino/AB</span>).</div>
        <div><span class=\"mono\">.aff</span>: Hunspell rules that expand flags into variants.</div>
        <div><span class=\"mono\">Ghost form</span>: proper-noun fallback-derived form accepted only when corpus-attested.</div>
      </div>
      <div class=\"term\"><b>AFF Flag Examples</b>
        <div><span class=\"mono\">S</span>: plural/nominal expansion. Example: <span class=\"mono\">Goblin -&gt; Goblins</span>.</div>
        <div><span class=\"mono\">G</span>: gender/number path (ES-like). Example: <span class=\"mono\">maestro -&gt; maestra</span>.</div>
        <div><span class=\"mono\">D</span>: verb-derived path (EN/PT-like). Example: <span class=\"mono\">craft -&gt; crafted</span>.</div>
      </div>
      <div class=\"term\"><b>Column Definitions (with examples)</b>
        <div><span class=\"mono\">game</span>: source game(s) merged into row. Example: <span class=\"mono\">WAKFU</span> or <span class=\"mono\">DOFUS | WAKFU</span>.</div>
        <div><span class=\"mono\">token</span>/<span class=\"mono\">token_lower</span>: output surface and normalized key.</div>
        <div><span class=\"mono\">origin_class</span>: lineage bucket (tb_and_corpus, tb_only, corpus_only, generated_casing_inference, generated_unknown).</div>
        <div><span class=\"mono\">lineage_tags</span>: path tags like tb_filtered_base, corpus_aff_form, corpus_ghost_form, casing_inference.</div>
        <div><span class=\"mono\">source_base_words</span>: parent lemmas/base words supporting this token.</div>
        <div><span class=\"mono\">filter_status</span>: Step 2-3 audit state (removed_known_word, kept_neologism, or empty).</div>
        <div><span class=\"mono\">filter_match_type</span>: dictionary_form, compound_word, or empty.</div>
        <div><span class=\"mono\">flag_evidence_count</span>: number of detailed flag evidence records for the row.</div>
        <table class=\"guide-table\">
          <thead>
            <tr><th>Column</th><th>Meaning</th><th>Example</th></tr>
          </thead>
          <tbody>
            <tr><td><span class=\"mono\">origin_class</span></td><td>High-level lineage bucket for quick triage.</td><td><span class=\"mono\">tb_and_corpus</span></td></tr>
            <tr><td><span class=\"mono\">lineage_tags</span></td><td>Detailed path tags indicating how token entered output.</td><td><span class=\"mono\">tb_filtered_base | corpus_aff_form</span></td></tr>
            <tr><td><span class=\"mono\">assigned_flags</span></td><td>Flags used in compressed dictionary output.</td><td><span class=\"mono\">S</span></td></tr>
            <tr><td><span class=\"mono\">filter_status</span></td><td>Step 2-3 keep/remove status from filter audit.</td><td><span class=\"mono\">kept_neologism</span></td></tr>
            <tr><td><span class=\"mono\">step4_related_bases</span></td><td>Bases whose found/ghost forms include this token.</td><td><span class=\"mono\">Xelor | Xelorian</span></td></tr>
          </tbody>
        </table>
      </div>
      <div class=\"term\"><b>Interpreting Empty filter_status</b>
        <div>Empty usually means this final surface token had no direct matching audit row from Step 2-3 (common for generated/derived outputs).</div>
      </div>
      <div class=\"term\"><b>Meaning of kept_neologism</b>
        <div><span class=\"mono\">kept_neologism</span> means the token was kept after Step 2-3 because it was not recognized as an already-known dictionary form (directly or via allowed Hunspell derivations).</div>
        <table class=\"guide-table\">
          <thead>
            <tr><th>Scenario</th><th>Status</th><th>Interpretation</th></tr>
          </thead>
          <tbody>
            <tr><td>Game/domain term not covered by base dictionary</td><td><span class=\"mono\">kept_neologism</span></td><td>Candidate domain term kept for later corpus validation and packaging.</td></tr>
            <tr><td>Common dictionary word or generated known form</td><td><span class=\"mono\">removed_known_word</span></td><td>Filtered out to avoid polluting domain dictionary.</td></tr>
            <tr><td>Final derived row without direct Step 2-3 audit row</td><td><span class=\"mono\">(empty)</span></td><td>Often generated by later steps (AFF/casing/ghost), so no direct filter audit mapping.</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </aside>
  <div id=\"cellModal\" class=\"cell-modal\" aria-hidden=\"true\">
    <div class=\"cell-modal-card\" role=\"dialog\" aria-modal=\"true\" aria-label=\"Cell full content\">
      <div class=\"cell-modal-head\">
        <span class=\"cell-modal-title\">Full Cell Value</span>
        <button id=\"cellModalClose\" type=\"button\">Close</button>
      </div>
      <pre id=\"cellModalText\" class=\"cell-modal-text\"></pre>
    </div>
  </div>
  <div id=\"colTip\" class=\"col-tip\"></div>

  <script>
    const PAYLOAD = {payload_json};
    const ROWS = Array.isArray(PAYLOAD.rows) ? PAYLOAD.rows : [];
    const EMPTY_SENTINEL = '__EMPTY__';
    const DEFAULT_PAGE_SIZE = 120;
    let currentPage = 1;
    let pageSize = DEFAULT_PAGE_SIZE;
    let filteredRows = ROWS.slice();
    let sortKey = '';
    let sortDir = 'asc';

    const COLS = [
      {{ key: 'game', label: 'game', title: 'Target report game.', help: '<b>game</b>Target report game for this row.<br><br><b>Example</b><br><span class="mono">ANK</span> (for ANK superconsolidated report).' }},
      {{ key: 'language', label: 'language', title: 'Language code.', help: '<b>language</b>Normalized language code.<br><br><b>Example</b><br><span class="mono">en</span>, <span class="mono">es</span>' }},
      {{ key: 'token', label: 'token', title: 'Final token surface form.', help: '<b>token</b>Exact final token as exported.<br><br><b>Example</b><br><span class="mono">Xelors</span>' }},
      {{ key: 'token_lower', label: 'token_lower', title: 'Lowercased matching key.', help: '<b>token_lower</b>Lowercased key used for matching and deduplication.<br><br><b>Example</b><br><span class="mono">xelors</span>' }},
      {{ key: 'origin_class', label: 'origin_class', title: 'High-level lineage class.', help: '<b>origin_class</b>Lineage bucket for quick triage.<br><br><b>Values</b><br><span class="mono">tb_and_corpus</span>, <span class="mono">tb_only</span>, <span class="mono">corpus_only</span>, <span class="mono">generated_casing_inference</span>, <span class="mono">generated_unknown</span>' }},
      {{ key: 'lineage_tags', label: 'lineage_tags', title: 'Detailed lineage tags.', help: '<b>lineage_tags</b>Path tags explaining how token entered output.<br><br><b>Example</b><br><span class="mono">tb_filtered_base | corpus_aff_form</span>' }},
      {{ key: 'in_tb_tokens', label: 'in_tb_tokens', title: 'Token appears in TB token list.', help: '<b>in_tb_tokens</b><span class="mono">true</span> when token appears in Step 1 extraction.' }},
      {{ key: 'tb_key', label: 'tb_key', title: 'Original TB key(s) for kept neologism token.', help: '<b>tb_key</b>Source TB key lineage for Step 2-3 kept neologisms. Multiple keys are joined with <span class="mono">|</span>.' }},
      {{ key: 'tb_source_entity', label: 'tb_source_entity', title: 'Primary entity derived from TB key.', help: '<b>tb_source_entity</b>Primary entity extracted from the first TB key.<br><br><b>Example</b><br><span class="mono">monster:123</span>' }},
      {{ key: 'in_filtered_dic_base', label: 'in_filtered_dic_base', title: 'Token appears as filtered base.', help: '<b>in_filtered_dic_base</b><span class="mono">true</span> when token appears as filtered neologism base entry.' }},
      {{ key: 'is_corpus_form', label: 'is_corpus_form', title: 'Corpus-attested AFF-derived form.', help: '<b>is_corpus_form</b>AFF-derived form confirmed in corpus.<br><br><b>Example</b><br><span class="mono">Goblin + S -&gt; Goblins</span>' }},
      {{ key: 'is_ghost_form', label: 'is_ghost_form', title: 'Corpus-attested ghost form.', help: '<b>is_ghost_form</b>Proper-noun fallback derivation confirmed in corpus.<br><br><b>Example</b><br><span class="mono">Xelor -&gt; Xelorian</span>' }},
      {{ key: 'is_casing_inferred', label: 'is_casing_inferred', title: 'Added through casing inference.', help: '<b>is_casing_inferred</b>Casing-driven addition, not morphology.<br><br><b>Example</b><br><span class="mono">xelor -&gt; Xelor</span>' }},
      {{ key: 'source_base_words', label: 'source_base_words', title: 'Parent base words.', help: '<b>source_base_words</b>Base words contributing to this token.' }},
      {{ key: 'assigned_flags', label: 'assigned_flags', title: 'Assigned compressed flags.', help: '<b>assigned_flags</b>Flags assigned in compressed output.<br><br><b>Example</b><br><span class="mono">S</span>' }},
      {{ key: 'mandatory_flags_assigned', label: 'mandatory_flags_assigned', title: 'Mandatory policy flags.', help: '<b>mandatory_flags_assigned</b>Assigned flags coming from mandatory policy.' }},
      {{ key: 'validated_flags_assigned', label: 'validated_flags_assigned', title: 'Validated Step 4 flags.', help: '<b>validated_flags_assigned</b>Assigned flags backed by Step 4 quorum evidence.' }},
      {{ key: 'validated_flags_candidates', label: 'validated_flags_candidates', title: 'Candidate validated flags.', help: '<b>validated_flags_candidates</b>Candidate flags linked to source bases before split.' }},
      {{ key: 'flag_generated_forms', label: 'flag_generated_forms', title: 'Forms generated by flags.', help: '<b>flag_generated_forms</b>Generated forms associated with assigned flags.<br><br><b>Example</b><br><span class="mono">xelors | xelor&#39;s</span>' }},
      {{ key: 'filter_status', label: 'filter_status', title: 'Step 2-3 filter status.', help: '<b>filter_status</b>Filtering status from audit.<br><br><b>Values</b><br><span class="mono">removed_known_word</span>, <span class="mono">kept_neologism</span>, or empty.' }},
      {{ key: 'filter_match_type', label: 'filter_match_type', title: 'Step 2-3 match class.', help: '<b>filter_match_type</b>Filter match type.<br><br><b>Values</b><br><span class="mono">dictionary_form</span>, <span class="mono">compound_word</span>, or empty.' }},
      {{ key: 'dropped_generated_by', label: 'dropped_generated_by', title: 'Generated entries that subsume form.', help: '<b>dropped_generated_by</b>Flagged entries that cover this explicit form during pruning.<br><br><b>Example</b><br><span class="mono">xelor/S</span>' }},
      {{ key: 'flag_evidence_count', label: 'flag_evidence_count', title: 'Number of detailed evidence entries.', help: '<b>flag_evidence_count</b>Count of detailed <span class="mono">flag_evidence</span> entries for this row.' }},
      {{ key: 'flag_evidence_trail', label: 'flag_evidence_trail', title: 'Detailed flag evidence trail.', help: '<b>flag_evidence_trail</b>Per-flag trace including base word, hit ratio and quorum pass/fail.' }},
      {{ key: 'step4_related_bases', label: 'step4_related_bases', title: 'Related Step 4 bases.', help: '<b>step4_related_bases</b>Bases whose found/ghost forms include this token.<br><br><b>Example</b><br><span class="mono">Xelor | Xelorian</span>' }},
      {{ key: 'source_games', label: 'source_game', title: 'Source game lineage list.', help: '<b>source_game</b>Source game lineage retained in ANK merge rows.<br><br><b>Example</b><br><span class="mono">WAKFU | DOFUS</span>' }},
      {{ key: 'source_rows_merged', label: 'source_rows_merged', title: 'Merged source row count.', help: '<b>source_rows_merged</b>How many source rows merged into this output row.' }},
    ];

    const colVisible = Object.fromEntries(COLS.map(c => [c.key, c.key !== 'token_lower']));
    const $ = (id) => document.getElementById(id);

    function uniq(values) {{
      return Array.from(new Set(values.filter(Boolean))).sort((a, b) => a.localeCompare(b));
    }}
    function toList(value) {{
      if (Array.isArray(value)) return value.map(v => String(v).trim()).filter(Boolean);
      if (!value) return [];
      return String(value).split('|').map(v => v.trim()).filter(Boolean);
    }}
    function textCell(value) {{
      const text = String(value || '').trim();
      return text || '<span class="muted">(empty)</span>';
    }}
    function boolCell(value) {{
      return value
        ? '<span class="bool-badge bool-true">true</span>'
        : '<span class="bool-badge bool-false">false</span>';
    }}
    function escapeHtml(value) {{
      return String(value || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }}
    function expandableText(value, threshold = 70) {{
      const raw = String(value || '').trim();
      if (!raw) return '<span class="muted">(empty)</span>';
      const shortRaw = raw.length > threshold ? raw.slice(0, threshold) + '...' : raw;
      const shortHtml = escapeHtml(shortRaw);
      if (raw.length <= threshold) {{
        return `<span class="cell-clip" title="${{escapeHtml(raw)}}">${{shortHtml}}</span>`;
      }}
      return `
        <span class="cell-inline" title="${{escapeHtml(raw)}}">
          <span class="cell-clip">${{shortHtml}}</span>
          <button type="button" class="cell-more" data-full="${{encodeURIComponent(raw)}}">View</button>
        </span>
      `;
    }}
    function listCell(value) {{ return toList(value).map(v => `<span class="pill">${{v}}</span>`).join(''); }}
    function flagTrailCell(row) {{
      const evidence = Array.isArray(row.flag_evidence) ? row.flag_evidence : [];
      if (!evidence.length) return '<span class="muted">(empty)</span>';
      return evidence.map((ev) => {{
        const base = String(ev.base_word || '').trim() || '?';
        const flag = String(ev.flag || '').trim() || '?';
        const hits = Number(ev.hit_count || 0);
        const derived = Number(ev.derived_count || 0);
        const passed = !!ev.passed;
        const ratio = Number(ev.hit_ratio || 0);
        const ratioPct = Number.isFinite(ratio) ? `${{(ratio * 100).toFixed(1)}}%` : '0.0%';
        return `<span class="pill" title="base=${{base}} | flag=${{flag}} | passed=${{passed}}">${{base}}:${{flag}} ${{hits}}/${{derived}} (${{ratioPct}}) ${{passed ? 'OK' : 'NO'}}</span>`;
      }}).join('');
    }}

    function tbKeyCell(value) {{
      return expandableText(value, 210);
    }}

    function openCellModal(text) {{
      $('cellModalText').textContent = String(text || '');
      $('cellModal').classList.add('open');
      $('cellModal').setAttribute('aria-hidden', 'false');
    }}
    function closeCellModal() {{
      $('cellModal').classList.remove('open');
      $('cellModal').setAttribute('aria-hidden', 'true');
      $('cellModalText').textContent = '';
    }}

    function rowValue(row, key) {{
      if (key === 'flag_evidence_count') return Array.isArray(row.flag_evidence) ? row.flag_evidence.length : 0;
      if (key === 'flag_evidence_trail') {{
        const evidence = Array.isArray(row.flag_evidence) ? row.flag_evidence : [];
        return evidence.map((ev) => `${{ev.base_word || ''}}:${{ev.flag || ''}}:${{ev.hit_count || 0}}/${{ev.derived_count || 0}}`).join(' | ');
      }}
      if (key === 'game') {{
        if (toList(row.source_games).length) return 'ANK';
        return String(row.game || '').trim();
      }}
      return row[key];
    }}

    function renderHeader() {{
      $('theadRow').innerHTML = COLS.map(col => `
        <th class="col-${{col.key}}" title="${{col.title}}" data-help="${{encodeURIComponent(col.help || col.title)}}">
          <span class="th-wrap">
            <button type="button" class="th-label" data-sort-key="${{col.key}}" title="Sort by ${{col.label}}">
              <span>${{col.label}}</span><span class="sort-indicator">${{sortKey === col.key ? (sortDir === 'asc' ? '^' : 'v') : ''}}</span>
            </button>
            <span class="th-help">?</span>
          </span>
        </th>
      `).join('');
      bindHeaderSort();
      bindHeaderHelp();
      applyColumnVisibility();
    }}

    function bindHeaderSort() {{
      document.querySelectorAll('#theadRow .th-label').forEach(btn => {{
        btn.addEventListener('click', (event) => {{
          event.preventDefault();
          event.stopPropagation();
          toggleSort(btn.dataset.sortKey || '');
        }});
      }});
    }}

    function bindHeaderHelp() {{
      const tip = $('colTip');
      const placeTip = (event) => {{
        const pad = 12;
        const rect = tip.getBoundingClientRect();
        let left = event.clientX + 14;
        let top = event.clientY + 14;
        if (left + rect.width > window.innerWidth - pad) left = window.innerWidth - rect.width - pad;
        if (top + rect.height > window.innerHeight - pad) top = window.innerHeight - rect.height - pad;
        tip.style.left = `${{Math.max(pad, left)}}px`;
        tip.style.top = `${{Math.max(pad, top)}}px`;
      }};

      document.querySelectorAll('#theadRow th').forEach(th => {{
        const show = (event) => {{
          const payload = th.getAttribute('data-help') || '';
          tip.innerHTML = decodeURIComponent(payload);
          tip.style.display = 'block';
          placeTip(event);
        }};
        const move = (event) => {{ if (tip.style.display === 'block') placeTip(event); }};
        const hide = () => {{ tip.style.display = 'none'; }};
        const helpNode = th.querySelector('.th-help');
        if (helpNode) helpNode.addEventListener('click', (event) => event.stopPropagation());
        th.addEventListener('mouseenter', show);
        th.addEventListener('mousemove', move);
        th.addEventListener('mouseleave', hide);
      }});
    }}

    function normalizeSortValue(value) {{
      if (value === null || value === undefined) return '';
      if (Array.isArray(value)) return value.join(' | ').toLowerCase();
      if (typeof value === 'boolean') return value ? 1 : 0;
      if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
      return String(value).trim().toLowerCase();
    }}

    function compareRows(a, b, key, direction) {{
      const av = normalizeSortValue(rowValue(a, key));
      const bv = normalizeSortValue(rowValue(b, key));
      const aEmpty = av === '';
      const bEmpty = bv === '';
      if (aEmpty && !bEmpty) return 1;
      if (!aEmpty && bEmpty) return -1;

      let cmp = 0;
      if (typeof av === 'number' && typeof bv === 'number') {{
        cmp = av - bv;
      }} else {{
        cmp = String(av).localeCompare(String(bv), undefined, {{ numeric: true, sensitivity: 'base' }});
      }}
      return direction === 'asc' ? cmp : -cmp;
    }}

    function applySort() {{
      if (!sortKey) return;
      filteredRows.sort((a, b) => compareRows(a, b, sortKey, sortDir));
    }}

    function toggleSort(key) {{
      if (!key) return;
      if (sortKey === key) {{
        sortDir = sortDir === 'asc' ? 'desc' : 'asc';
      }} else {{
        sortKey = key;
        sortDir = 'asc';
      }}
      currentPage = 1;
      applySort();
      renderHeader();
      renderTable();
    }}

    function renderColumnToggles() {{
      $('colToggles').innerHTML = COLS.map(col => `
        <label title="Show/hide column ${{col.label}}">
          <input type="checkbox" data-col="${{col.key}}" ${{colVisible[col.key] ? 'checked' : ''}} /> ${{col.label}}
        </label>
      `).join('');
      document.querySelectorAll('#colToggles input[type="checkbox"]').forEach(cb => {{
        cb.addEventListener('change', () => {{
          colVisible[cb.dataset.col] = cb.checked;
          applyColumnVisibility();
        }});
      }});
    }}

    function pageWindow(totalPages, current) {{
      if (totalPages <= 1) return [1];
      const fixed = new Set([1, 2, totalPages - 1, totalPages]);
      for (let p = current - 3; p <= current + 3; p += 1) {{
        if (p >= 1 && p <= totalPages) fixed.add(p);
      }}
      const sorted = Array.from(fixed).filter(p => p >= 1 && p <= totalPages).sort((a, b) => a - b);
      const tokens = [];
      let prev = 0;
      for (const p of sorted) {{
        if (prev && p - prev > 1) tokens.push('...');
        tokens.push(p);
        prev = p;
      }}
      return tokens;
    }}

    function renderPageButtons(totalPages) {{
      const holder = $('pageButtons');
      if (!holder) return;
      if (totalPages <= 1) {{
        holder.innerHTML = '';
        return;
      }}
      const tokens = pageWindow(totalPages, currentPage);
      holder.innerHTML = tokens.map((token) => {{
        if (token === '...') return '<span class="ellipsis">...</span>';
        const active = token === currentPage ? 'active' : '';
        return `<button type="button" class="page-btn ${{active}}" data-page="${{token}}">${{token}}</button>`;
      }}).join('');
      holder.querySelectorAll('button[data-page]').forEach((btn) => {{
        btn.addEventListener('click', () => {{
          currentPage = Number(btn.dataset.page || '1');
          renderTable();
        }});
      }});
    }}

    function initPageSizeOptions() {{
      const select = $('pageSize');
      if (!select) return;
      const half = Math.max(1, Math.floor(DEFAULT_PAGE_SIZE / 2));
      const plus50 = Math.max(1, Math.ceil(DEFAULT_PAGE_SIZE * 1.5));
      const plus100 = Math.max(1, DEFAULT_PAGE_SIZE * 2);
      const options = [
        [half, `Half (${{half}})`],
        [DEFAULT_PAGE_SIZE, `Default (${{DEFAULT_PAGE_SIZE}})`],
        [plus50, `+50% (${{plus50}})`],
        [plus100, `+100% (${{plus100}})`],
        [0, `All (${{ROWS.length}})`],
      ];
      select.innerHTML = options.map(([value, label]) => `<option value="${{value}}">${{label}}</option>`).join('');
      select.value = String(DEFAULT_PAGE_SIZE);
      select.addEventListener('change', () => {{
        pageSize = Number(select.value || DEFAULT_PAGE_SIZE);
        currentPage = 1;
        renderTable();
      }});
    }}

    function setupCollapsibles() {{
      document.querySelectorAll('.collapse-btn[data-target]').forEach((btn) => {{
        btn.addEventListener('click', () => {{
          const targetId = btn.getAttribute('data-target');
          const body = targetId ? document.getElementById(targetId) : null;
          if (!body) return;
          const collapsed = body.classList.toggle('is-collapsed');
          btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
          btn.textContent = collapsed ? 'Show' : 'Hide';
          updateTableViewport();
        }});
      }});
      updateTableViewport();
    }}

    function updateTableViewport() {{
      const kpiCollapsed = $('kpiBody')?.classList.contains('is-collapsed');
      const chartCollapsed = $('chartBody')?.classList.contains('is-collapsed');
      document.body.classList.toggle('tight-table', !!(kpiCollapsed || chartCollapsed));
      document.body.classList.toggle('ultra-tight-table', !!(kpiCollapsed && chartCollapsed));
    }}

    function applyColumnVisibility() {{
      COLS.forEach(col => {{
        const show = !!colVisible[col.key];
        document.querySelectorAll(`.col-${{col.key}}`).forEach(el => {{
          el.style.display = show ? '' : 'none';
        }});
      }});
    }}

    function renderKpis(rows) {{
      const distinctLower = new Set(rows.map(r => String(r.token_lower || '').toLowerCase()).filter(Boolean)).size;
      const corpus = rows.filter(r => r.is_corpus_form).length;
      const ghost = rows.filter(r => r.is_ghost_form).length;
      const casing = rows.filter(r => r.is_casing_inferred).length;
      const tb = rows.filter(r => r.in_tb_tokens).length;
      const base = rows.filter(r => r.in_filtered_dic_base).length;
      const cards = [
        ['Visible Rows', rows.length],
        ['Distinct token_lower', distinctLower],
        ['Corpus forms', corpus],
        ['Ghost forms', ghost],
        ['Casing inferred', casing],
        ['In TB', tb],
        ['In filtered dic base', base],
        ['Source format', PAYLOAD.source_format || 'unknown'],
      ];
      $('kpis').innerHTML = cards.map(([label, value]) => `<div class="card"><div class="k">${{value}}</div><div class="l">${{label}}</div></div>`).join('');
    }}

    function countsFromRows(rows, keyFn, limit = 12) {{
      const m = new Map();
      for (const row of rows) {{
        for (const key of keyFn(row)) {{
          if (!key) continue;
          m.set(key, (m.get(key) || 0) + 1);
        }}
      }}
      return Array.from(m.entries()).sort((a, b) => b[1] - a[1]).slice(0, limit);
    }}

    function renderBars(containerId, entries) {{
      const container = $(containerId);
      if (!entries.length) {{
        container.innerHTML = '<div class="count">No data</div>';
        return;
      }}
      const max = Math.max(...entries.map(e => e[1]));
      container.innerHTML = entries.map(([label, count]) => `
        <div class="bar-row">
          <div class="bar-label" title="${{label}}">${{label}}</div>
          <div class="bar-wrap"><div class="bar-fill" style="width:${{Math.max(2, Math.round((count/max)*100))}}%"></div></div>
          <div class="count">${{count}}</div>
        </div>`).join('');
    }}

    function initFilters() {{
      const origins = uniq(ROWS.map(r => String(r.origin_class || '').trim()));
      const statuses = uniq(ROWS.map(r => String(r.filter_status || '').trim()));
      const matchTypes = uniq(ROWS.map(r => String(r.filter_match_type || '').trim()));
      const sourceGames = uniq(ROWS.flatMap(r => toList(r.source_games)));
      for (const value of origins) $('origin').insertAdjacentHTML('beforeend', `<option value="${{value}}">${{value}}</option>`);
      for (const value of statuses) $('filterStatus').insertAdjacentHTML('beforeend', `<option value="${{value}}">${{value}}</option>`);
      for (const value of matchTypes) $('filterMatchType').insertAdjacentHTML('beforeend', `<option value="${{value}}">${{value}}</option>`);
      for (const value of sourceGames) $('sourceGame').insertAdjacentHTML('beforeend', `<option value="${{value}}">${{value}}</option>`);
    }}

    function rowMatches(row) {{
      const q = $('q').value.trim().toLowerCase();
      const origin = $('origin').value;
      const status = $('filterStatus').value;
      const matchType = $('filterMatchType').value;
      const sourceGame = $('sourceGame').value;
      const rowStatus = String(row.filter_status || '').trim();
      const rowMatchType = String(row.filter_match_type || '').trim();

      if (origin && String(row.origin_class || '') !== origin) return false;
      if (status === EMPTY_SENTINEL && rowStatus !== '') return false;
      if (status && status !== EMPTY_SENTINEL && rowStatus !== status) return false;
      if (matchType === EMPTY_SENTINEL && rowMatchType !== '') return false;
      if (matchType && matchType !== EMPTY_SENTINEL && rowMatchType !== matchType) return false;
      if (sourceGame && !toList(row.source_games).includes(sourceGame)) return false;
      if ($('cCorpus').checked && !row.is_corpus_form) return false;
      if ($('cGhost').checked && !row.is_ghost_form) return false;
      if ($('cCasing').checked && !row.is_casing_inferred) return false;
      if ($('cTb').checked && !row.in_tb_tokens) return false;

      if (q) {{
        const hay = [
          row.token, row.game, row.language, row.token_lower, row.origin_class,
          row.filter_status, row.filter_match_type,
          row.tb_key, row.tb_source_entity,
          ...toList(row.lineage_tags),
          ...toList(row.source_base_words),
          ...toList(row.assigned_flags),
          ...toList(row.source_games),
        ].join(' ').toLowerCase();
        if (!hay.includes(q)) return false;
      }}
      return true;
    }}

    function renderTable() {{
      const effectivePageSize = pageSize === 0 ? Math.max(1, filteredRows.length) : Math.max(1, pageSize);
      const totalPages = Math.max(1, Math.ceil(filteredRows.length / effectivePageSize));
      currentPage = Math.min(currentPage, totalPages);
      const start = (currentPage - 1) * effectivePageSize;
      const pageRows = filteredRows.slice(start, start + effectivePageSize);

      const listKeys = new Set(['lineage_tags','source_base_words','assigned_flags','mandatory_flags_assigned','validated_flags_assigned','validated_flags_candidates','flag_generated_forms','dropped_generated_by','step4_related_bases','source_games']);
      const boolKeys = new Set(['in_tb_tokens','in_filtered_dic_base','is_corpus_form','is_ghost_form','is_casing_inferred']);
      const specialKeys = new Set(['flag_evidence_trail']);

      $('tbody').innerHTML = pageRows.map(row => {{
        const cells = COLS.map(col => {{
          const key = col.key;
          const value = rowValue(row, key);
          let cell = '';
          if (specialKeys.has(key)) {{
            cell = flagTrailCell(row);
          }} else if (key === 'tb_key') {{
            cell = tbKeyCell(value);
          }} else if (listKeys.has(key)) {{
            cell = listCell(value);
          }} else if (boolKeys.has(key)) {{
            cell = boolCell(value);
          }} else {{
            cell = textCell(value);
          }}
          return `<td class="col-${{key}}">${{cell}}</td>`;
        }}).join('');
        return `<tr>${{cells}}</tr>`;
      }}).join('');

      $('pagerInfo').textContent = `Page ${{currentPage}}/${{totalPages}} • rows ${{filteredRows.length}}`;
      $('prev').disabled = currentPage <= 1;
      $('next').disabled = currentPage >= totalPages;
      renderPageButtons(totalPages);
      applyColumnVisibility();
    }}

    function refresh() {{
      filteredRows = ROWS.filter(rowMatches);
      applySort();
      currentPage = 1;
      renderKpis(filteredRows);
      renderBars('originChart', countsFromRows(filteredRows, r => [String(r.origin_class || '').trim()], 12));
      renderBars('statusChart', countsFromRows(filteredRows, r => [String(r.filter_status || '').trim() || '(empty)'], 12));
      renderBars('flagChart', countsFromRows(filteredRows, r => toList(r.assigned_flags), 12));
      renderBars('lenChart', countsFromRows(filteredRows, r => {{
        const len = String(r.token || '').length;
        if (len <= 4) return ['1-4'];
        if (len <= 8) return ['5-8'];
        if (len <= 12) return ['9-12'];
        return ['13+'];
      }}, 4));
      renderTable();
    }}

    for (const id of ['q','origin','filterStatus','filterMatchType','sourceGame','cCorpus','cGhost','cCasing','cTb']) {{
      $(id).addEventListener('input', refresh);
      $(id).addEventListener('change', refresh);
    }}

    $('prev').addEventListener('click', () => {{ if (currentPage > 1) currentPage -= 1; renderTable(); }});
    $('next').addEventListener('click', () => {{ const effectivePageSize = pageSize === 0 ? Math.max(1, filteredRows.length) : Math.max(1, pageSize); const totalPages = Math.max(1, Math.ceil(filteredRows.length / effectivePageSize)); if (currentPage < totalPages) currentPage += 1; renderTable(); }});
    $('tbody').addEventListener('click', (event) => {{
      const btn = event.target.closest('.cell-more');
      if (!btn) return;
      const encoded = btn.getAttribute('data-full') || '';
      openCellModal(decodeURIComponent(encoded));
    }});
    $('cellModalClose').addEventListener('click', closeCellModal);
    $('cellModal').addEventListener('click', (event) => {{
      if (event.target.id === 'cellModal') closeCellModal();
    }});
    document.addEventListener('keydown', (event) => {{
      if (event.key === 'Escape') closeCellModal();
    }});

    const open = () => {{ $('sidebar').classList.add('open'); $('overlay').classList.add('open'); }};
    const close = () => {{ $('sidebar').classList.remove('open'); $('overlay').classList.remove('open'); }};
    $('openGlossary').addEventListener('click', open);
    $('closeGlossary').addEventListener('click', close);
    $('overlay').addEventListener('click', close);

    renderHeader();
    renderColumnToggles();
    initFilters();
    initPageSizeOptions();
    setupCollapsibles();
    refresh();
  </script>
</body>
</html>
"""
