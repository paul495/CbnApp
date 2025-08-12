// server.cjs  (CommonJS)
const express = require("express");
const cors = require("cors");
const Database = require("better-sqlite3");
const fs = require("fs");
const path = require("path");

console.log("[boot] starting server.cjs");

const app = express();
app.use(cors());

const DATA_DIR = process.env.DB_DIR || "./data";
const SEED_DIR = "./seed";
fs.mkdirSync(DATA_DIR, { recursive: true });

const OVERWRITE = process.env.SEED_OVERWRITE === "1";

// seed copy on first boot (or overwrite if flag is set)
for (const f of ["CBNYT_sql.db", "ENZ_sql.db"]) {
  const src = path.join(SEED_DIR, f);
  const dst = path.join(DATA_DIR, f);
  console.log("[boot] seed check", {
    src, dst, srcExists: fs.existsSync(src), dstExists: fs.existsSync(dst), overwrite: OVERWRITE
  });
  if ((!fs.existsSync(dst) || OVERWRITE) && fs.existsSync(src)) {
    fs.copyFileSync(src, dst);
    console.log("[boot] seeded", f, OVERWRITE ? "(overwrite)" : "");
  }
}


// open DBs
let cbn, enz;
try {
  cbn = new Database(path.join(DATA_DIR, "CBNYT_sql.db"), { readonly: true });
  console.log("[boot] opened CBNYT_sql.db");
} catch (e) { console.error("[boot] failed to open CBNYT_sql.db:", e); }

try {
  enz = new Database(path.join(DATA_DIR, "ENZ_sql.db"), { readonly: true });
  console.log("[boot] opened ENZ_sql.db");
} catch (e) { console.error("[boot] failed to open ENZ_sql.db:", e); }

// helpers
const paginated = (req) => {
  const page = Math.max(1, Number(req.query.page || 1));
  const limit = Math.min(100, Number(req.query.limit || 20));
  return { limit, offset: (page - 1) * limit };
};

// health
app.get("/health", (_req, res) => res.json({ ok: true }));

// filters (from CBNYT)
// /filters with dependent narrowing
app.get("/filters", (req, res) => {
  try {
    const { Ministry_Category, category, language, state } = req.query;
    const cat = (Ministry_Category ?? category ?? "Choirs in concert").toString();

    // LANGUAGES — always returned
    const langs = cbn.prepare(`
      SELECT DISTINCT UPPER(TRIM(Segment_Language)) AS v
      FROM YT_tbl
      WHERE Ministry_Category = @cat
        AND Segment_Language IS NOT NULL AND TRIM(Segment_Language) <> ''
        ${state ? "AND TRIM(Church_State) = TRIM(@state)" : ""}
      ORDER BY v
    `).all({ cat, state }).map(r => r.v);

    // If Choirs, also return states & churches (narrowed by language/state)
    let states = [], churches = [];
    if (cat === "Choirs in concert") {
      states = cbn.prepare(`
        SELECT DISTINCT TRIM(Church_State) AS v
        FROM YT_tbl
        WHERE Ministry_Category = @cat
          AND Church_State IS NOT NULL AND TRIM(Church_State) <> ''
          ${language ? "AND UPPER(TRIM(Segment_Language)) = UPPER(TRIM(@language))" : ""}
        ORDER BY v
      `).all({ cat, language }).map(r => r.v);

      churches = cbn.prepare(`
        SELECT DISTINCT TRIM(Church_Name) AS v
        FROM YT_tbl
        WHERE Ministry_Category = @cat
          AND Church_Name IS NOT NULL AND TRIM(Church_Name) <> ''
          ${language ? "AND UPPER(TRIM(Segment_Language)) = UPPER(TRIM(@language))" : ""}
          ${state ? "AND TRIM(Church_State) = TRIM(@state)" : ""}
        ORDER BY v
      `).all({ cat, language, state }).map(r => r.v);
    }

    res.set("Cache-Control", "no-store");
    res.json({ languages: langs, states, churches });
  } catch (e) {
    console.error("[/filters] error:", e);
    res.status(500).json({ error: String(e) });
  }
});


// videos (CBNYT) — supports ?Ministry_Category=… or ?category=…
app.get("/videos", (req, res) => {
  try {
    const { Ministry_Category, category, language, church, state, q } = req.query;
    const pickedCategory = Ministry_Category ?? category;
    const { limit, offset } = paginated(req);

    const where = [];
    const params = { limit, offset };

    if (pickedCategory) { where.push(`Ministry_Category = @category`); params.category = pickedCategory; }
    if (language)      { where.push(`UPPER(TRIM(Segment_Language)) = UPPER(TRIM(@language))`); params.language = String(language); }
    if (state)         { where.push(`TRIM(Church_State) = TRIM(@state)`);                      params.state    = String(state); }
    if (church)        { where.push(`TRIM(Church_Name) = TRIM(@church)`);                      params.church   = String(church); }
    if (q)             { where.push(`(Video_Title LIKE @q OR Church_Name LIKE @q)`);           params.q        = `%${q}%`; }

    const sql = `
      SELECT rowid as id,
             Video_Title         as title,
             Segment_Language    as language,
             Ministry_Category   as category,
             Church_Name         as churchName,
             Church_State        as churchState,
             Youtube_Links       as youtubeUrl,
             Upload_Date         as uploadDate
      FROM YT_tbl
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY Upload_Date DESC
      LIMIT @limit OFFSET @offset`;

    const rows = cbn.prepare(sql).all(params);
    res.set("Cache-Control", "no-store");
    res.json(rows);
  } catch (e) {
    console.error("[/videos] error:", e);
    res.status(500).json({ error: String(e) });
  }
});

// ENZ dates
app.get("/enz/dates", (_req, res) => {
  try {
    const rows = enz.prepare(`
      SELECT DISTINCT date(Telecast_date) AS d
      FROM ENZ_EPS
      ORDER BY d DESC
    `).all();
    res.json(rows.map(r => r.d));
  } catch (e) {
    console.error("[/enz/dates] error:", e);
    res.status(500).json({ error: String(e) });
  }
});

// ENZ by date
// Years that have shows
app.get("/enz/years", (_req, res) => {
  try {
    const years = enz.prepare(`
      SELECT DISTINCT strftime('%Y', Telecast_date) AS y
      FROM ENZ_EPS
      WHERE Telecast_date IS NOT NULL
      ORDER BY y DESC
    `).all().map(r => r.y);
    res.set("Cache-Control", "no-store");
    res.json({ years });
  } catch (e) { res.status(500).json({ error: String(e) }); }
});

// Months available within a year
app.get("/enz/months", (req, res) => {
  try {
    const { year } = req.query;
    const months = enz.prepare(`
      SELECT DISTINCT strftime('%m', Telecast_date) AS m
      FROM ENZ_EPS
      WHERE Telecast_date IS NOT NULL
        AND strftime('%Y', Telecast_date) = @year
      ORDER BY m DESC
    `).all({ year: String(year) }).map(r => r.m);
    res.set("Cache-Control", "no-store");
    res.json({ months });
  } catch (e) { res.status(500).json({ error: String(e) }); }
});

// Episodes filtered by year & month (both optional)
app.get("/enz", (req, res) => {
  try {
    const { year, month } = req.query;
    const { limit, offset } = paginated(req);
    const where = ["Telecast_date IS NOT NULL"];
    const p = { limit, offset };

    if (year)  { where.push(`strftime('%Y', Telecast_date) = @year`);  p.year  = String(year); }
    if (month) { where.push(`strftime('%m', Telecast_date) = @month`); p.month = String(month); }

    const rows = enz.prepare(`
      SELECT rowid as id, Video_Title as title, Upload_Date as uploadDate,
             Telecast_date as telecastDate, Youtube_Links as youtubeUrl, ESS_CODE
      FROM ENZ_EPS
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY Telecast_date DESC
      LIMIT @limit OFFSET @offset
    `).all(p);

    res.set("Cache-Control", "no-store");
    res.json(rows);
  } catch (e) { res.status(500).json({ error: String(e) }); }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`[boot] API running on ${PORT}`));

