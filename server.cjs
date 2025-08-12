// server.cjs — Node/Express + better-sqlite3 API

const express = require("express");
const cors = require("cors");
const Database = require("better-sqlite3");
const fs = require("fs");
const path = require("path");

// ------------------- app bootstrap -------------------
console.log("[boot] starting server.cjs");

const app = express();
app.use(cors());

function paginated(req) {
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit ?? "50", 10)));
  const offset = Math.max(0, parseInt(req.query.offset ?? "0", 10));
  return { limit, offset };
}

const DATA_DIR = process.env.DB_DIR || "./data";
const SEED_DIR = "./seed";
const OVERWRITE = process.env.SEED_OVERWRITE === "1";

fs.mkdirSync(DATA_DIR, { recursive: true });

// seed copy
for (const f of ["CBNYT_sql.db", "ENZ_sql.db"]) {
  const src = path.join(SEED_DIR, f);
  const dst = path.join(DATA_DIR, f);
  const seedInfo = {
    src,
    dst,
    srcExists: fs.existsSync(src),
    dstExists: fs.existsSync(dst),
    overwrite: OVERWRITE
  };
  console.log("[boot] seed check", seedInfo);
  if ((OVERWRITE || !fs.existsSync(dst)) && fs.existsSync(src)) {
    fs.copyFileSync(src, dst);
    console.log("[boot] seeded", f, OVERWRITE ? "(overwrite)" : "");
  }
}

// open DBs
const cbn = new Database(path.join(DATA_DIR, "CBNYT_sql.db"));
console.log("[boot] opened CBNYT_sql.db");
const enz = new Database(path.join(DATA_DIR, "ENZ_sql.db"));
console.log("[boot] opened ENZ_sql.db");

// ------------------- health -------------------
app.get("/healthz", (_req, res) => res.json({ ok: true }));

// ------------------- filters (CBNYT) -------------------
// Category-aware: returns languages for any category.
// If category is “Choirs in concert”, also returns states and churches.
app.get("/filters", (req, res) => {
  try {
    const { Ministry_Category, category, language, state } = req.query;
    const cat = (Ministry_Category ?? category ?? "Choirs in concert").toString();

    const langs = cbn.prepare(`
      SELECT DISTINCT UPPER(TRIM(Segment_Language)) AS v
      FROM YT_tbl
      WHERE Ministry_Category = @cat
        AND Segment_Language IS NOT NULL AND TRIM(Segment_Language) <> ''
        ${state ? "AND TRIM(Church_State) = TRIM(@state)" : ""}
      ORDER BY v
    `).all({ cat, state }).map(r => r.v);

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

// ------------------- videos (CBNYT) -------------------
app.get("/videos", (req, res) => {
  try {
    const { Ministry_Category, category, language, church, q, state } = req.query;
    const pickedCategory = Ministry_Category ?? category;
    const { limit, offset } = paginated(req);

    const where = [];
    const params = { limit, offset };

    if (pickedCategory) { where.push(`Ministry_Category = @category`); params.category = pickedCategory; }
    if (language)      { where.push(`UPPER(TRIM(Segment_Language)) = UPPER(TRIM(@language))`);  params.language = language; }
    if (church)        { where.push(`TRIM(Church_Name) = TRIM(@church)`);                       params.church = church; }
    if (state)         { where.push(`TRIM(Church_State) = TRIM(@state)`);                       params.state = state; }
    if (q)             { where.push(`(Video_Title LIKE @q OR Church_Name LIKE @q)`);            params.q = `%${q}%`; }

    const sql = `
      SELECT rowid as id,
             Video_Title as title,
             Segment_Language as language,
             Ministry_Category as category,
             Church_Name as churchName,
             Church_State as churchState,
             Youtube_Links as youtubeUrl,
             Upload_Date as uploadDate
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

// =================== ENZ SHOWS ===================

app.get("/enz", (req, res) => {
  try {
    const { year, month } = req.query;
    const { limit, offset } = paginated(req);

    const where = [
      "Telecast_date IS NOT NULL",
      "TRIM(Telecast_date) <> ''",
      "Youtube_Links IS NOT NULL",
      "TRIM(Youtube_Links) <> ''"
      // If you also want to require a title, UNCOMMENT:
      // ,"Video_Title IS NOT NULL",
      // "TRIM(Video_Title) <> ''"
    ];
    const p = { limit, offset };
    if (year)  { where.push("strftime('%Y', Telecast_date) = @year");  p.year  = String(year); }
    if (month) { where.push("strftime('%m', Telecast_date) = @month"); p.month = String(month); }

    const rows = enz.prepare(`
      SELECT rowid AS id,
             Video_Title    AS title,
             Upload_Date    AS uploadDate,
             Telecast_date  AS telecastDate,
             Youtube_Links  AS youtubeUrl,
             ESS_CODE
      FROM ENZ_EPS
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY Telecast_date DESC
      LIMIT @limit OFFSET @offset
    `).all(p);

    res.set("Cache-Control", "no-store");
    res.json(rows);
  } catch (e) { res.status(500).json({ error: String(e) }); }
});




// Years that have at least one episode with a YouTube link
app.get("/enz/years", (_req, res) => {
  try {
    const rows = enz.prepare(`
      SELECT strftime('%Y', Telecast_date) AS y
      FROM ENZ_EPS
      WHERE Telecast_date IS NOT NULL AND TRIM(Telecast_date) <> ''
        AND Youtube_Links IS NOT NULL AND TRIM(Youtube_Links) <> ''
      GROUP BY y
      HAVING COUNT(*) > 0
      ORDER BY y DESC
    `).all();
    res.set("Cache-Control", "no-store");
    res.json({ years: rows.map(r => r.y).filter(Boolean) });
  } catch (e) { res.status(500).json({ error: String(e) }); }
});


// Months (MM) within a year that have at least one playable episode
app.get("/enz/months", (req, res) => {
  try {
    const { year } = req.query;
    if (!year) return res.status(400).json({ error: "year is required" });

    const rows = enz.prepare(`
      SELECT strftime('%m', Telecast_date) AS m
      FROM ENZ_EPS
      WHERE Telecast_date IS NOT NULL AND TRIM(Telecast_date) <> ''
        AND strftime('%Y', Telecast_date) = @year
        AND Youtube_Links IS NOT NULL AND TRIM(Youtube_Links) <> ''
      GROUP BY m
      HAVING COUNT(*) > 0
      ORDER BY m DESC
    `).all({ year: String(year) });

    res.set("Cache-Control", "no-store");
    res.json({ months: rows.map(r => r.m).filter(Boolean) });
  } catch (e) { res.status(500).json({ error: String(e) }); }
});

// 3) Start server (KEEP LAST)
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`[boot] API running on ${PORT}`));
