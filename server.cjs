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
// /filters?Ministry_Category=Choirs%20in%20concert&language=Hindi&state=Punjab
app.get("/filters", (req, res) => {
  try {
    const { Ministry_Category, language, state } = req.query;

    const W = [];
    const P = {};
    if (Ministry_Category) { W.push("Ministry_Category = @cat"); P.cat = Ministry_Category; }
    if (language)          { W.push("Segment_Language = @lang"); P.lang = language; }
    if (state)             { W.push("Church_State = @st");        P.st = state; }

    const baseWhere = W.length ? `WHERE ${W.join(" AND ")}` : "";

    const langs = cbn.prepare(`
      SELECT DISTINCT Segment_Language AS v
      FROM YT_tbl
      ${Ministry_Category ? "WHERE Ministry_Category = @cat" : ""}
      ORDER BY 1
    `).all({ cat: Ministry_Category }).map(r => r.v);

    const states = cbn.prepare(`
      SELECT DISTINCT Church_State AS v
      FROM YT_tbl
      ${[
          Ministry_Category ? "Ministry_Category = @cat" : null,
          language ? "Segment_Language = @lang" : null,
        ].filter(Boolean).length
          ? "WHERE " + [ Ministry_Category ? "Ministry_Category = @cat" : null,
                         language ? "Segment_Language = @lang" : null]
              .filter(Boolean).join(" AND ")
          : ""
      }
      AND Church_State IS NOT NULL AND TRIM(Church_State) <> ''
      ORDER BY 1
    `).all({ cat: Ministry_Category, lang: language }).map(r => r.v);

    const churches = cbn.prepare(`
      SELECT DISTINCT Church_Name AS v
      FROM YT_tbl
      ${baseWhere}
      AND Church_Name IS NOT NULL AND TRIM(Church_Name) <> ''
      ORDER BY 1
    `).all(P).map(r => r.v);

    res.json({ languages: langs, states, churches });
  } catch (e) {
    console.error("[/filters]", e);
    res.status(500).json({ error: String(e) });
  }
});


// ------------------- videos (CBNYT) -------------------
// videos (CBNYT)
app.get("/videos", (req, res) => {
  try {
    const { Ministry_Category, category, language, church, state, q } = req.query;
    const pickedCategory = Ministry_Category ?? category;
    const { limit, offset } = paginated(req);

    const where = [];
    const params = { limit, offset };

    if (pickedCategory) { where.push(`Ministry_Category = @category`); params.category = pickedCategory; }
    if (language)      { where.push(`Segment_Language = @language`);  params.language = language; }
    if (state)         { where.push(`Church_State = @state`);          params.state = state; }
    if (church)        { where.push(`Church_Name = @church`);          params.church = church; }
    if (q)             { where.push(`(Video_Title LIKE @q OR Church_Name LIKE @q)`); params.q = `%${q}%`; }

    const sql = `
      SELECT rowid as id, Video_Title as title,
             Segment_Language as language, Ministry_Category as category,
             Church_Name as churchName, Youtube_Links as youtubeUrl,
             Upload_Date as uploadDate
      FROM YT_tbl
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY Upload_Date DESC
      LIMIT @limit OFFSET @offset`;

    res.json(cbn.prepare(sql).all(params));
  } catch (e) {
    console.error("[/videos] error:", e);
    res.status(500).json({ error: String(e) });
  }
});
// Distinct states for choirs, optionally narrowed by language
app.get("/filters/states", (req, res) => {
  try {
    const { Ministry_Category, language } = req.query;
    const where = ["Church_State IS NOT NULL AND TRIM(Church_State) <> ''"];
    const p = {};

    if (Ministry_Category) { where.push("Ministry_Category = @cat"); p.cat = Ministry_Category; }
    if (language)          { where.push("Segment_Language  = @lang"); p.lang = language; }

    const rows = cbn.prepare(`
      SELECT DISTINCT Church_State AS v
      FROM YT_tbl
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY 1
    `).all(p);

    res.json({ states: rows.map(r => r.v) });
  } catch (e) {
    console.error("[/filters/states] error:", e);
    res.status(500).json({ error: String(e) });
  }
});

// Distinct churches for choirs, narrowed by language + state
app.get("/filters/churches", (req, res) => {
  try {
    const { Ministry_Category, language, state } = req.query;
    const where = ["Church_Name IS NOT NULL AND TRIM(Church_Name) <> ''"];
    const p = {};

    if (Ministry_Category) { where.push("Ministry_Category = @cat"); p.cat = Ministry_Category; }
    if (language)          { where.push("Segment_Language  = @lang"); p.lang = language; }
    if (state)             { where.push("Church_State       = @state"); p.state = state; }

    const rows = cbn.prepare(`
      SELECT DISTINCT Church_Name AS v
      FROM YT_tbl
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY 1
    `).all(p);

    res.json({ churches: rows.map(r => r.v) });
  } catch (e) {
    console.error("[/filters/churches] error:", e);
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
// Months (MM) within a year that have at least one playable episode
// Add ?names=1 to get label+value objects.
app.get("/enz/months", (req, res) => {
  try {
    const { year, names } = req.query;
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

    const months = rows.map(r => r.m).filter(Boolean);

    res.set("Cache-Control", "no-store");

    // Backward compatible (numbers) if no flag is set:
    if (names === '1') {
      // return objects with value + human name, newest-first
      return res.json({
        months: months.map(mm => ({ value: mm, name: monthName(mm) }))
      });
    }
    // old behavior
    res.json({ months });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});


// 3) Start server (KEEP LAST)
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`[boot] API running on ${PORT}`));
