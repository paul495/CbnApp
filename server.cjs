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

// seed copy on first boot
for (const f of ["CBNYT_sql.db", "ENZ_sql.db"]) {
  const src = path.join(SEED_DIR, f);
  const dst = path.join(DATA_DIR, f);
  console.log("[boot] seed check", {
    src, dst, srcExists: fs.existsSync(src), dstExists: fs.existsSync(dst)
  });
  if (!fs.existsSync(dst) && fs.existsSync(src)) {
    fs.copyFileSync(src, dst);
    console.log("[boot] seeded", f);
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
app.get("/filters", (_req, res) => {
  try {
    const langs = cbn.prepare(`
      SELECT DISTINCT Segment_Language AS v
      FROM YT_tbl
      WHERE Segment_Language IS NOT NULL AND Segment_Language <> ''
      ORDER BY 1
    `).all().map(r => r.v);

    const churches = cbn.prepare(`
      SELECT DISTINCT Church_Name AS v
      FROM YT_tbl
      WHERE Church_Name IS NOT NULL AND Church_Name <> ''
      ORDER BY 1
    `).all().map(r => r.v);

    res.json({ languages: langs, churches });
  } catch (e) {
    console.error("[/filters] error:", e);
    res.status(500).json({ error: String(e) });
  }
});

// videos (CBNYT) — supports ?Ministry_Category=… or ?category=…
app.get("/videos", (req, res) => {
  try {
    const { Ministry_Category, category, language, church, q } = req.query;
    const pickedCategory = Ministry_Category ?? category;
    const { limit, offset } = paginated(req);

    const where = [];
    const params = { limit, offset };

    if (pickedCategory) { where.push(`Ministry_Category = @category`); params.category = pickedCategory; }
    if (language)      { where.push(`Segment_Language = @language`);  params.language = language; }
    if (church)        { where.push(`Church_Name = @church`);         params.church = church; }
    if (q)             { where.push(`(Video_Title LIKE @q OR Church_Name LIKE @q)`); params.q = `%${q}%`; }

    const sql = `
      SELECT rowid as id,
             Video_Title as title,
             Segment_Language as language,
             Ministry_Category as category,
             Church_Name as churchName,
             Youtube_Links as youtubeUrl,
             Upload_Date as uploadDate
      FROM YT_tbl
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY Upload_Date DESC
      LIMIT @limit OFFSET @offset`;

    const rows = cbn.prepare(sql).all(params);
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
app.get("/enz", (req, res) => {
  try {
    const { date } = req.query;
    const { limit, offset } = paginated(req);

    const where = [];
    const params = { limit, offset };
    if (date) { where.push(`date(Telecast_date) = date(@date)`); params.date = date; }

    const sql = `
      SELECT rowid as id,
             Video_Title as title,
             Youtube_Links as youtubeUrl,
             Telecast_date as telecastDate,
             Upload_Date as uploadDate
      FROM ENZ_EPS
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY Telecast_date DESC
      LIMIT @limit OFFSET @offset`;

    const rows = enz.prepare(sql).all(params);
    res.json(rows);
  } catch (e) {
    console.error("[/enz] error:", e);
    res.status(500).json({ error: String(e) });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`[boot] API running on ${PORT}`));

