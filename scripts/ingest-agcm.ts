/**
 * Ingestion crawler for the AGCM (Autorità Garante della Concorrenza e del Mercato) MCP server.
 *
 * Scrapes competition decisions, merger-control decisions, and sector data
 * from agcm.it and populates the SQLite database.
 *
 * Data sources:
 *   - Bollettino settimanale listing pages (weekly bulletins with provvedimenti)
 *   - Individual bulletin detail pages with decision text
 *   - Competition decisions (intese, abuso di posizione dominante)
 *   - Merger decisions (concentrazioni)
 *   - Press releases (comunicati stampa)
 *
 * AGCM case number patterns:
 *   A###  — abuse of dominance (abuso di posizione dominante)
 *   I###  — cartels / restrictive agreements (intese)
 *   C###  — merger control (concentrazioni)
 *   IC### — combined investigation
 *   PS### — consumer protection (excluded — not competition)
 *
 * Usage:
 *   npx tsx scripts/ingest-agcm.ts                 # full crawl
 *   npx tsx scripts/ingest-agcm.ts --resume        # skip already-ingested case numbers
 *   npx tsx scripts/ingest-agcm.ts --dry-run       # parse but do not write to DB
 *   npx tsx scripts/ingest-agcm.ts --force         # delete DB and start fresh
 *   npx tsx scripts/ingest-agcm.ts --max-pages 5   # limit bulletin listing pages (for testing)
 *   npx tsx scripts/ingest-agcm.ts --mergers-only  # only ingest merger decisions
 *   npx tsx scripts/ingest-agcm.ts --decisions-only # only ingest competition decisions
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";
import * as cheerio from "cheerio";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const BASE_URL = "https://www.agcm.it";
const BOLLETTINO_INDEX_PATH = "/pubblicazioni/bollettino-settimanale/";
const DECISIONS_LIST_PATH = "/competenze/tutela-della-concorrenza/delibere/provvedimenti";
const MERGERS_LIST_PATH = "/competenze/tutela-della-concorrenza/operazioni-di-concentrazione/lista-concentrazioni/";
const PRESS_RELEASE_PATH = "/media/comunicati-stampa";

const DB_PATH = process.env["AGCM_DB_PATH"] ?? "data/agcm.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");

const RATE_LIMIT_MS = 1_500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 3_000;
const REQUEST_TIMEOUT_MS = 30_000;
const USER_AGENT =
  "AnsvarAGCMCrawler/1.0 (+https://github.com/Ansvar-Systems/italian-competition-mcp; competition-law-research)";

/**
 * Year range for Bollettino PDF crawling.
 * AGCM has published bulletins since 1991 (Anno I).
 * Start from 2010 for a practical initial crawl.
 */
const BOLLETTINO_START_YEAR = 2010;
const BOLLETTINO_END_YEAR = new Date().getFullYear();

/**
 * Maximum bulletins per year. The AGCM publishes weekly, so roughly 50-52
 * per year. Use 53 as an upper bound to catch edge cases.
 */
const MAX_BULLETINS_PER_YEAR = 53;

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const FLAG_RESUME = args.includes("--resume");
const FLAG_DRY_RUN = args.includes("--dry-run");
const FLAG_FORCE = args.includes("--force");
const FLAG_MERGERS_ONLY = args.includes("--mergers-only");
const FLAG_DECISIONS_ONLY = args.includes("--decisions-only");

function getFlagValue(flag: string): string | undefined {
  const idx = args.indexOf(flag);
  if (idx === -1 || idx + 1 >= args.length) return undefined;
  return args[idx + 1];
}

const MAX_PAGES = getFlagValue("--max-pages")
  ? parseInt(getFlagValue("--max-pages")!, 10)
  : Infinity;

// ---------------------------------------------------------------------------
// Italian month map
// ---------------------------------------------------------------------------

const ITALIAN_MONTHS: Record<string, string> = {
  gennaio: "01",
  febbraio: "02",
  marzo: "03",
  aprile: "04",
  maggio: "05",
  giugno: "06",
  luglio: "07",
  agosto: "08",
  settembre: "09",
  ottobre: "10",
  novembre: "11",
  dicembre: "12",
};

/**
 * Parse an Italian date string like "23 novembre 2021" into "2021-11-23".
 * Also handles "23/11/2021" and "2021-11-23" formats.
 * Returns null if unparseable.
 */
function parseItalianDate(raw: string): string | null {
  const cleaned = raw.trim().toLowerCase();

  // Try "DD monthName YYYY"
  const longMatch = cleaned.match(/^(\d{1,2})\s+(\S+)\s+(\d{4})$/);
  if (longMatch) {
    const [, day, monthName, year] = longMatch;
    const month = ITALIAN_MONTHS[monthName!];
    if (month && day && year) {
      return `${year}-${month}-${day.padStart(2, "0")}`;
    }
  }

  // Try "DD/MM/YYYY"
  const slashMatch = cleaned.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (slashMatch) {
    const [, day, month, year] = slashMatch;
    if (day && month && year) {
      return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
    }
  }

  // Try ISO "YYYY-MM-DD"
  const isoMatch = cleaned.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    return cleaned;
  }

  return null;
}

/**
 * Extract an Italian date from a longer text string.
 * Returns the first date found, or null.
 */
function extractItalianDate(text: string): string | null {
  const match = text.match(
    /(\d{1,2})\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s+(\d{4})/i,
  );
  if (match) {
    return parseItalianDate(match[0]);
  }

  // Try DD/MM/YYYY
  const slashMatch = text.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (slashMatch) {
    return parseItalianDate(slashMatch[0]);
  }

  return null;
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimit(): Promise<void> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }
  lastRequestTime = Date.now();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Fetch a URL with rate limiting, retries, and proper headers.
 * Returns the response text, or null on persistent failure.
 */
async function fetchWithRetry(url: string, retries = MAX_RETRIES): Promise<string | null> {
  await rateLimit();
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
      const res = await fetch(url, {
        signal: controller.signal,
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "it-IT,it;q=0.9,en;q=0.5",
          "Accept-Encoding": "gzip, deflate",
          Connection: "keep-alive",
          "Cache-Control": "no-cache",
        },
      });
      clearTimeout(timeoutId);

      if (res.status === 403 || res.status === 429) {
        log(`  WARN: HTTP ${res.status} for ${url} (attempt ${attempt}/${retries})`);
        if (attempt < retries) {
          const backoff = RETRY_BACKOFF_MS * attempt * 2;
          log(`    Backing off ${backoff}ms before retry...`);
          await sleep(backoff);
          continue;
        }
        return null;
      }

      if (!res.ok) {
        log(`  WARN: HTTP ${res.status} for ${url} (attempt ${attempt}/${retries})`);
        if (attempt < retries) {
          await sleep(RETRY_BACKOFF_MS * attempt);
          continue;
        }
        return null;
      }

      return await res.text();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`  WARN: attempt ${attempt}/${retries} failed for ${url}: ${msg}`);
      if (attempt < retries) {
        await sleep(RETRY_BACKOFF_MS * attempt);
      }
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

const stats = {
  decisionsScraped: 0,
  decisionsInserted: 0,
  decisionsSkipped: 0,
  mergersScraped: 0,
  mergersInserted: 0,
  mergersSkipped: 0,
  pagesScraped: 0,
  bulletinsProcessed: 0,
  errors: 0,
  sectorsUpserted: 0,
};

function log(msg: string): void {
  const ts = new Date().toISOString().slice(0, 19).replace("T", " ");
  console.log(`[${ts}] ${msg}`);
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

interface IngestState {
  processedCaseNumbers: string[];
  processedBulletins: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

function loadState(): IngestState {
  if (FLAG_RESUME && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      log("WARN: Could not read state file, starting fresh.");
    }
  }
  return {
    processedCaseNumbers: [],
    processedBulletins: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// Database setup
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    log(`Created directory ${dir}`);
  }

  if (FLAG_FORCE && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    log(`Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  log(`Database ready at ${DB_PATH}`);
  return db;
}

// ---------------------------------------------------------------------------
// Sector normalisation
// ---------------------------------------------------------------------------

const SECTOR_MAP: Record<string, { id: string; name: string; name_en: string }> = {
  digitale: { id: "digitale", name: "Economia digitale", name_en: "Digital Economy" },
  "economia digitale": { id: "digitale", name: "Economia digitale", name_en: "Digital Economy" },
  energia: { id: "energia", name: "Energia", name_en: "Energy" },
  "grande distribuzione organizzata": { id: "grande_distribuzione", name: "Grande distribuzione organizzata", name_en: "Retail" },
  "grande distribuzione": { id: "grande_distribuzione", name: "Grande distribuzione organizzata", name_en: "Retail" },
  distribuzione: { id: "grande_distribuzione", name: "Grande distribuzione organizzata", name_en: "Retail" },
  "servizi finanziari": { id: "servizi_finanziari", name: "Servizi finanziari", name_en: "Financial Services" },
  banche: { id: "servizi_finanziari", name: "Servizi finanziari", name_en: "Financial Services" },
  bancario: { id: "servizi_finanziari", name: "Servizi finanziari", name_en: "Financial Services" },
  assicurazioni: { id: "assicurazioni", name: "Assicurazioni", name_en: "Insurance" },
  farmaceutico: { id: "farmaceutico", name: "Settore farmaceutico", name_en: "Pharmaceutical" },
  "settore farmaceutico": { id: "farmaceutico", name: "Settore farmaceutico", name_en: "Pharmaceutical" },
  telecomunicazioni: { id: "telecomunicazioni", name: "Telecomunicazioni", name_en: "Telecommunications" },
  "tlc": { id: "telecomunicazioni", name: "Telecomunicazioni", name_en: "Telecommunications" },
  trasporti: { id: "trasporti", name: "Trasporti", name_en: "Transport" },
  trasporto: { id: "trasporti", name: "Trasporti", name_en: "Transport" },
  "trasporto ferroviario": { id: "trasporti", name: "Trasporti", name_en: "Transport" },
  "trasporto aereo": { id: "trasporti", name: "Trasporti", name_en: "Transport" },
  costruzioni: { id: "costruzioni", name: "Costruzioni", name_en: "Construction" },
  edilizia: { id: "costruzioni", name: "Costruzioni", name_en: "Construction" },
  media: { id: "media", name: "Media", name_en: "Media" },
  editoria: { id: "media", name: "Media", name_en: "Media" },
  agroalimentare: { id: "agroalimentare", name: "Agroalimentare", name_en: "Food & Agriculture" },
  alimentare: { id: "agroalimentare", name: "Agroalimentare", name_en: "Food & Agriculture" },
  automotive: { id: "automotive", name: "Automotive", name_en: "Automotive" },
  automobile: { id: "automotive", name: "Automotive", name_en: "Automotive" },
  chimico: { id: "chimico", name: "Settore chimico", name_en: "Chemicals" },
  siderurgico: { id: "siderurgico", name: "Settore siderurgico", name_en: "Steel" },
  "servizi professionali": { id: "servizi_professionali", name: "Servizi professionali", name_en: "Professional Services" },
  "professionisti": { id: "servizi_professionali", name: "Servizi professionali", name_en: "Professional Services" },
  sanitario: { id: "sanitario", name: "Settore sanitario", name_en: "Healthcare" },
  sanita: { id: "sanitario", name: "Settore sanitario", name_en: "Healthcare" },
  turismo: { id: "turismo", name: "Turismo", name_en: "Tourism" },
  immobiliare: { id: "immobiliare", name: "Settore immobiliare", name_en: "Real Estate" },
  rifiuti: { id: "rifiuti", name: "Gestione rifiuti", name_en: "Waste Management" },
  "servizi postali": { id: "servizi_postali", name: "Servizi postali", name_en: "Postal Services" },
  "servizi idrici": { id: "servizi_idrici", name: "Servizi idrici", name_en: "Water Services" },
};

function normaliseSector(rawSector: string): { id: string; name: string; name_en: string } {
  const key = rawSector.trim().toLowerCase();
  const mapped = SECTOR_MAP[key];
  if (mapped) return mapped;

  // Generate a slug from the raw text
  const id = key
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
  return { id, name: rawSector.trim(), name_en: rawSector.trim() };
}

// ---------------------------------------------------------------------------
// Case type classification
// ---------------------------------------------------------------------------

/**
 * Classify the type of AGCM case from the case number prefix.
 *
 * Patterns:
 *   A###  — abuse of dominance (abuso di posizione dominante)
 *   I###  — cartels / restrictive agreements (intese restrittive)
 *   C###  — merger control (concentrazioni)
 *   IC### — combined investigation (intesa + concentrazione)
 *   S###  — sector inquiry (indagine conoscitiva)
 *   PS### — consumer protection (excluded from competition DB)
 *   IP### — misleading advertising (excluded)
 *   CV### — comparative advertising (excluded)
 *   SP### — moral suasion (excluded)
 */
function classifyCaseType(caseNumber: string): "decision" | "merger" | "excluded" {
  const upper = caseNumber.toUpperCase().trim();
  if (/^A\d/.test(upper)) return "decision";
  if (/^I\d/.test(upper)) return "decision";
  if (/^IC\d/.test(upper)) return "decision";
  if (/^S\d/.test(upper)) return "decision";
  if (/^C\d/.test(upper)) return "merger";
  // Consumer protection and advertising — not competition law
  if (/^PS\d/.test(upper)) return "excluded";
  if (/^IP\d/.test(upper)) return "excluded";
  if (/^CV\d/.test(upper)) return "excluded";
  if (/^SP\d/.test(upper)) return "excluded";
  // Unknown prefix — include as decision to be safe
  return "decision";
}

/**
 * Determine the competition decision sub-type from the case number and text context.
 */
function classifyDecisionType(caseNumber: string, text: string): string {
  const upper = caseNumber.toUpperCase().trim();
  const lower = text.toLowerCase();

  if (/^A\d/.test(upper)) return "abuse_of_dominance";
  if (/^I\d/.test(upper)) {
    // Distinguish between cartel and other intese
    if (lower.includes("cartello") || lower.includes("cartel")) return "cartel";
    if (lower.includes("intesa orizzontale") || lower.includes("fissazione dei prezzi")) return "cartel";
    if (lower.includes("intesa verticale")) return "vertical_agreement";
    return "cartel"; // Default for I-cases
  }
  if (/^IC\d/.test(upper)) return "mixed_investigation";
  if (/^S\d/.test(upper)) return "sector_inquiry";

  // Fallback: try to detect from text
  if (lower.includes("abuso di posizione dominante") || lower.includes("abuso")) return "abuse_of_dominance";
  if (lower.includes("intesa") || lower.includes("accordo restrittivo")) return "cartel";
  if (lower.includes("indagine conoscitiva")) return "sector_inquiry";

  return "decision";
}

// ---------------------------------------------------------------------------
// Outcome normalisation
// ---------------------------------------------------------------------------

/**
 * Normalise Italian outcome text into a taxonomy.
 */
function normaliseOutcome(raw: string): string {
  const lower = raw.toLowerCase();

  if (/sanzione pecuniaria|sanzione|multa|ammenda/.test(lower)) return "fine";
  if (/impegni/.test(lower)) return "commitments";
  if (/misure cautelari|misura cautelare/.test(lower)) return "interim_measures";
  if (/non violazione|non sussist|archivia|non luogo/.test(lower)) return "dismissed";
  if (/inottemperanza/.test(lower)) return "non_compliance";
  if (/autorizzat[ao]\s+(con|sotto)\s+(condizioni|prescrizioni)/.test(lower)) return "cleared_with_conditions";
  if (/autorizzat[ao]/.test(lower)) return "cleared";
  if (/divieto|vietat[ao]/.test(lower)) return "prohibited";
  if (/fase\s*II|fase\s*2|secondo\s+fase/.test(lower)) return "phase_2_referral";
  if (/rinvio/.test(lower)) return "referral";
  if (/violazione|infrazione|accertamento/.test(lower)) return "infringement_found";

  return raw.trim();
}

// ---------------------------------------------------------------------------
// Fine amount extraction
// ---------------------------------------------------------------------------

/**
 * Extract a fine amount in EUR from Italian text.
 *
 * Handles formats like:
 *   - "102.000.000 euro"
 *   - "102 milioni di euro"
 *   - "1,13 miliardi di euro"
 *   - "euro 10.700.000"
 */
function extractFineAmount(text: string): number | null {
  // Pattern 1: "N milioni/miliardi di euro"
  const multiplierMatch = text.match(
    /([\d.,\s]+)\s*(milion[ie]|miliard[ie])\s+(?:di\s+)?euro/i,
  );
  if (multiplierMatch) {
    const rawNum = multiplierMatch[1]!.replace(/\s/g, "").replace(/\./g, "").replace(",", ".");
    let amount = parseFloat(rawNum);
    if (!isNaN(amount)) {
      const unit = multiplierMatch[2]!.toLowerCase();
      if (unit.startsWith("milion")) amount *= 1_000_000;
      if (unit.startsWith("miliard")) amount *= 1_000_000_000;
      return amount;
    }
  }

  // Pattern 2: "N.NNN.NNN euro" or "euro N.NNN.NNN"
  const euroMatch = text.match(
    /(?:euro\s+)?([\d.]+(?:,\d+)?)\s*(?:euro|EUR)/i,
  );
  if (euroMatch) {
    // Italian number format: dots as thousands separators, comma as decimal
    const rawNum = euroMatch[1]!.replace(/\./g, "").replace(",", ".");
    const amount = parseFloat(rawNum);
    if (!isNaN(amount) && amount > 0) return amount;
  }

  // Pattern 3: "sanzione di euro N" or "sanzione pari a euro N"
  const sanctionMatch = text.match(
    /sanzione\s+(?:pecuniaria\s+)?(?:di|pari\s+a)\s+(?:euro\s+)?([\d.,\s]+)/i,
  );
  if (sanctionMatch) {
    const rawNum = sanctionMatch[1]!.replace(/\s/g, "").replace(/\./g, "").replace(",", ".");
    const amount = parseFloat(rawNum);
    if (!isNaN(amount) && amount > 0) return amount;
  }

  return null;
}

// ---------------------------------------------------------------------------
// Legal articles extraction
// ---------------------------------------------------------------------------

/**
 * Extract references to Italian and EU competition law articles.
 */
function extractLegalArticles(text: string): string[] {
  const articles: string[] = [];
  const seen = new Set<string>();

  // Italian competition law: Legge 287/1990
  const l287Matches = text.matchAll(
    /art(?:icol[oi])?\s*\.?\s*(\d+)\s+(?:della\s+)?(?:L(?:egge)?\.?\s*(?:n\.?\s*)?)?287\/1990/gi,
  );
  for (const m of l287Matches) {
    const art = `Art. ${m[1]} L. 287/1990`;
    if (!seen.has(art)) {
      seen.add(art);
      articles.push(art);
    }
  }

  // TFUE articles (101 and 102)
  const tfueMatches = text.matchAll(
    /art(?:icol[oi])?\s*\.?\s*(101|102)\s+(?:del\s+)?T(?:FUE|rattato)/gi,
  );
  for (const m of tfueMatches) {
    const art = `Art. ${m[1]} TFUE`;
    if (!seen.has(art)) {
      seen.add(art);
      articles.push(art);
    }
  }

  // Broader pattern: "L. 287/1990" without specific article
  if (articles.length === 0 && /L\.?\s*287\/1990/i.test(text)) {
    articles.push("L. 287/1990");
  }

  return articles;
}

// ---------------------------------------------------------------------------
// Parsed data interfaces
// ---------------------------------------------------------------------------

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  legge_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

// ---------------------------------------------------------------------------
// Bollettino index crawler — discover bulletin URLs
// ---------------------------------------------------------------------------

/**
 * Build the list of Bollettino PDF URLs to crawl.
 *
 * The AGCM publishes weekly bulletins as PDFs at a predictable URL:
 *   https://www.agcm.it/dotcmsdoc/bollettini/{YYYY}/{N}-{YY}.pdf
 *
 * Where N is the weekly bulletin number (1-52) and YY is the 2-digit year.
 * We generate all candidate URLs and filter by HTTP status.
 */
function generateBollettinoUrls(startYear: number, endYear: number, maxTotal: number): string[] {
  const urls: string[] = [];

  for (let year = endYear; year >= startYear; year--) {
    const yy = (year % 100).toString().padStart(2, "0");
    for (let num = MAX_BULLETINS_PER_YEAR; num >= 1; num--) {
      if (urls.length >= maxTotal) return urls;
      urls.push(`${BASE_URL}/dotcmsdoc/bollettini/${year}/${num}-${yy}.pdf`);
    }
  }

  return urls;
}

/**
 * Discover Bollettino HTML detail page URLs from the listing.
 *
 * HTML bulletin pages follow the pattern:
 *   /pubblicazioni/bollettino-settimanale/{YYYY}/{N}/Bollettino-{N}-{YYYY}
 *
 * We crawl the listing page and also generate predictable URLs.
 */
function generateBollettinoPageUrls(startYear: number, endYear: number, maxTotal: number): string[] {
  const urls: string[] = [];

  for (let year = endYear; year >= startYear; year--) {
    for (let num = MAX_BULLETINS_PER_YEAR; num >= 1; num--) {
      if (urls.length >= maxTotal) return urls;
      urls.push(
        `${BASE_URL}/pubblicazioni/bollettino-settimanale/${year}/${num}/Bollettino-${num}-${year}`,
      );
    }
  }

  return urls;
}

// ---------------------------------------------------------------------------
// Listing page parser: decisions
// ---------------------------------------------------------------------------

interface ListingItem {
  caseNumber: string;
  title: string;
  date: string | null;
  detailUrl: string | null;
  provvedimentoNumber: string | null;
}

/**
 * Parse an AGCM decision listing page.
 *
 * The AGCM site (dotCMS) renders provvedimenti in various list formats.
 * We try multiple selectors to extract case numbers and links.
 */
function parseDecisionListing(html: string): ListingItem[] {
  const $ = cheerio.load(html);
  const items: ListingItem[] = [];
  const seen = new Set<string>();

  // Strategy 1: Look for links to detail pages containing case numbers
  $("a").each((_i, el) => {
    const $a = $(el);
    const href = $a.attr("href") ?? "";
    const text = $a.text().trim();

    // Skip navigation, footer, menu links
    if ($a.closest("nav, footer, header, .menu, .breadcrumb").length > 0) return;
    if (!text) return;

    // Extract case number from link text or href
    const caseMatch = text.match(/\b([AIC]\d{2,5}[A-Z]?)\b/) ??
      href.match(/\/([AIC]\d{2,5}[A-Z]?)(?:[/-]|$)/);

    if (!caseMatch) return;
    const caseNumber = caseMatch[1]!;

    if (seen.has(caseNumber)) return;
    seen.add(caseNumber);

    // Extract date from surrounding context
    const $container = $a.closest("tr, li, .item, .row, div").first();
    const containerText = $container.text();
    const date = extractItalianDate(containerText);

    const detailUrl = href.startsWith("http") ? href : href ? `${BASE_URL}${href}` : null;

    // Extract provvedimento number
    const provMatch = containerText.match(/[Pp]rovvedimento\s+n\.\s*(\d+)/);

    items.push({
      caseNumber,
      title: text.length > 200 ? text.slice(0, 200) : text,
      date,
      detailUrl,
      provvedimentoNumber: provMatch ? provMatch[1]! : null,
    });
  });

  // Strategy 2: Look for text patterns with case numbers in table rows or list items
  $("tr, li, .item, .views-row, .list-item").each((_i, el) => {
    const $el = $(el);
    const text = $el.text().trim();

    const caseMatch = text.match(/\b([AIC]\d{2,5}[A-Z]?)\b/);
    if (!caseMatch) return;
    const caseNumber = caseMatch[1]!;

    if (seen.has(caseNumber)) return;
    seen.add(caseNumber);

    const date = extractItalianDate(text);
    const $link = $el.find("a").first();
    const href = $link.attr("href") ?? "";
    const detailUrl = href.startsWith("http") ? href : href ? `${BASE_URL}${href}` : null;

    const provMatch = text.match(/[Pp]rovvedimento\s+n\.\s*(\d+)/);

    items.push({
      caseNumber,
      title: ($link.text().trim() || text).slice(0, 200),
      date,
      detailUrl,
      provvedimentoNumber: provMatch ? provMatch[1]! : null,
    });
  });

  return items;
}

// ---------------------------------------------------------------------------
// Bollettino HTML page parser — extract decisions from bulletin detail pages
// ---------------------------------------------------------------------------

interface BollettinoDecision {
  caseNumber: string;
  title: string;
  date: string | null;
  fullText: string;
  provvedimentoNumber: string | null;
}

/**
 * Parse a Bollettino HTML page and extract individual competition decisions.
 *
 * Bollettino pages contain multiple provvedimenti. Competition decisions
 * appear under the "TUTELA DELLA CONCORRENZA" section. Each provvedimento
 * starts with a header like "Provvedimento n. NNNNN" followed by case info.
 */
function parseBollettinoPage(html: string): BollettinoDecision[] {
  const $ = cheerio.load(html);
  const decisions: BollettinoDecision[] = [];
  const seen = new Set<string>();

  // Remove navigation and boilerplate
  $("nav, footer, header, script, style, .menu, .breadcrumb").remove();

  const bodyText = $("body").text();

  // Split the bulletin text by provvedimento markers
  const provvedimentoBlocks = bodyText.split(/(?=Provvedimento\s+n\.\s*\d+)/i);

  for (const block of provvedimentoBlocks) {
    if (block.length < 50) continue;

    // Extract provvedimento number
    const provMatch = block.match(/Provvedimento\s+n\.\s*(\d+)/i);
    if (!provMatch) continue;
    const provNumber = provMatch[1]!;

    // Extract case number
    const caseMatch = block.match(/\b([AIC]\d{2,5}[A-Z]?)\b/);
    if (!caseMatch) continue;
    const caseNumber = caseMatch[1]!;

    // Skip consumer protection / advertising cases
    if (classifyCaseType(caseNumber) === "excluded") continue;

    if (seen.has(caseNumber)) continue;
    seen.add(caseNumber);

    // Extract date
    const date = extractItalianDate(block);

    // Try to extract a meaningful title from the block
    // Titles often appear after "L'AUTORITÀ GARANTE..." or as the subject line
    let title = "";
    const titleMatch = block.match(
      /(?:Caso|Fascicolo|Procedimento)\s+[AIC]\d{2,5}[A-Z]?\s*[-–—]\s*(.+?)(?:\n|$)/i,
    );
    if (titleMatch) {
      title = titleMatch[1]!.trim();
    }

    if (!title) {
      // Try to find a meaningful first line after the provvedimento header
      const lines = block.split("\n").map((l) => l.trim()).filter((l) => l.length > 10);
      for (const line of lines.slice(0, 5)) {
        if (!/^Provvedimento/i.test(line) && !/^L['']AUTORIT/i.test(line)) {
          title = line.slice(0, 200);
          break;
        }
      }
    }

    if (!title) {
      title = `Provvedimento n. ${provNumber} — ${caseNumber}`;
    }

    // Use the full block as full_text (cleaned)
    const fullText = block
      .replace(/\s{3,}/g, "\n\n")
      .trim();

    decisions.push({
      caseNumber,
      title,
      date,
      fullText,
      provvedimentoNumber: provNumber,
    });
  }

  return decisions;
}

// ---------------------------------------------------------------------------
// Decision detail page parser
// ---------------------------------------------------------------------------

/**
 * Parse an individual AGCM decision detail page.
 *
 * These pages (at dettaglio?id=UUID or press release URLs) contain the
 * full text of a provvedimento. The dotCMS layout varies, so we try
 * multiple extraction strategies.
 */
function parseDecisionDetail(html: string, fallbackCaseNumber: string): ParsedDecision | null {
  const $ = cheerio.load(html);

  // Remove boilerplate
  $("nav, footer, header, script, style, .menu, .breadcrumb, .visually-hidden").remove();

  const pageText = $("body").text();
  if (pageText.length < 100) return null;

  // Extract case number (verify or discover)
  let caseNumber = fallbackCaseNumber;
  const caseMatch = pageText.match(/\b([AIC]\d{2,5}[A-Z]?)\b/);
  if (caseMatch) caseNumber = caseMatch[1]!;

  // Title from h1 or page title
  let title = $("h1").first().text().trim() || $("title").text().trim();
  if (!title) title = `Case ${caseNumber}`;

  // Clean title: remove "AGCM - " prefix if present
  title = title.replace(/^AGCM\s*[-–—]\s*/i, "").trim();

  // Date
  const date = extractItalianDate(pageText);

  // Decision type
  const type = classifyDecisionType(caseNumber, pageText);

  // Parties — look for named entities near "parti" or "imprese"
  const parties = extractParties($, pageText);

  // Summary — first substantial paragraph or explicit summary section
  const summary = extractSummary($, pageText);

  // Full text
  let fullText = "";
  const contentSelectors = [
    ".content-body",
    ".node__content",
    ".field--name-body",
    "article .content",
    "#content-area",
    "main article",
    "main .content",
    ".detail-content",
    "main",
  ];

  for (const sel of contentSelectors) {
    const $content = $(sel);
    if ($content.length > 0) {
      $content.find("nav, .menu, .breadcrumb, script, style").remove();
      const bodyText = $content.text().trim();
      if (bodyText.length > fullText.length) {
        fullText = bodyText;
      }
      break;
    }
  }

  if (!fullText || fullText.length < 100) {
    fullText = pageText;
  }

  fullText = fullText.replace(/\s{3,}/g, "\n\n").trim();

  // Outcome
  const outcome = detectOutcome(pageText);

  // Fine amount
  const fineAmount = extractFineAmount(pageText);

  // Legal articles
  const articles = extractLegalArticles(pageText);

  // Status
  let status = "final";
  if (/ricorso|appello|impugnazione|impugnat[oa]/i.test(pageText)) {
    status = "appealed";
  }
  if (/misure?\s+cautelar[ie]/i.test(pageText)) {
    status = "interim";
  }

  // Sector
  const sector = detectSector(pageText);

  return {
    case_number: caseNumber,
    title,
    date,
    type,
    sector,
    parties: parties ? JSON.stringify(parties) : null,
    summary,
    full_text: fullText,
    outcome,
    fine_amount: fineAmount,
    legge_articles: articles.length > 0 ? JSON.stringify(articles) : null,
    status,
  };
}

// ---------------------------------------------------------------------------
// Merger detail page parser
// ---------------------------------------------------------------------------

function parseMergerDetail(html: string, fallbackCaseNumber: string): ParsedMerger | null {
  const $ = cheerio.load(html);

  $("nav, footer, header, script, style, .menu, .breadcrumb, .visually-hidden").remove();

  const pageText = $("body").text();
  if (pageText.length < 100) return null;

  let caseNumber = fallbackCaseNumber;
  const caseMatch = pageText.match(/\b(C\d{2,5}[A-Z]?)\b/);
  if (caseMatch) caseNumber = caseMatch[1]!;

  let title = $("h1").first().text().trim() || $("title").text().trim();
  title = title.replace(/^AGCM\s*[-–—]\s*/i, "").trim();
  if (!title) title = `Merger ${caseNumber}`;

  const date = extractItalianDate(pageText);
  const sector = detectSector(pageText);

  // Acquiring party and target
  let acquiringParty: string | null = null;
  let target: string | null = null;

  // Try title patterns: "X / Y" or "acquisizione di Y da parte di X"
  const slashMatch = title.match(/^(.+?)\s*[/–—]\s*(.+?)(?:\s*[-–—]|$)/);
  if (slashMatch) {
    acquiringParty = slashMatch[1]!.trim();
    target = slashMatch[2]!.trim();
  }

  if (!acquiringParty) {
    const acquMatch = pageText.match(
      /(?:acquisizione|presa\s+di\s+controllo)\s+(?:di|del(?:la)?)\s+(.+?)\s+(?:da\s+parte\s+di|ad\s+opera\s+di)\s+(.+?)(?:[.,;]|$)/i,
    );
    if (acquMatch) {
      target = acquMatch[1]!.trim().slice(0, 200);
      acquiringParty = acquMatch[2]!.trim().slice(0, 200);
    }
  }

  if (!acquiringParty) {
    const partiMatch = pageText.match(
      /parti\s*(?:notificanti?)?\s*[:]\s*(.+?)(?:\n|$)/i,
    );
    if (partiMatch) {
      acquiringParty = partiMatch[1]!.trim().slice(0, 200);
    }
  }

  // Summary
  const summary = extractSummary($, pageText);

  // Full text
  let fullText = "";
  const contentSelectors = [
    ".content-body",
    ".node__content",
    ".field--name-body",
    "article .content",
    "main article",
    "main",
  ];

  for (const sel of contentSelectors) {
    const $content = $(sel);
    if ($content.length > 0) {
      $content.find("nav, .menu, .breadcrumb, script, style").remove();
      const bodyText = $content.text().trim();
      if (bodyText.length > fullText.length) {
        fullText = bodyText;
      }
      break;
    }
  }

  if (!fullText || fullText.length < 100) {
    fullText = pageText;
  }
  fullText = fullText.replace(/\s{3,}/g, "\n\n").trim();

  // Outcome
  const outcome = detectMergerOutcome(pageText);

  // Turnover
  let turnover: number | null = null;
  const turnoverMatch = pageText.match(
    /fatturato\s+(?:totale|complessivo|aggregate|mondiale)?\s*(?:di\s+)?([\d.,\s]+)\s*(milion[ie]|miliard[ie])?\s*(?:di\s+)?euro/i,
  );
  if (turnoverMatch) {
    let rawNum = turnoverMatch[1]!.replace(/\s/g, "").replace(/\./g, "").replace(",", ".");
    let amount = parseFloat(rawNum);
    if (!isNaN(amount)) {
      const unit = (turnoverMatch[2] ?? "").toLowerCase();
      if (unit.startsWith("milion")) amount *= 1_000_000;
      if (unit.startsWith("miliard")) amount *= 1_000_000_000;
      turnover = amount;
    }
  }

  return {
    case_number: caseNumber,
    title,
    date,
    sector,
    acquiring_party: acquiringParty,
    target,
    summary,
    full_text: fullText,
    outcome,
    turnover,
  };
}

// ---------------------------------------------------------------------------
// Content extraction helpers
// ---------------------------------------------------------------------------

/**
 * Extract party names from the text.
 */
function extractParties($: cheerio.CheerioAPI, pageText: string): string[] | null {
  const parties: string[] = [];
  const seen = new Set<string>();

  // Strategy 1: Look for "Parti:" or "Imprese:" labels
  const labels = ["Parti", "Imprese", "Soggetti", "Societa", "Società"];
  for (const label of labels) {
    const re = new RegExp(label + "\\s*[:]\\s*(.+?)(?:\\n|$)", "i");
    const match = pageText.match(re);
    if (match) {
      const raw = match[1]!;
      for (const p of raw.split(/[,;]|(?:\s+e\s+)/)) {
        const trimmed = p.trim();
        if (trimmed.length > 2 && trimmed.length < 200 && !seen.has(trimmed)) {
          seen.add(trimmed);
          parties.push(trimmed);
        }
      }
      if (parties.length > 0) return parties;
    }
  }

  // Strategy 2: Look for company names (S.p.A., S.r.l., etc.) in the text
  const companyMatches = pageText.matchAll(
    /([A-Z][A-Za-z\s&.''-]+(?:S\.p\.A\.|S\.r\.l\.|S\.a\.s\.|S\.n\.c\.|Inc\.|LLC|Ltd|GmbH|SA|BV|NV|SpA|Srl))/g,
  );
  for (const m of companyMatches) {
    const name = m[1]!.trim();
    if (name.length > 3 && name.length < 200 && !seen.has(name)) {
      seen.add(name);
      parties.push(name);
      if (parties.length >= 10) break; // Cap at 10 parties
    }
  }

  return parties.length > 0 ? parties : null;
}

/**
 * Extract a summary from the page.
 */
function extractSummary($: cheerio.CheerioAPI, pageText: string): string | null {
  // Strategy 1: Look for explicit summary sections
  let summary: string | null = null;

  $("h2, h3, h4, strong, b").each((_i, heading) => {
    if (summary) return;
    const headingText = $(heading).text().trim().toLowerCase();
    if (
      headingText.includes("sintesi") ||
      headingText.includes("sommario") ||
      headingText.includes("abstract") ||
      headingText.includes("riassunto")
    ) {
      const parts: string[] = [];
      let $next = $(heading).next();
      while ($next.length > 0 && !$next.is("h1, h2, h3")) {
        const text = $next.text().trim();
        if (text) parts.push(text);
        $next = $next.next();
      }
      if (parts.length > 0) {
        summary = parts.join("\n\n").slice(0, 2000);
      }
    }
  });

  if (summary) return summary;

  // Strategy 2: First substantive paragraph
  const paragraphs = pageText
    .split("\n")
    .map((p) => p.trim())
    .filter((p) => p.length > 100);

  if (paragraphs.length > 0) {
    return paragraphs[0]!.slice(0, 1000);
  }

  return null;
}

/**
 * Detect the outcome of a competition decision from Italian text.
 */
function detectOutcome(text: string): string | null {
  const lower = text.toLowerCase();

  // Check for fine/sanction — most common outcome
  if (/sanzione\s+pecuniaria|ha\s+irrogato\s+una\s+sanzione|ha\s+sanzionato/i.test(lower)) return "fine";
  if (/impegni\s+(?:presentati|accettati|resi\s+obbligatori)/i.test(lower)) return "commitments";
  if (/misure?\s+cautelar[ie]/i.test(lower)) return "interim_measures";
  if (/non\s+(?:ha\s+)?(?:sussist|ravis)/i.test(lower)) return "dismissed";
  if (/archiviazione|archiviat[oa]/i.test(lower)) return "dismissed";
  if (/violazione|accertamento.*infrazione|ha\s+accertato.*abuso/i.test(lower)) return "infringement_found";
  if (/inottemperanza/i.test(lower)) return "non_compliance";

  return null;
}

/**
 * Detect the outcome of a merger decision from Italian text.
 */
function detectMergerOutcome(text: string): string | null {
  const lower = text.toLowerCase();

  if (/autorizzat[oa]\s+(?:con|a)\s+(?:condizioni|prescrizioni)/i.test(lower)) return "cleared_with_conditions";
  if (/autorizzat[oa]|non\s+osta/i.test(lower)) return "cleared";
  if (/divieto|vietat[oa]/i.test(lower)) return "prohibited";
  if (/fase\s*(?:II|2)|secondo.*fase|approfondimento/i.test(lower)) return "phase_2_referral";
  if (/impegni/i.test(lower)) return "cleared_with_conditions";
  if (/rinvio.*commissione/i.test(lower)) return "referral_to_ec";

  return null;
}

/**
 * Detect the sector from page text using keyword matching.
 */
function detectSector(text: string): string | null {
  const lower = text.toLowerCase();

  const sectorKeywords: Array<[string, string]> = [
    ["telecomunicazioni|telefonia|banda larga|rete fissa|rete mobile", "telecomunicazioni"],
    ["farmaceutic[oa]|medicinale|farmaci|dispositivi medici", "farmaceutico"],
    ["energia|elettricità|elettricita|gas naturale|fonti rinnovabili", "energia"],
    ["digitale|online|internet|piattaforma|e-commerce|marketplace|motore di ricerca", "digitale"],
    ["bancari[oa]|banca|credito|finanziario|assicurativ[oa]", "servizi_finanziari"],
    ["distribuzione|supermercato|retail|grande distribuzione|GDO", "grande_distribuzione"],
    ["trasport[oi]|ferroviari[oa]|aere[oa]|autostrad[ae]|logistic[oa]", "trasporti"],
    ["edilizia|costruzion[ie]|infrastrutture|appalti", "costruzioni"],
    ["agroalimentar[ei]|alimentar[ei]|agricol[oa]", "agroalimentare"],
    ["automobilistico|automotive|auto|veicol[oi]", "automotive"],
    ["media|editoria|televisione|radiofonico|audiovisivo", "media"],
    ["chimic[oa]|petrolchimico|carburanti|benzina", "chimico"],
    ["siderurgic[oa]|acciaio|metallurigico", "siderurgico"],
    ["sanitari[oa]|ospedaliero|salute", "sanitario"],
    ["turism[oa]|alberghiero|viaggi", "turismo"],
    ["immobiliar[ei]|real estate", "immobiliare"],
    ["rifiuti|ambientale|ecologia", "rifiuti"],
    ["postal[ei]|corriere|spedizioni", "servizi_postali"],
  ];

  for (const [pattern, sectorId] of sectorKeywords) {
    if (new RegExp(pattern!, "i").test(lower)) {
      return sectorId!;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Bollettino-based decision extraction
// ---------------------------------------------------------------------------

/**
 * Process a single Bollettino page and extract competition decisions.
 *
 * Transforms BollettinoDecision items into ParsedDecision / ParsedMerger.
 */
function processBolettinoDecisions(
  bollettinoDecisions: BollettinoDecision[],
): { decisions: ParsedDecision[]; mergers: ParsedMerger[] } {
  const decisions: ParsedDecision[] = [];
  const mergers: ParsedMerger[] = [];

  for (const bd of bollettinoDecisions) {
    const caseType = classifyCaseType(bd.caseNumber);

    if (caseType === "excluded") continue;

    if (caseType === "merger") {
      const sector = detectSector(bd.fullText);
      const outcome = detectMergerOutcome(bd.fullText);

      // Try to extract acquiring party and target from title
      let acquiringParty: string | null = null;
      let target: string | null = null;
      const slashMatch = bd.title.match(/^(.+?)\s*[/–—]\s*(.+?)(?:\s*[-–—]|$)/);
      if (slashMatch) {
        acquiringParty = slashMatch[1]!.trim();
        target = slashMatch[2]!.trim();
      }

      // Turnover
      let turnover: number | null = null;
      const turnoverMatch = bd.fullText.match(
        /fatturato\s+(?:totale|complessivo|aggregate|mondiale)?\s*(?:di\s+)?([\d.,\s]+)\s*(milion[ie]|miliard[ie])?\s*(?:di\s+)?euro/i,
      );
      if (turnoverMatch) {
        let rawNum = turnoverMatch[1]!.replace(/\s/g, "").replace(/\./g, "").replace(",", ".");
        let amount = parseFloat(rawNum);
        if (!isNaN(amount)) {
          const unit = (turnoverMatch[2] ?? "").toLowerCase();
          if (unit.startsWith("milion")) amount *= 1_000_000;
          if (unit.startsWith("miliard")) amount *= 1_000_000_000;
          turnover = amount;
        }
      }

      // Summary: first paragraph
      const paragraphs = bd.fullText.split("\n\n").filter((p) => p.length > 50);
      const summary = paragraphs.length > 0 ? paragraphs[0]!.slice(0, 1000) : null;

      mergers.push({
        case_number: bd.caseNumber,
        title: bd.title,
        date: bd.date,
        sector,
        acquiring_party: acquiringParty,
        target,
        summary,
        full_text: bd.fullText,
        outcome,
        turnover,
      });
    } else {
      // Competition decision (abuse, cartel, sector inquiry)
      const type = classifyDecisionType(bd.caseNumber, bd.fullText);
      const sector = detectSector(bd.fullText);
      const outcome = detectOutcome(bd.fullText);
      const fineAmount = extractFineAmount(bd.fullText);
      const articles = extractLegalArticles(bd.fullText);

      // Parties
      const $ = cheerio.load("<body>" + bd.fullText + "</body>");
      const parties = extractParties($, bd.fullText);

      // Summary
      const paragraphs = bd.fullText.split("\n\n").filter((p) => p.length > 50);
      const summary = paragraphs.length > 0 ? paragraphs[0]!.slice(0, 1000) : null;

      // Status
      let status = "final";
      if (/ricorso|appello|impugnazione/i.test(bd.fullText)) status = "appealed";
      if (/misure?\s+cautelar[ie]/i.test(bd.fullText)) status = "interim";

      decisions.push({
        case_number: bd.caseNumber,
        title: bd.title,
        date: bd.date,
        type,
        sector,
        parties: parties ? JSON.stringify(parties) : null,
        summary,
        full_text: bd.fullText,
        outcome,
        fine_amount: fineAmount,
        legge_articles: articles.length > 0 ? JSON.stringify(articles) : null,
        status,
      });
    }
  }

  return { decisions, mergers };
}

// ---------------------------------------------------------------------------
// Main crawl strategies
// ---------------------------------------------------------------------------

/**
 * Strategy 1: Crawl the provvedimenti listing page and follow links
 * to individual decision detail pages.
 */
async function crawlDecisionListing(
  db: Database.Database | null,
  existingCases: Set<string>,
  insertDecision: Database.Statement | null,
  upsertSector: Database.Statement | null,
): Promise<void> {
  log("--- Strategy: Decision listing pages ---");

  let page = 0;
  let totalItems = 0;

  while (page < MAX_PAGES) {
    const url = page === 0
      ? `${BASE_URL}${DECISIONS_LIST_PATH}`
      : `${BASE_URL}${DECISIONS_LIST_PATH}?page=${page}`;

    log(`Fetching decisions listing page ${page}: ${url}`);
    stats.pagesScraped++;

    const html = await fetchWithRetry(url);
    if (!html) {
      log(`  Could not fetch listing page ${page} — stopping listing crawl`);
      break;
    }

    const items = parseDecisionListing(html);
    if (items.length === 0) {
      log(`  No items on page ${page} — reached end of listing`);
      break;
    }

    log(`  Found ${items.length} items on page ${page}`);
    totalItems += items.length;

    for (const item of items) {
      const caseType = classifyCaseType(item.caseNumber);
      if (caseType === "excluded") continue;

      if (existingCases.has(item.caseNumber)) {
        if (caseType === "merger") stats.mergersSkipped++;
        else stats.decisionsSkipped++;
        continue;
      }

      // Only crawl detail pages for decisions (not mergers from this listing)
      if (caseType !== "merger" && item.detailUrl) {
        log(`  Scraping decision detail: ${item.caseNumber} — ${item.detailUrl}`);
        stats.decisionsScraped++;

        const detailHtml = await fetchWithRetry(item.detailUrl);
        if (detailHtml) {
          const detail = parseDecisionDetail(detailHtml, item.caseNumber);
          if (detail && !FLAG_DRY_RUN && insertDecision && upsertSector) {
            try {
              insertDecision.run(
                detail.case_number, detail.title, detail.date, detail.type,
                detail.sector, detail.parties, detail.summary, detail.full_text,
                detail.outcome, detail.fine_amount, detail.legge_articles, detail.status,
              );
              stats.decisionsInserted++;
              existingCases.add(detail.case_number);

              if (detail.sector) {
                const norm = normaliseSector(detail.sector);
                upsertSector.run(norm.id, norm.name, norm.name_en);
                stats.sectorsUpserted++;
              }
            } catch (err: unknown) {
              const msg = err instanceof Error ? err.message : String(err);
              log(`    ERROR inserting ${detail.case_number}: ${msg}`);
              stats.errors++;
            }
          } else if (detail && FLAG_DRY_RUN) {
            log(`    [DRY RUN] Would insert: ${detail.case_number} | ${detail.title.slice(0, 60)}...`);
          }
        } else {
          log(`    Could not fetch detail page for ${item.caseNumber}`);
          stats.errors++;
        }
      }
    }

    page++;
  }

  log(`  Decision listing crawl complete: ${totalItems} items found across ${page} pages`);
}

/**
 * Strategy 2: Crawl the merger/concentration listing page.
 */
async function crawlMergerListing(
  db: Database.Database | null,
  existingCases: Set<string>,
  insertMerger: Database.Statement | null,
  upsertSector: Database.Statement | null,
): Promise<void> {
  log("--- Strategy: Merger listing pages ---");

  let page = 0;
  let totalItems = 0;

  while (page < MAX_PAGES) {
    const url = page === 0
      ? `${BASE_URL}${MERGERS_LIST_PATH}`
      : `${BASE_URL}${MERGERS_LIST_PATH}?page=${page}`;

    log(`Fetching mergers listing page ${page}: ${url}`);
    stats.pagesScraped++;

    const html = await fetchWithRetry(url);
    if (!html) {
      log(`  Could not fetch merger listing page ${page} — stopping`);
      break;
    }

    const items = parseDecisionListing(html);
    // Filter to only C-prefixed cases
    const mergerItems = items.filter((item) => classifyCaseType(item.caseNumber) === "merger");

    if (mergerItems.length === 0 && items.length === 0) {
      log(`  No items on page ${page} — reached end of listing`);
      break;
    }

    log(`  Found ${mergerItems.length} merger items on page ${page}`);
    totalItems += mergerItems.length;

    for (const item of mergerItems) {
      if (existingCases.has(item.caseNumber)) {
        stats.mergersSkipped++;
        continue;
      }

      if (item.detailUrl) {
        log(`  Scraping merger detail: ${item.caseNumber} — ${item.detailUrl}`);
        stats.mergersScraped++;

        const detailHtml = await fetchWithRetry(item.detailUrl);
        if (detailHtml) {
          const detail = parseMergerDetail(detailHtml, item.caseNumber);
          if (detail && !FLAG_DRY_RUN && insertMerger && upsertSector) {
            try {
              insertMerger.run(
                detail.case_number, detail.title, detail.date, detail.sector,
                detail.acquiring_party, detail.target, detail.summary,
                detail.full_text, detail.outcome, detail.turnover,
              );
              stats.mergersInserted++;
              existingCases.add(detail.case_number);

              if (detail.sector) {
                const norm = normaliseSector(detail.sector);
                upsertSector.run(norm.id, norm.name, norm.name_en);
                stats.sectorsUpserted++;
              }
            } catch (err: unknown) {
              const msg = err instanceof Error ? err.message : String(err);
              log(`    ERROR inserting merger ${detail.case_number}: ${msg}`);
              stats.errors++;
            }
          } else if (detail && FLAG_DRY_RUN) {
            log(`    [DRY RUN] Would insert merger: ${detail.case_number} | ${detail.title.slice(0, 60)}...`);
          }
        } else {
          log(`    Could not fetch detail page for ${item.caseNumber}`);
          stats.errors++;
        }
      }
    }

    page++;
  }

  log(`  Merger listing crawl complete: ${totalItems} items found across ${page} pages`);
}

/**
 * Strategy 3: Crawl Bollettino HTML pages.
 *
 * The weekly bulletins contain the full text of all decisions.
 * This is the most reliable data source — the listing pages may block
 * scraping but the bulletin pages are designed for public access.
 */
async function crawlBollettini(
  db: Database.Database | null,
  existingCases: Set<string>,
  processedBulletins: Set<string>,
  insertDecision: Database.Statement | null,
  insertMerger: Database.Statement | null,
  upsertSector: Database.Statement | null,
  state: IngestState,
): Promise<void> {
  log("--- Strategy: Bollettino weekly bulletins ---");

  const totalBulletins = Math.min(
    (BOLLETTINO_END_YEAR - BOLLETTINO_START_YEAR + 1) * MAX_BULLETINS_PER_YEAR,
    MAX_PAGES === Infinity ? Infinity : MAX_PAGES * MAX_BULLETINS_PER_YEAR,
  );

  // Iterate years from newest to oldest for fresher data first
  let bulletinsAttempted = 0;
  let consecutiveNotFound = 0;

  for (let year = BOLLETTINO_END_YEAR; year >= BOLLETTINO_START_YEAR; year--) {
    if (bulletinsAttempted >= totalBulletins) break;

    // Reset consecutive not found counter for each year
    consecutiveNotFound = 0;

    // For the current year, start from the current week; otherwise start from 52
    const maxWeek = year === BOLLETTINO_END_YEAR
      ? Math.ceil((Date.now() - new Date(year, 0, 1).getTime()) / (7 * 24 * 60 * 60 * 1000))
      : MAX_BULLETINS_PER_YEAR;

    for (let num = maxWeek; num >= 1; num--) {
      if (bulletinsAttempted >= totalBulletins) break;

      const bulletinKey = `${year}-${num}`;
      if (processedBulletins.has(bulletinKey)) {
        continue;
      }

      bulletinsAttempted++;

      const url = `${BASE_URL}/pubblicazioni/bollettino-settimanale/${year}/${num}/Bollettino-${num}-${year}`;
      log(`Fetching Bollettino ${num}/${year}: ${url}`);
      stats.pagesScraped++;

      const html = await fetchWithRetry(url);
      if (!html) {
        consecutiveNotFound++;
        // If we get 5 consecutive failures for a year, assume no more bulletins
        if (consecutiveNotFound >= 5) {
          log(`  5 consecutive failures for ${year} — moving to previous year`);
          break;
        }
        continue;
      }

      consecutiveNotFound = 0;
      stats.bulletinsProcessed++;

      const bollettinoDecisions = parseBollettinoPage(html);
      if (bollettinoDecisions.length === 0) {
        log(`  No competition decisions found in Bollettino ${num}/${year}`);
        processedBulletins.add(bulletinKey);
        state.processedBulletins.push(bulletinKey);
        continue;
      }

      log(`  Found ${bollettinoDecisions.length} competition decisions in Bollettino ${num}/${year}`);

      const { decisions, mergers } = processBolettinoDecisions(bollettinoDecisions);

      // Insert decisions
      if (!FLAG_MERGERS_ONLY) {
        for (const d of decisions) {
          if (existingCases.has(d.case_number)) {
            stats.decisionsSkipped++;
            continue;
          }

          if (FLAG_DRY_RUN) {
            log(`    [DRY RUN] Would insert decision: ${d.case_number} | ${d.title.slice(0, 60)}...`);
            continue;
          }

          if (insertDecision && upsertSector) {
            try {
              insertDecision.run(
                d.case_number, d.title, d.date, d.type, d.sector,
                d.parties, d.summary, d.full_text, d.outcome,
                d.fine_amount, d.legge_articles, d.status,
              );
              stats.decisionsInserted++;
              existingCases.add(d.case_number);

              if (d.sector) {
                const norm = normaliseSector(d.sector);
                upsertSector.run(norm.id, norm.name, norm.name_en);
                stats.sectorsUpserted++;
              }
            } catch (err: unknown) {
              const msg = err instanceof Error ? err.message : String(err);
              log(`    ERROR inserting decision ${d.case_number}: ${msg}`);
              stats.errors++;
            }
          }
        }
      }

      // Insert mergers
      if (!FLAG_DECISIONS_ONLY) {
        for (const m of mergers) {
          if (existingCases.has(m.case_number)) {
            stats.mergersSkipped++;
            continue;
          }

          if (FLAG_DRY_RUN) {
            log(`    [DRY RUN] Would insert merger: ${m.case_number} | ${m.title.slice(0, 60)}...`);
            continue;
          }

          if (insertMerger && upsertSector) {
            try {
              insertMerger.run(
                m.case_number, m.title, m.date, m.sector,
                m.acquiring_party, m.target, m.summary,
                m.full_text, m.outcome, m.turnover,
              );
              stats.mergersInserted++;
              existingCases.add(m.case_number);

              if (m.sector) {
                const norm = normaliseSector(m.sector);
                upsertSector.run(norm.id, norm.name, norm.name_en);
                stats.sectorsUpserted++;
              }
            } catch (err: unknown) {
              const msg = err instanceof Error ? err.message : String(err);
              log(`    ERROR inserting merger ${m.case_number}: ${msg}`);
              stats.errors++;
            }
          }
        }
      }

      processedBulletins.add(bulletinKey);
      state.processedBulletins.push(bulletinKey);

      // Save state periodically
      if (stats.bulletinsProcessed % 10 === 0 && !FLAG_DRY_RUN) {
        saveState(state);
        log(`  State saved (${stats.bulletinsProcessed} bulletins processed)`);
      }
    }
  }

  log(`  Bollettino crawl complete: ${stats.bulletinsProcessed} bulletins processed`);
}

/**
 * Strategy 4: Crawl press releases for additional decision data.
 *
 * AGCM press releases follow:
 *   /media/comunicati-stampa/{YYYY}/{MM}/{CASE_NUMBER}
 */
async function crawlPressReleases(
  db: Database.Database | null,
  existingCases: Set<string>,
  insertDecision: Database.Statement | null,
  insertMerger: Database.Statement | null,
  upsertSector: Database.Statement | null,
): Promise<void> {
  log("--- Strategy: Press releases ---");

  let page = 0;
  let totalItems = 0;

  while (page < Math.min(MAX_PAGES, 20)) {
    const url = page === 0
      ? `${BASE_URL}${PRESS_RELEASE_PATH}`
      : `${BASE_URL}${PRESS_RELEASE_PATH}?page=${page}`;

    log(`Fetching press releases page ${page}: ${url}`);
    stats.pagesScraped++;

    const html = await fetchWithRetry(url);
    if (!html) {
      log(`  Could not fetch press release page ${page} — stopping`);
      break;
    }

    // Parse press release listing for links containing case numbers
    const $ = cheerio.load(html);
    const links: Array<{ caseNumber: string; url: string }> = [];

    $("a").each((_i, el) => {
      const href = $(el).attr("href") ?? "";
      const text = $(el).text().trim();

      if ($(el).closest("nav, footer, header, .menu, .breadcrumb").length > 0) return;

      // Look for case number in URL path (e.g. /2025/12/A576)
      const urlCaseMatch = href.match(/\/(\d{4})\/\d{1,2}\/([AIC]\d{2,5}[A-Z]?)(?:[/-]|$)/);
      if (urlCaseMatch) {
        const caseNumber = urlCaseMatch[2]!;
        const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
        links.push({ caseNumber, url: fullUrl });
        return;
      }

      // Look for case number in link text
      const textCaseMatch = text.match(/\b([AIC]\d{2,5}[A-Z]?)\b/);
      if (textCaseMatch && href) {
        const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
        links.push({ caseNumber: textCaseMatch[1]!, url: fullUrl });
      }
    });

    if (links.length === 0) {
      log(`  No press release links found on page ${page} — stopping`);
      break;
    }

    log(`  Found ${links.length} press release links on page ${page}`);
    totalItems += links.length;

    // Deduplicate
    const seen = new Set<string>();
    for (const link of links) {
      if (seen.has(link.caseNumber)) continue;
      seen.add(link.caseNumber);

      if (existingCases.has(link.caseNumber)) continue;

      const caseType = classifyCaseType(link.caseNumber);
      if (caseType === "excluded") continue;

      if (FLAG_DECISIONS_ONLY && caseType === "merger") continue;
      if (FLAG_MERGERS_ONLY && caseType !== "merger") continue;

      log(`  Scraping press release: ${link.caseNumber} — ${link.url}`);

      const detailHtml = await fetchWithRetry(link.url);
      if (!detailHtml) {
        stats.errors++;
        continue;
      }

      if (caseType === "merger") {
        stats.mergersScraped++;
        const detail = parseMergerDetail(detailHtml, link.caseNumber);
        if (detail && !FLAG_DRY_RUN && insertMerger && upsertSector) {
          try {
            insertMerger.run(
              detail.case_number, detail.title, detail.date, detail.sector,
              detail.acquiring_party, detail.target, detail.summary,
              detail.full_text, detail.outcome, detail.turnover,
            );
            stats.mergersInserted++;
            existingCases.add(detail.case_number);

            if (detail.sector) {
              const norm = normaliseSector(detail.sector);
              upsertSector.run(norm.id, norm.name, norm.name_en);
              stats.sectorsUpserted++;
            }
          } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err);
            log(`    ERROR inserting merger ${detail.case_number}: ${msg}`);
            stats.errors++;
          }
        } else if (detail && FLAG_DRY_RUN) {
          log(`    [DRY RUN] Would insert merger: ${detail.case_number}`);
        }
      } else {
        stats.decisionsScraped++;
        const detail = parseDecisionDetail(detailHtml, link.caseNumber);
        if (detail && !FLAG_DRY_RUN && insertDecision && upsertSector) {
          try {
            insertDecision.run(
              detail.case_number, detail.title, detail.date, detail.type,
              detail.sector, detail.parties, detail.summary, detail.full_text,
              detail.outcome, detail.fine_amount, detail.legge_articles, detail.status,
            );
            stats.decisionsInserted++;
            existingCases.add(detail.case_number);

            if (detail.sector) {
              const norm = normaliseSector(detail.sector);
              upsertSector.run(norm.id, norm.name, norm.name_en);
              stats.sectorsUpserted++;
            }
          } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err);
            log(`    ERROR inserting decision ${detail.case_number}: ${msg}`);
            stats.errors++;
          }
        } else if (detail && FLAG_DRY_RUN) {
          log(`    [DRY RUN] Would insert decision: ${detail.case_number}`);
        }
      }
    }

    page++;
  }

  log(`  Press release crawl complete: ${totalItems} links found across ${page} pages`);
}

// ---------------------------------------------------------------------------
// Sector count refresh
// ---------------------------------------------------------------------------

function refreshSectorCounts(db: Database.Database): void {
  if (FLAG_DRY_RUN) return;

  log("Refreshing sector counts...");

  db.exec(`
    UPDATE sectors SET
      decision_count = COALESCE((
        SELECT COUNT(*) FROM decisions WHERE decisions.sector = sectors.id
      ), 0),
      merger_count = COALESCE((
        SELECT COUNT(*) FROM mergers WHERE mergers.sector = sectors.id
      ), 0)
  `);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  log("AGCM (Autorità Garante della Concorrenza e del Mercato) ingestion crawler");
  log(`  DB_PATH:         ${DB_PATH}`);
  log(`  --resume:        ${FLAG_RESUME}`);
  log(`  --dry-run:       ${FLAG_DRY_RUN}`);
  log(`  --force:         ${FLAG_FORCE}`);
  log(`  --max-pages:     ${MAX_PAGES === Infinity ? "unlimited" : MAX_PAGES}`);
  log(`  --mergers-only:  ${FLAG_MERGERS_ONLY}`);
  log(`  --decisions-only: ${FLAG_DECISIONS_ONLY}`);
  log(`  Bollettino range: ${BOLLETTINO_START_YEAR} — ${BOLLETTINO_END_YEAR}`);
  log("");

  // Load resume state
  const state = loadState();
  const processedBulletins = new Set(state.processedBulletins);

  // Initialize DB
  const db = FLAG_DRY_RUN ? null : initDb();

  // Build set of existing case numbers for resume/dedup
  const existingCases = new Set<string>();
  if (db && (FLAG_RESUME || !FLAG_FORCE)) {
    const decisionRows = db
      .prepare("SELECT case_number FROM decisions")
      .all() as Array<{ case_number: string }>;
    for (const r of decisionRows) existingCases.add(r.case_number);

    const mergerRows = db
      .prepare("SELECT case_number FROM mergers")
      .all() as Array<{ case_number: string }>;
    for (const r of mergerRows) existingCases.add(r.case_number);

    log(`Existing records: ${existingCases.size} case numbers in DB`);
  }

  // Prepare statements
  const insertDecision = db
    ? db.prepare(`
        INSERT OR IGNORE INTO decisions
          (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, legge_articles, status)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `)
    : null;

  const insertMerger = db
    ? db.prepare(`
        INSERT OR IGNORE INTO mergers
          (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `)
    : null;

  const upsertSector = db
    ? db.prepare(`
        INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
        VALUES (?, ?, ?, '', 0, 0)
        ON CONFLICT(id) DO UPDATE SET
          name = excluded.name,
          name_en = excluded.name_en
      `)
    : null;

  try {
    // Strategy 1: Bollettino pages (most reliable — full decision text)
    await crawlBollettini(
      db, existingCases, processedBulletins,
      insertDecision, insertMerger, upsertSector, state,
    );

    // Strategy 2: Decision listing pages (fills gaps)
    if (!FLAG_MERGERS_ONLY) {
      await crawlDecisionListing(db, existingCases, insertDecision, upsertSector);
    }

    // Strategy 3: Merger listing pages
    if (!FLAG_DECISIONS_ONLY) {
      await crawlMergerListing(db, existingCases, insertMerger, upsertSector);
    }

    // Strategy 4: Press releases (supplementary, recent decisions)
    await crawlPressReleases(
      db, existingCases, insertDecision, insertMerger, upsertSector,
    );

    // Refresh sector counts
    if (db) {
      refreshSectorCounts(db);
    }

    // Save final state
    if (!FLAG_DRY_RUN) {
      state.decisionsIngested = stats.decisionsInserted;
      state.mergersIngested = stats.mergersInserted;
      saveState(state);
    }
  } finally {
    if (db) db.close();
  }

  // Print summary
  log("");
  log("=== Ingestion complete ===");
  log(`  Bulletins processed: ${stats.bulletinsProcessed}`);
  log(`  Pages scraped:       ${stats.pagesScraped}`);
  log(`  Decisions scraped:   ${stats.decisionsScraped}`);
  log(`  Decisions inserted:  ${stats.decisionsInserted}`);
  log(`  Decisions skipped:   ${stats.decisionsSkipped}`);
  log(`  Mergers scraped:     ${stats.mergersScraped}`);
  log(`  Mergers inserted:    ${stats.mergersInserted}`);
  log(`  Mergers skipped:     ${stats.mergersSkipped}`);
  log(`  Sectors upserted:    ${stats.sectorsUpserted}`);
  log(`  Errors:              ${stats.errors}`);

  if (stats.errors > 0) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exitCode = 2;
});
