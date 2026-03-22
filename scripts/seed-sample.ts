/**
 * Seed the AGCM database with sample decisions, mergers, and sectors for testing.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["AGCM_DB_PATH"] ?? "data/agcm.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

// --- Sectors -----------------------------------------------------------------

interface SectorRow {
  id: string;
  name: string;
  name_en: string;
  description: string;
  decision_count: number;
  merger_count: number;
}

const sectors: SectorRow[] = [
  { id: "digitale", name: "Economia digitale", name_en: "Digital Economy", description: "Piattaforme online, social network, motori di ricerca e marketplace digitali.", decision_count: 2, merger_count: 1 },
  { id: "energia", name: "Energia", name_en: "Energy", description: "Forniture di elettricita e gas, energie rinnovabili e reti energetiche.", decision_count: 1, merger_count: 0 },
  { id: "grande_distribuzione", name: "Grande distribuzione organizzata", name_en: "Retail", description: "Grande distribuzione alimentare, discount e relazioni nella filiera.", decision_count: 1, merger_count: 1 },
  { id: "servizi_finanziari", name: "Servizi finanziari", name_en: "Financial Services", description: "Banche, assicurazioni, pagamenti e infrastrutture dei mercati finanziari.", decision_count: 0, merger_count: 1 },
  { id: "farmaceutico", name: "Settore farmaceutico", name_en: "Pharmaceutical", description: "Produzione e distribuzione di farmaci, dispositivi medici e servizi sanitari.", decision_count: 1, merger_count: 0 },
  { id: "telecomunicazioni", name: "Telecomunicazioni", name_en: "Telecommunications", description: "Telefonia mobile, banda larga, reti fisse e infrastrutture TLC.", decision_count: 1, merger_count: 1 },
];

const insertSector = db.prepare(
  "INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)",
);

for (const s of sectors) {
  insertSector.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
}

console.log(`Inserted ${sectors.length} sectors`);

// --- Decisions ---------------------------------------------------------------

interface DecisionRow {
  case_number: string;
  title: string;
  date: string;
  type: string;
  sector: string;
  parties: string;
  summary: string;
  full_text: string;
  outcome: string;
  fine_amount: number | null;
  legge_articles: string;
  status: string;
}

const decisions: DecisionRow[] = [
  {
    case_number: "A555",
    title: "Google — Abuso di posizione dominante nel settore dei servizi di comparazione prezzi",
    date: "2021-11-23",
    type: "abuse_of_dominance",
    sector: "digitale",
    parties: JSON.stringify(["Google LLC", "Alphabet Inc."]),
    summary: "L'AGCM ha sanzionato Google per 102 milioni di euro per aver abusato della propria posizione dominante nel mercato dei servizi di comparazione prezzi (Comparison Shopping Services), favorendo il proprio servizio Google Shopping nei risultati di ricerca generali.",
    full_text: "L'Autorita Garante della Concorrenza e del Mercato ha avviato un procedimento istruttorio nei confronti di Google LLC e Alphabet Inc. per abuso di posizione dominante ai sensi dell'articolo 102 TFUE e dell'articolo 3 della Legge n. 287/1990. L'istruttoria ha accertato che Google ha favorito il proprio servizio di comparazione di prezzi (Google Shopping) all'interno dei risultati di ricerca generali di Google Search, posizionando il proprio servizio in modo prominente mediante la c.d. Shopping Unit. Questo favoritismo ha penalizzato i servizi di comparazione di prezzi concorrenti, riducendo il traffico verso questi ultimi e rafforzando la posizione di Google Shopping. L'AGCM ha ritenuto che tale condotta costituisse un abuso di posizione dominante, in quanto sfavoriva i concorrenti senza una giustificazione obiettiva. La sanzione irrogata e stata pari a 102 milioni di euro. Google ha fatto ricorso al TAR del Lazio, che ha confermato la decisione dell'AGCM.",
    outcome: "fine",
    fine_amount: 102000000,
    legge_articles: JSON.stringify(["Art. 3 L. 287/1990", "Art. 102 TFUE"]),
    status: "appealed",
  },
  {
    case_number: "A559",
    title: "Amazon — Abuso di posizione dominante nel mercato della logistica",
    date: "2022-11-03",
    type: "abuse_of_dominance",
    sector: "digitale",
    parties: JSON.stringify(["Amazon EU SARL", "Amazon.com Inc."]),
    summary: "L'AGCM ha sanzionato Amazon per 1,13 miliardi di euro per aver abusato della propria posizione dominante nel mercato dei marketplace, favorendo i venditori che utilizzavano il servizio logistico FBA (Fulfillment by Amazon) nell'accesso alla Buy Box e alla gestione della piattaforma.",
    full_text: "L'AGCM ha condotto un'istruttoria approfondita sulle pratiche di Amazon nel mercato dei marketplace in Italia e ha accertato che Amazon ha abusato della propria posizione dominante in due mercati tra loro collegati: (1) il mercato dei servizi di intermediazione su piattaforma per la vendita online al dettaglio (marketplace) e (2) il mercato dei servizi di logistica per i venditori terzi su marketplace. Amazon ha favorito il proprio servizio di logistica FBA (Fulfillment by Amazon) nel mercato dei marketplace attraverso: il condizionamento dell'accesso alla Buy Box (l'offerta in evidenza nella scheda prodotto) all'utilizzo di FBA, l'esclusione dei venditori non FBA dal servizio Prime, la gestione dei resi e il supporto al cliente. L'AGCM ha concluso che queste condotte costituivano abusi di posizione dominante ai sensi dell'art. 3 della L. 287/1990 e dell'art. 102 TFUE. La sanzione irrogata e stata pari a circa 1,13 miliardi di euro. L'AGCM ha inoltre imposto misure correttive per separare le attivita di marketplace da quelle di logistica.",
    outcome: "fine",
    fine_amount: 1130000000,
    legge_articles: JSON.stringify(["Art. 3 L. 287/1990", "Art. 102 TFUE"]),
    status: "appealed",
  },
  {
    case_number: "I839",
    title: "Industria molitoria — Cartello nel settore della semola di grano duro",
    date: "2019-07-24",
    type: "cartel",
    sector: "grande_distribuzione",
    parties: JSON.stringify(["Casillo Group", "Grandi Molini Italiani", "Molino Grassi", "Lo Conte Srl", "Caputo"]),
    summary: "L'AGCM ha sanzionato le principali aziende molitorie italiane per un cartello nel settore della semola di grano duro. Le imprese si erano coordinate sui prezzi e sulla ripartizione dei clienti nel periodo 2010-2016, causando danni significativi all'industria della pasta.",
    full_text: "L'AGCM ha accertato l'esistenza di un cartello tra le principali imprese operanti nel settore della molitura del grano duro in Italia per la produzione di semole e semolati. Il cartello si e svolto nel periodo 2010-2016 e ha interessato le principali imprese del settore. Le pratiche anticoncorrenziali accertate comprendevano: (1) coordinamento sui prezzi di vendita della semola di grano duro alla grande distribuzione organizzata e ai pastifici; (2) ripartizione dei clienti tra le imprese partecipanti all'intesa; (3) scambi di informazioni commercialmente sensibili relative ai prezzi, ai volumi di vendita e alla clientela. Il cartello ha causato un aumento artificioso dei prezzi della semola, con conseguenti effetti negativi sulla competitivita dell'industria della pasta italiana. La semola di grano duro e il principale ingrediente della pasta italiana e le pratiche anticoncorrenziali hanno influenzato l'intera filiera della pasta. Le sanzioni totali irrogate dall'AGCM ammontano a circa 59 milioni di euro.",
    outcome: "fine",
    fine_amount: 59000000,
    legge_articles: JSON.stringify(["Art. 2 L. 287/1990", "Art. 101 TFUE"]),
    status: "final",
  },
  {
    case_number: "A517",
    title: "Pfizer — Abuso di posizione dominante nel mercato del Xalatan",
    date: "2012-01-11",
    type: "abuse_of_dominance",
    sector: "farmaceutico",
    parties: JSON.stringify(["Pfizer Inc.", "Pfizer Italy Srl"]),
    summary: "L'AGCM ha sanzionato Pfizer per 10,7 milioni di euro per aver messo in atto una strategia abusiva volta a prolungare artificialmente il proprio monopolio nel mercato del Latanoprost, il principio attivo del farmaco antiglaucoma Xalatan, ostacolando l'ingresso dei generici.",
    full_text: "L'AGCM ha accertato che Pfizer Inc. e Pfizer Italy hanno abusato della propria posizione dominante nel mercato del Latanoprost (principio attivo del farmaco antiglaucoma Xalatan), attuando una strategia volta a ritardare artificialmente l'ingresso dei farmaci generici nel mercato italiano. La strategia abusiva comprendeva: (1) la divisione volontaria del brevetto europeo relativo al Latanoprost in brevetti nazionali, al fine di avere procedure legali separate in ogni paese dell'UE e rallentare la scadenza brevettuale; (2) la concessione di licenze in condizioni sfavorevoli ai produttori di generici, con clausole che ritardavano l'effettiva entrata in commercio dei generici; (3) accordi di licenza con i produttori di generici che prevedevano il ritardo volontario nell'immissione in commercio. L'AGCM ha ritenuto che queste pratiche, nel loro complesso, costituissero un abuso di posizione dominante ai sensi dell'art. 3 della L. 287/1990 e dell'art. 102 TFUE. La sanzione irrogata e stata pari a 10,7 milioni di euro.",
    outcome: "fine",
    fine_amount: 10700000,
    legge_articles: JSON.stringify(["Art. 3 L. 287/1990", "Art. 102 TFUE"]),
    status: "final",
  },
  {
    case_number: "A557",
    title: "Telecom Italia (TIM) — Abuso di posizione dominante nel mercato della rete fissa",
    date: "2021-07-08",
    type: "abuse_of_dominance",
    sector: "telecomunicazioni",
    parties: JSON.stringify(["Telecom Italia SpA", "TIM"]),
    summary: "L'AGCM ha sanzionato TIM per 116 milioni di euro per aver ostacolato l'attivita degli operatori concorrenti che necessitano dell'accesso alla rete fissa di TIM per fornire servizi di telecomunicazioni agli utenti finali.",
    full_text: "L'AGCM ha condotto un'istruttoria sull'operato di Telecom Italia SpA (TIM) nel mercato della fornitura di servizi all'ingrosso di accesso alla rete fissa (servizi di accesso wholesale) per accertare se TIM avesse abusato della propria posizione dominante in tale mercato. L'istruttoria ha accertato che TIM ha sistematicamente ostacolato l'attivita degli operatori alternativi (OAO) che si avvalgono dell'accesso alla propria rete per fornire servizi ai clienti finali. Le pratiche accertate includevano: (1) ostacoli alla migrazione dei clienti dagli accessi in rame agli accessi in fibra ottica forniti da TIM tramite Open Fiber; (2) difficolta nella portabilita del numero e nel cambio operatore; (3) ritardi e errori nelle pratiche di attivazione degli accessi wholesale per gli OAO; (4) degradazione della qualita del servizio wholesale fornito agli OAO rispetto a quello offerto ai propri clienti finali. La sanzione irrogata e stata pari a 116 milioni di euro.",
    outcome: "fine",
    fine_amount: 116000000,
    legge_articles: JSON.stringify(["Art. 3 L. 287/1990", "Art. 102 TFUE"]),
    status: "appealed",
  },
];

const insertDecision = db.prepare(`
  INSERT OR IGNORE INTO decisions
    (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, legge_articles, status)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertDecisionsAll = db.transaction(() => {
  for (const d of decisions) {
    insertDecision.run(
      d.case_number, d.title, d.date, d.type, d.sector,
      d.parties, d.summary, d.full_text, d.outcome,
      d.fine_amount, d.legge_articles, d.status,
    );
  }
});

insertDecisionsAll();
console.log(`Inserted ${decisions.length} decisions`);

// --- Mergers -----------------------------------------------------------------

interface MergerRow {
  case_number: string;
  title: string;
  date: string;
  sector: string;
  acquiring_party: string;
  target: string;
  summary: string;
  full_text: string;
  outcome: string;
  turnover: number | null;
}

const mergers: MergerRow[] = [
  {
    case_number: "C12345",
    title: "Intesa Sanpaolo / UBI Banca — Fusione bancaria",
    date: "2020-10-22",
    sector: "servizi_finanziari",
    acquiring_party: "Intesa Sanpaolo SpA",
    target: "Unione di Banche Italiane (UBI Banca) SpA",
    summary: "L'AGCM ha autorizzato la fusione tra Intesa Sanpaolo e UBI Banca con condizioni significative. La fusione ha creato il principale gruppo bancario italiano. Le autorizzazioni sono state subordinate alla cessione di circa 500 filiali bancarie a BPER Banca per mantenere la concorrenza nei mercati locali.",
    full_text: "Intesa Sanpaolo ha lanciato un'offerta pubblica di acquisto (OPA) su UBI Banca, uno dei principali gruppi bancari italiani. La fusione ha creato il piu grande gruppo bancario italiano per attivi. L'AGCM ha esaminato la concentrazione ai sensi della normativa italiana sulle concentrazioni. L'analisi ha riguardato i mercati bancari locali, in cui i due gruppi erano presenti con reti di filiali sovrapposte. L'AGCM ha identificato rischi concorrenziali in numerose province italiane in cui la combinazione di Intesa Sanpaolo e UBI Banca avrebbe comportato una riduzione significativa della concorrenza nel mercato della raccolta dei depositi e dell'erogazione di crediti alla clientela retail e alle PMI. Per rimediare a questi problemi, l'AGCM ha imposto come condizione la cessione di circa 500 filiali bancarie a BPER Banca, garantendo che i clienti delle filiali cedute potessero continuare ad avvalersi di servizi bancari competitivi. La Banca d'Italia ha agito come autorita di vigilanza prudenziale, mentre l'AGCM ha esercitato il controllo delle concentrazioni.",
    outcome: "cleared_with_conditions",
    turnover: 20000000000,
  },
  {
    case_number: "C12190",
    title: "Esselunga / Ipercoop — Acquisizione di punti vendita",
    date: "2018-11-14",
    sector: "grande_distribuzione",
    acquiring_party: "Esselunga SpA",
    target: "Punti vendita Ipercoop (Coop)",
    summary: "L'AGCM ha autorizzato con condizioni l'acquisizione di alcuni punti vendita Ipercoop da parte di Esselunga. Nelle aree dove le due insegne si sovrapponevano geograficamente, l'Autorita ha imposto la cessione di alcuni punti vendita a operatori terzi.",
    full_text: "Esselunga ha acquisito alcuni punti vendita della catena Ipercoop in diverse localita italiane. L'AGCM ha esaminato l'operazione e ha identificato sovrapposizioni geografiche tra i punti vendita di Esselunga e quelli di Ipercoop in alcune zone di Milano e hinterland, nonche in localita della Toscana. In queste aree di sovrapposizione, l'acquisizione avrebbe ridotto la concorrenza nel mercato della distribuzione alimentare al dettaglio, considerato su base locale (l'area di attrazione di un supermercato e tipicamente valutata entro un raggio di circa 10-15 minuti di percorrenza). L'AGCM ha autorizzato l'operazione a condizione che Esselunga cedesse a terzi alcune delle strutture acquisite nelle aree di maggiore sovrapposizione.",
    outcome: "cleared_with_conditions",
    turnover: 8000000000,
  },
  {
    case_number: "C12500",
    title: "Vodafone Italia / INWIT — Accordo sulle torri di telecomunicazione",
    date: "2019-07-24",
    sector: "telecomunicazioni",
    acquiring_party: "Vodafone Italia SpA",
    target: "Infrastrutture Wireless Italiane (INWIT) SpA",
    summary: "L'AGCM ha autorizzato l'accordo tra Vodafone Italia e INWIT per la creazione di un polo comune delle infrastrutture passive di telecomunicazione. L'AGCM ha accettato impegni per garantire l'accesso non discriminatorio di operatori terzi alle infrastrutture.",
    full_text: "Vodafone Italia e INWIT (societa quotata che gestisce le torri di telecomunicazione, originariamente controllata da Telecom Italia) hanno concordato la creazione di una joint venture per la gestione congiunta delle rispettive infrastrutture passive (torri e siti di trasmissione). L'operazione ha comportato il conferimento da parte di Vodafone Italia del proprio portafoglio di torri e la fusione con INWIT, con conseguente aumento della partecipazione di TIM in INWIT. L'AGCM ha esaminato l'operazione e ha identificato potenziali rischi per la concorrenza nell'accesso alle infrastrutture passive di telecomunicazione da parte degli operatori di rete terzi (MVNO e operatori alternativi). L'AGCM ha autorizzato l'operazione a seguito dell'assunzione di impegni comportamentali che garantiscono l'accesso non discriminatorio e a condizioni di mercato alle infrastrutture della joint venture da parte di tutti gli operatori di rete.",
    outcome: "cleared_with_conditions",
    turnover: 3000000000,
  },
];

const insertMerger = db.prepare(`
  INSERT OR IGNORE INTO mergers
    (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertMergersAll = db.transaction(() => {
  for (const m of mergers) {
    insertMerger.run(
      m.case_number, m.title, m.date, m.sector,
      m.acquiring_party, m.target, m.summary, m.full_text,
      m.outcome, m.turnover,
    );
  }
});

insertMergersAll();
console.log(`Inserted ${mergers.length} mergers`);

const decisionCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mergerCount = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sectorCount = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Sectors:   ${sectorCount}`);
console.log(`  Decisions: ${decisionCount}`);
console.log(`  Mergers:   ${mergerCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
