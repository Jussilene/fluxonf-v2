// src/db/sqlite.js
import Database from "better-sqlite3";
import path from "path";
import fs from "fs";

const dbPath = path.join(process.cwd(), "data", "nfse.db");

// garante que a pasta data existe
const dataDir = path.join(process.cwd(), "data");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new Database(dbPath);

// ✅ pragmas seguros (melhora concorrência e estabilidade; não muda tua lógica)
try {
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("foreign_keys = ON");
} catch {}

// ---------------------------
// Histórico (já existia)
// ---------------------------
db.exec(`
  CREATE TABLE IF NOT EXISTS historico_execucoes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- 🔹 ADICIONADO para separar histórico por usuário (sem quebrar nada)
    usuarioEmail TEXT,
    usuarioNome TEXT,

    empresaId TEXT,
    empresaNome TEXT,
    tipo TEXT,                -- 'manual' | 'lote'
    dataHora TEXT,            -- ISO string
    qtdXml INTEGER,
    qtdPdf INTEGER,
    totalArquivos INTEGER,
    status TEXT,              -- 'sucesso' | 'erro' | 'parcial'
    erros TEXT,               -- string JSON
    detalhes TEXT,            -- texto livre (ex: 'Baixou emitidas de 01/10 a 31/10')
    logsJson TEXT             -- JSON com logs da execução (para "ver/copiar logs" em qualquer dispositivo)
  );
`);

// ✅ índices leves (não muda nada, só acelera listagens por usuário/empresa)
try {
  db.exec(`CREATE INDEX IF NOT EXISTS idx_hist_usuario_data ON historico_execucoes(usuarioEmail, dataHora);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_hist_empresa_data ON historico_execucoes(empresaId, dataHora);`);
} catch {}

// migração segura para bases antigas que ainda não têm logsJson
try {
  const histCols = db.prepare(`PRAGMA table_info(historico_execucoes)`).all().map((c) => c.name);
  if (!histCols.includes("logsJson")) {
    db.exec(`ALTER TABLE historico_execucoes ADD COLUMN logsJson TEXT;`);
  }
} catch {}


// ---------------------------
// ✅ Auth tables (NOVO)
// ---------------------------
export function ensureAuthTables() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      email TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      password_plain TEXT,
      owner_admin_id INTEGER,
      role TEXT NOT NULL DEFAULT 'USER',     -- 'ADMIN' | 'USER'
      is_active INTEGER NOT NULL DEFAULT 1,  -- 1 ativo, 0 inativo
      company_name TEXT,
      cnpj TEXT,
      whatsapp TEXT,
      plan TEXT NOT NULL DEFAULT 'ESSENCIAL',
      plan_value REAL NOT NULL DEFAULT 49.9,
      created_at TEXT NOT NULL,
      last_login_at TEXT
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS sessions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      token_hash TEXT UNIQUE NOT NULL,
      created_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
    );
  `);

  try {
    db.exec(`CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_sessions_exp ON sessions(expires_at);`);
  } catch {}
}

// ---------------------------
// ✅ Seed admins (NOVO)
// - cria Ju e Ronaldo como ADMIN se não existirem
// - gera senha temporária e imprime no console na 1ª vez
// ---------------------------
export function ensureSeedAdmins({ bcryptHashFn }) {
  // evita circularidade: passamos o hash fn por parâmetro
  const now = new Date().toISOString();

  const getByEmail = db.prepare(`SELECT id, email, role FROM users WHERE email = ?`);
  const insertUser = db.prepare(`
    INSERT INTO users (name, email, password_hash, password_plain, role, is_active, created_at, owner_admin_id)
    VALUES (?, ?, ?, ?, ?, 1, ?, NULL)
  `);

  const admins = [
    { name: "Ronaldo", email: "Ronaldo@brasilprice.com.br", role: "ADMIN" },
    { name: "Ju", email: "jussilene.valim@gmail.com", role: "ADMIN" },
  ];

  const created = [];

  for (const a of admins) {
    const exists = getByEmail.get(a.email);
    if (!exists) {
      const tempPass =
        (process.env.AUTH_SEED_TEMP_PASSWORD || "").trim() ||
        `Tmp@${Math.random().toString(36).slice(2, 8)}${Math.random().toString(36).slice(2, 6)}`;

      const hash = bcryptHashFn(tempPass);

      const info = insertUser.run(a.name, a.email, hash, tempPass, a.role, now);
      db.prepare(`UPDATE users SET owner_admin_id = id WHERE id = ?`).run(info.lastInsertRowid);

      created.push({ email: a.email, tempPass });
    } else {
      // se existir e não for admin, promove (sem alterar senha)
      if (String(exists.role || "").toUpperCase() !== "ADMIN") {
        db.prepare(`UPDATE users SET role = 'ADMIN' WHERE email = ?`).run(a.email);
      }
    }
  }

  if (created.length) {
    console.log("===================================================");
    console.log("✅ ADMINS CRIADOS/ATUALIZADOS");
    console.log("⚠️ Senhas temporárias (troque em Configurações):");
    created.forEach((c) => console.log(`- ${c.email}  |  senha: ${c.tempPass}`));
    console.log("===================================================");
  }
}

export default db;
// ---------------------------
// ✅ Auth tables (users + sessions)
// ---------------------------
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    password_plain TEXT,
    owner_admin_id INTEGER,
    role TEXT NOT NULL DEFAULT 'USER',
    is_active INTEGER NOT NULL DEFAULT 1,
    company_name TEXT,
    cnpj TEXT,
    whatsapp TEXT,
    plan TEXT NOT NULL DEFAULT 'ESSENCIAL',
    plan_value REAL NOT NULL DEFAULT 49.9,
    last_login_at TEXT,
    created_at TEXT NOT NULL
  );
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
  );
`);

try {
  db.exec(`CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_sessions_exp ON sessions(expires_at);`);
} catch {}

try {
  db.exec(`ALTER TABLE users ADD COLUMN company_name TEXT;`);
} catch {}
try {
  db.exec(`ALTER TABLE users ADD COLUMN cnpj TEXT;`);
} catch {}
try {
  db.exec(`ALTER TABLE users ADD COLUMN whatsapp TEXT;`);
} catch {}
try {
  db.exec(`ALTER TABLE users ADD COLUMN plan TEXT NOT NULL DEFAULT 'ESSENCIAL';`);
} catch {}
try {
  db.exec(`ALTER TABLE users ADD COLUMN plan_value REAL NOT NULL DEFAULT 49.9;`);
} catch {}
try {
  db.exec(`ALTER TABLE users ADD COLUMN password_plain TEXT;`);
} catch {}
try {
  db.exec(`ALTER TABLE users ADD COLUMN owner_admin_id INTEGER;`);
} catch {}

// Normaliza escopo administrativo (multi-tenant de admins):
// - Admin sem owner_admin_id vira dono dele mesmo
// - Usuários sem owner_admin_id herdam o admin raiz (JVR) ou o primeiro admin existente
try {
  db.prepare(`UPDATE users SET owner_admin_id = id WHERE upper(trim(role)) = 'ADMIN' AND owner_admin_id IS NULL`).run();
  const rootAdmin =
    db.prepare(`SELECT id FROM users WHERE lower(trim(email)) = lower(trim(?)) LIMIT 1`).get("jussilene.valim@gmail.com") ||
    db.prepare(`SELECT id FROM users WHERE upper(trim(role)) = 'ADMIN' ORDER BY id ASC LIMIT 1`).get();
  if (rootAdmin?.id) {
    db.prepare(`
      UPDATE users
      SET owner_admin_id = ?
      WHERE (upper(trim(role)) <> 'ADMIN' OR role IS NULL) AND owner_admin_id IS NULL
    `).run(rootAdmin.id);
  }
} catch {}
