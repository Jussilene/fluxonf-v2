// src/utils/usersStore.js
import fs from "fs";
import path from "path";
import crypto from "crypto";

// ✅ NOVO: usa o MESMO SQLite do login (sem mudar login)
import db from "../db/sqlite.js";
// ✅ NOVO: usa o MESMO hash/verify do seu auth
import { hashPassword } from "../auth/password.js";

const ROOT = process.cwd();
const DATA_DIR = path.join(ROOT, "data");
const USERS_FILE = path.join(DATA_DIR, "users.json");

// =========================================================
// 1) Tenta usar um "store" de usuários existente no projeto
//    (pra não quebrar seu login / hash atual).
// =========================================================
async function tryLoadExistingAuthStore() {
  const candidates = [
    path.join(ROOT, "src", "auth", "users.store.js"),
    path.join(ROOT, "src", "auth", "usersStore.js"),
    path.join(ROOT, "src", "auth", "user.store.js"),
    path.join(ROOT, "src", "auth", "userStore.js"),
    path.join(ROOT, "src", "utils", "userStore.js"),
  ];

  for (const full of candidates) {
    if (fs.existsSync(full)) {
      const mod = await import(pathToFileUrl(full));
      return mod;
    }
  }

  return null;
}

function pathToFileUrl(p) {
  const resolved = path.resolve(p);
  const url = new URL("file:///");
  url.pathname = resolved.replace(/\\/g, "/");
  return url.href;
}

// =========================================================
// 2) Fallback JSON store (caso não exista store real)
// =========================================================
function ensureUsersFile() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(USERS_FILE)) {
    fs.writeFileSync(USERS_FILE, JSON.stringify({ users: [] }, null, 2), "utf8");
  }
}

function readUsersDb() {
  ensureUsersFile();
  try {
    const raw = fs.readFileSync(USERS_FILE, "utf8");
    const parsed = JSON.parse(raw || "{}");
    if (!parsed || typeof parsed !== "object") return { users: [] };
    if (!Array.isArray(parsed.users)) parsed.users = [];
    return parsed;
  } catch {
    return { users: [] };
  }
}

function writeUsersDb(dbJson) {
  ensureUsersFile();
  fs.writeFileSync(USERS_FILE, JSON.stringify(dbJson, null, 2), "utf8");
}

// =========================================================
// 3) Hash simples (fallback) – NÃO muda seu auth existente.
//    Só serve se você ainda não tem auth real.
// =========================================================
function hashPasswordFallback(password, salt = crypto.randomBytes(16).toString("hex")) {
  const iter = 120000;
  const keylen = 32;
  const digest = "sha256";
  const derived = crypto.pbkdf2Sync(password, salt, iter, keylen, digest).toString("hex");
  return `pbkdf2$${digest}$${iter}$${salt}$${derived}`;
}

// =========================================================
// 4) API que o passwordReset.routes.js precisa
// =========================================================
let _existingStorePromise = null;
function getExistingStore() {
  if (!_existingStorePromise) _existingStorePromise = tryLoadExistingAuthStore();
  return _existingStorePromise;
}

/**
 * Busca usuário por email.
 * ✅ AGORA: primeiro tenta SQLite (users), que é o que seu /auth/login usa.
 * Retorna objeto user ou null.
 */
export async function findUserByEmail(email) {
  const e = String(email || "").trim().toLowerCase();
  if (!e) return null;

  // ✅ 1) SQLite (fonte principal do seu auth atual)
  try {
    const user = db
      .prepare(`SELECT id, name, email, role, is_active FROM users WHERE email = ?`)
      .get(e);

    if (user) return user;
  } catch (err) {
    // não quebra o app se sqlite tiver qualquer problema
    console.error("[usersStore] sqlite findUserByEmail error:", err?.message || err);
  }

  // 2) store existente (se houver)
  const existing = await getExistingStore();
  if (existing) {
    if (typeof existing.findUserByEmail === "function") return existing.findUserByEmail(e);
    if (typeof existing.getUserByEmail === "function") return existing.getUserByEmail(e);
    if (typeof existing.findByEmail === "function") return existing.findByEmail(e);
  }

  // 3) fallback json
  const dbJson = readUsersDb();
  return dbJson.users.find((u) => String(u.email || "").toLowerCase() === e) || null;
}

/**
 * ✅ NOVO: Cria usuário no SQLite como "user" (não ADM).
 * - Se já existir, retorna o existente.
 * - Se existir e estiver inativo, pode reativar via setUserActiveByEmail.
 *
 * OBS: senha padrão pode vir por parâmetro, ou via env HOTMART_DEFAULT_PASSWORD.
 */
export async function createUser({ email, name = "", role = "user", password = "", plan = "STARTER", planValue = 49.9 } = {}) {
  const e = String(email || "").trim().toLowerCase();
  if (!e) return null;

  const pw =
    String(password || "").trim() ||
    String(process.env.HOTMART_DEFAULT_PASSWORD || "").trim() ||
    "123456";

  // ✅ 1) SQLite (preferencial)
  try {
    const existing = db
      .prepare(`SELECT id, name, email, role, is_active FROM users WHERE email = ?`)
      .get(e);

    if (existing) return existing;

    const password_hash = await hashPassword(pw);

    const finalName = String(name || "").trim() || "Cliente Hotmart";

    const rootAdmin =
      db.prepare(`SELECT id FROM users WHERE lower(trim(email)) = lower(trim(?)) LIMIT 1`).get("jussilene.valim@gmail.com") ||
      db.prepare(`SELECT id FROM users WHERE upper(trim(role)) = 'ADMIN' ORDER BY id ASC LIMIT 1`).get();
    const ownerAdminId = Number(rootAdmin?.id || 0) || null;

    const normalizedPlan = String(plan || "STARTER").trim().toUpperCase() || "STARTER";
    const normalizedPlanValue = Number(planValue || 49.9) || 49.9;

    const info = db
      .prepare(
        `INSERT INTO users (name, email, role, is_active, password_hash, password_plain, created_at, owner_admin_id, plan, plan_value)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(finalName, e, role, 1, password_hash, pw, new Date().toISOString(), ownerAdminId, normalizedPlan, normalizedPlanValue);

    return {
      id: info.lastInsertRowid,
      name: finalName,
      email: e,
      role,
      is_active: 1,
    };
  } catch (err) {
    console.error("[usersStore] sqlite createUser error:", err?.message || err);
  }

  // 2) store existente (se houver)
  const existingStore = await getExistingStore();
  if (existingStore) {
    if (typeof existingStore.createUser === "function") return existingStore.createUser({ email: e, name, role, password: pw });
    if (typeof existingStore.addUser === "function") return existingStore.addUser({ email: e, name, role, password: pw });
  }

  // 3) fallback json
  const dbJson = readUsersDb();

  const exists = dbJson.users.find((u) => String(u.email || "").toLowerCase() === e);
  if (exists) return exists;

  const newUser = {
    id: String(Date.now()),
    name: String(name || "").trim() || "Cliente Hotmart",
    email: e,
    role,
    is_active: 1,
    passwordHash: hashPasswordFallback(pw),
    createdAt: Date.now(),
    source: "hotmart",
  };

  dbJson.users.push(newUser);
  writeUsersDb(dbJson);

  return newUser;
}

/**
 * ✅ NOVO: Ativa/desativa usuário no SQLite (bloqueia login pelo is_active)
 */
export async function setUserActiveByEmail(email, active) {
  const e = String(email || "").trim().toLowerCase();
  if (!e) return null;

  const a = active ? 1 : 0;

  // ✅ 1) SQLite
  try {
    const user = db.prepare(`SELECT id FROM users WHERE email = ?`).get(e);
    if (!user?.id) return null;

    db.prepare(`UPDATE users SET is_active = ? WHERE email = ?`).run(a, e);

    return db
      .prepare(`SELECT id, name, email, role, is_active FROM users WHERE email = ?`)
      .get(e);
  } catch (err) {
    console.error("[usersStore] sqlite setUserActiveByEmail error:", err?.message || err);
  }

  // 2) store existente (se houver)
  const existing = await getExistingStore();
  if (existing) {
    if (typeof existing.setUserActiveByEmail === "function") return existing.setUserActiveByEmail(e, !!active);
    if (typeof existing.setActiveByEmail === "function") return existing.setActiveByEmail(e, !!active);
  }

  // 3) fallback json
  const dbJson = readUsersDb();
  const idx = dbJson.users.findIndex((u) => String(u.email || "").toLowerCase() === e);
  if (idx === -1) return null;

  dbJson.users[idx].is_active = a;
  writeUsersDb(dbJson);

  return dbJson.users[idx];
}

/**
 * Atualiza senha do usuário (por email) e limpa token.
 * ✅ AGORA: atualiza password_hash no SQLite usando o MESMO hash do auth.
 */
export async function updatePasswordByEmail(email, newPassword) {
  const e = String(email || "").trim().toLowerCase();
  if (!e) return false;

  // ✅ 1) SQLite (fonte principal do seu auth atual)
  try {
    const user = db.prepare(`SELECT id FROM users WHERE email = ?`).get(e);
    if (user?.id) {
      const pw = String(newPassword || "");
      if (!pw || pw.length < 6) return false;

      const newHash = await hashPassword(pw);
      db.prepare(`UPDATE users SET password_hash = ?, password_plain = ? WHERE email = ?`).run(newHash, pw, e);
      return true;
    }
  } catch (err) {
    console.error("[usersStore] sqlite updatePasswordByEmail error:", err?.message || err);
  }

  // 2) store existente (se houver)
  const existing = await getExistingStore();
  if (existing) {
    if (typeof existing.updatePasswordByEmail === "function") return existing.updatePasswordByEmail(e, newPassword);
    if (typeof existing.setUserPassword === "function") return existing.setUserPassword(e, newPassword);
    if (typeof existing.updateUserPassword === "function") return existing.updateUserPassword(e, newPassword);
  }

  // 3) fallback json
  const dbJson = readUsersDb();
  const idx = dbJson.users.findIndex((u) => String(u.email || "").toLowerCase() === e);
  if (idx === -1) return false;

  dbJson.users[idx].passwordHash = hashPasswordFallback(String(newPassword || ""));
  dbJson.users[idx].resetToken = "";
  dbJson.users[idx].resetTokenExpiresAt = 0;
  writeUsersDb(dbJson);
  return true;
}

/**
 * As funções abaixo ficaram como estavam (fallback / compat),
 * mesmo que você hoje use resetTokensStore separado.
 */

export async function setResetTokenByEmail(email, token, expiresAtMs) {
  const e = String(email || "").trim().toLowerCase();
  if (!e) return false;

  const existing = await getExistingStore();
  if (existing) {
    if (typeof existing.setResetTokenByEmail === "function") return existing.setResetTokenByEmail(e, token, expiresAtMs);
    if (typeof existing.setPasswordResetToken === "function") return existing.setPasswordResetToken(e, token, expiresAtMs);
  }

  const dbJson = readUsersDb();
  const idx = dbJson.users.findIndex((u) => String(u.email || "").toLowerCase() === e);
  if (idx === -1) return false;

  dbJson.users[idx].resetToken = token;
  dbJson.users[idx].resetTokenExpiresAt = expiresAtMs;
  writeUsersDb(dbJson);
  return true;
}

export async function findUserByResetToken(token) {
  const t = String(token || "").trim();
  if (!t) return null;

  const existing = await getExistingStore();
  if (existing) {
    if (typeof existing.findUserByResetToken === "function") return existing.findUserByResetToken(t);
    if (typeof existing.getUserByResetToken === "function") return existing.getUserByResetToken(t);
  }

  const dbJson = readUsersDb();
  const u = dbJson.users.find((u) => String(u.resetToken || "") === t);
  if (!u) return null;

  const exp = Number(u.resetTokenExpiresAt || 0);
  if (!exp || Date.now() > exp) return null;

  return u;
}

export async function clearResetTokenByEmail(email) {
  const e = String(email || "").trim().toLowerCase();
  if (!e) return false;

  const existing = await getExistingStore();
  if (existing) {
    if (typeof existing.clearResetTokenByEmail === "function") return existing.clearResetTokenByEmail(e);
    if (typeof existing.clearPasswordResetToken === "function") return existing.clearPasswordResetToken(e);
  }

  const dbJson = readUsersDb();
  const idx = dbJson.users.findIndex((u) => String(u.email || "").toLowerCase() === e);
  if (idx === -1) return false;

  dbJson.users[idx].resetToken = "";
  dbJson.users[idx].resetTokenExpiresAt = 0;
  writeUsersDb(dbJson);
  return true;
}
