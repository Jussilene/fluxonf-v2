// src/auth/seedAdmins.js
import "dotenv/config";
import db from "../db/sqlite.js";
import { hashPassword } from "./password.js";

function parseSeedAdmins() {
  const raw = String(process.env.AUTH_SEED_ADMINS || "").trim();
  if (!raw) return [];

  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (err) {
    console.error("[seed] AUTH_SEED_ADMINS inválido. Use JSON array.", err?.message || err);
    return [];
  }
}

async function ensureAdmin({ name, email, password }) {
  const e = String(email).trim().toLowerCase();
  const pw = String(password || "").trim();
  if (!e || !pw) return;

  const existing = db.prepare(`SELECT id, role FROM users WHERE email = ?`).get(e);

  if (existing) {
    // se já existe, garante ADMIN e ativo
    db.prepare(`UPDATE users SET role = 'ADMIN', is_active = 1, owner_admin_id = COALESCE(owner_admin_id, id) WHERE id = ?`).run(existing.id);
    console.log(`[seed] Admin garantido: ${e}`);
    return;
  }

  const passHash = await hashPassword(pw);

  db.prepare(`
    INSERT INTO users (name, email, password_hash, password_plain, role, is_active, created_at, owner_admin_id)
    VALUES (?, ?, ?, ?, 'ADMIN', 1, ?, NULL)
  `).run(String(name || e).trim(), e, passHash, pw, new Date().toISOString());
  db.prepare(`UPDATE users SET owner_admin_id = id WHERE email = ?`).run(e);

  console.log(`[seed] Admin criado: ${e}`);
}

async function main() {
  const admins = parseSeedAdmins();
  if (!admins.length) {
    console.log("[seed] Nenhum admin configurado em AUTH_SEED_ADMINS.");
    return;
  }

  for (const admin of admins) {
    await ensureAdmin(admin);
  }
  console.log("[seed] OK");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
