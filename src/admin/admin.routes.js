// src/admin/admin.routes.js
import express from "express";
import db from "../db/sqlite.js";
import { requireAuth, requireAdmin } from "../auth/auth.middleware.js";
import { hashPassword } from "../auth/password.js";

const router = express.Router();

// tudo aqui exige estar logado + ser ADMIN
router.use(requireAuth, requireAdmin);

function normalizePlan(input) {
  const p = String(input || "ESSENCIAL").trim().toUpperCase();

  // compatibilidade com planos antigos
  if (p === "LANCAMENTO") return { plan: "STARTER", plan_value: 49.9 };
  if (p === "STARTER") return { plan: "STARTER", plan_value: 49.9 };
  if (p === "PRO") return { plan: "EMPRESARIAL", plan_value: 147.0 };

  if (p === "FUNDADORES") return { plan: "STARTER", plan_value: 49.9 };
  if (p === "PROFISSIONAL") return { plan: "PROFISSIONAL", plan_value: 97.0 };
  if (p === "EMPRESARIAL") return { plan: "EMPRESARIAL", plan_value: 147.0 };
  return { plan: "ESSENCIAL", plan_value: 49.9 };
}

function formatPhoneNumber(value) {
  const digits = String(value || "").replace(/\D/g, "").slice(0, 11);
  if (!digits) return "";
  if (digits.length <= 2) return `(${digits}`;
  if (digits.length <= 6) return `(${digits.slice(0, 2)}) ${digits.slice(2)}`;
  if (digits.length <= 10) return `(${digits.slice(0, 2)}) ${digits.slice(2, 6)}-${digits.slice(6)}`;
  return `(${digits.slice(0, 2)}) ${digits.slice(2, 7)}-${digits.slice(7, 11)}`;
}

function formatCnpj(value) {
  const digits = String(value || "").replace(/\D/g, "").slice(0, 14);
  if (!digits) return "";
  if (digits.length <= 2) return digits;
  if (digits.length <= 5) return `${digits.slice(0, 2)}.${digits.slice(2)}`;
  if (digits.length <= 8) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5)}`;
  if (digits.length <= 12) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8)}`;
  return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12, 14)}`;
}

function adminScopeId(req) {
  const scope = Number(req.user?.id);
  return Number.isFinite(scope) ? scope : null;
}

// GET /admin/users
router.get("/users", (req, res) => {
  const scopeId = adminScopeId(req);
  if (!scopeId) return res.status(403).json({ ok: false, error: "Escopo administrativo inválido" });
  res.set("Cache-Control", "no-store");
  const users = db.prepare(`
    SELECT id, name, email, role, is_active, last_login_at, created_at, company_name, cnpj, whatsapp, plan, plan_value, password_plain
    FROM users
    WHERE owner_admin_id = ? OR id = ?
    ORDER BY created_at DESC
  `).all(scopeId, scopeId);

  const totals = db.prepare(`
    SELECT
      SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active,
      SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END) AS inactive,
      COUNT(*) AS total
    FROM users
    WHERE owner_admin_id = ? OR id = ?
  `).get(scopeId, scopeId);

  return res.json({ ok: true, users, totals });
});

// POST /admin/users  (criar usuário)
router.post("/users", async (req, res) => {
  const scopeId = adminScopeId(req);
  if (!scopeId) return res.status(403).json({ ok: false, error: "Escopo administrativo inválido" });
  const { name, email, password, role, company_name, cnpj, whatsapp, plan } = req.body || {};

  const n = String(name || "").trim();
  const e = String(email || "").trim().toLowerCase();
  const p = String(password || "");
  const r = String(role || "USER").toUpperCase();
  const company = String(company_name || "").trim();
  const cnpjValue = formatCnpj(cnpj);
  const whatsappValue = formatPhoneNumber(whatsapp);
  const normalizedPlan = normalizePlan(plan);

  if (!n || !e || !p || !company || !cnpjValue || !whatsappValue || !plan || !r) {
    return res.status(400).json({ ok: false, error: "Para criar usuário, preencha todos os campos obrigatórios." });
  }

  if (p.length < 6) {
    return res.status(400).json({ ok: false, error: "Senha muito curta (mín. 6)" });
  }

  if (!["USER", "ADMIN"].includes(r)) {
    return res.status(400).json({ ok: false, error: "Role inválida" });
  }

  const exists = db.prepare(`SELECT id FROM users WHERE email = ?`).get(e);
  if (exists) {
    return res.status(409).json({ ok: false, error: "Já existe usuário com esse email" });
  }

  const passHash = await hashPassword(p);
  const ownerAdminId = scopeId;

  const info = db.prepare(`
    INSERT INTO users (name, email, password_hash, password_plain, role, is_active, created_at, company_name, cnpj, whatsapp, plan, plan_value, owner_admin_id)
    VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
  `).run(n, e, passHash, p, r, new Date().toISOString(), company, cnpjValue, whatsappValue, normalizedPlan.plan, normalizedPlan.plan_value, ownerAdminId);

  const createdUser = db.prepare(`
    SELECT id, name, email, role, company_name, cnpj, whatsapp, plan, plan_value, password_plain, is_active, created_at
    FROM users WHERE id = ?
  `).get(info.lastInsertRowid);

  return res.status(201).json({ ok: true, id: info.lastInsertRowid, user: createdUser });
});

// PUT /admin/users/:id  (editar usuário)
router.put("/users/:id", (req, res) => {
  const scopeId = adminScopeId(req);
  if (!scopeId) return res.status(403).json({ ok: false, error: "Escopo administrativo inválido" });
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ ok: false, error: "ID inválido" });

  const { name, email, role, company_name, cnpj, whatsapp, plan, password } = req.body || {};
  const u = db.prepare(`
    SELECT id, name, email, role, owner_admin_id, company_name, cnpj, whatsapp, plan
    FROM users
    WHERE id = ? AND (owner_admin_id = ? OR id = ?)
  `).get(id, scopeId, scopeId);
  if (!u) return res.status(404).json({ ok: false, error: "Usuário não encontrado" });

  const e = String(email || "").trim().toLowerCase();
  if (!e) {
    return res.status(400).json({ ok: false, error: "Na edição, o email é obrigatório." });
  }

  const n = String(name ?? u.name ?? "").trim() || String(u.name || "");
  const r = String(role ?? u.role ?? "USER").toUpperCase();
  if (!["USER", "ADMIN"].includes(r)) {
    return res.status(400).json({ ok: false, error: "Role inválida" });
  }

  const company = company_name === undefined ? String(u.company_name || "") : String(company_name || "").trim();
  const cnpjValue = cnpj === undefined ? formatCnpj(u.cnpj || "") : formatCnpj(cnpj);
  const whatsappValue = whatsapp === undefined ? formatPhoneNumber(u.whatsapp || "") : formatPhoneNumber(whatsapp);
  const normalizedPlan = normalizePlan(plan === undefined ? u.plan : plan);
  const nextOwnerAdminId = Number(u.owner_admin_id || scopeId);

  const existsEmail = db.prepare(`SELECT id FROM users WHERE email = ? AND id <> ?`).get(e, id);
  if (existsEmail) {
    return res.status(409).json({ ok: false, error: "Já existe usuário com esse email" });
  }

  const pass = String(password || "").trim();
  if (pass) {
    if (pass.length < 6) {
      return res.status(400).json({ ok: false, error: "Senha muito curta (mín. 6)" });
    }
    hashPassword(pass).then((hash) => {
      db.prepare(`
        UPDATE users
        SET name = ?, email = ?, role = ?, company_name = ?, cnpj = ?, whatsapp = ?, plan = ?, plan_value = ?, password_hash = ?, password_plain = ?, owner_admin_id = ?
        WHERE id = ? AND (owner_admin_id = ? OR id = ?)
      `).run(n, e, r, company, cnpjValue, whatsappValue, normalizedPlan.plan, normalizedPlan.plan_value, hash, pass, nextOwnerAdminId, id, scopeId, scopeId);
      const updatedUser = db.prepare(`
        SELECT id, name, email, role, company_name, cnpj, whatsapp, plan, plan_value, password_plain, is_active, created_at
        FROM users WHERE id = ?
      `).get(id);
      return res.json({ ok: true, user: updatedUser });
    }).catch(() => res.status(500).json({ ok: false, error: "Erro ao salvar senha" }));
    return;
  }

  db.prepare(`
    UPDATE users
    SET name = ?, email = ?, role = ?, company_name = ?, cnpj = ?, whatsapp = ?, plan = ?, plan_value = ?, owner_admin_id = ?
    WHERE id = ? AND (owner_admin_id = ? OR id = ?)
  `).run(n, e, r, company, cnpjValue, whatsappValue, normalizedPlan.plan, normalizedPlan.plan_value, nextOwnerAdminId, id, scopeId, scopeId);

  const updatedUser = db.prepare(`
    SELECT id, name, email, role, company_name, cnpj, whatsapp, plan, plan_value, password_plain, is_active, created_at
    FROM users WHERE id = ?
  `).get(id);

  return res.json({ ok: true, user: updatedUser });
});

// POST /admin/users/:id/toggle  (ativar/desativar)
router.post("/users/:id/toggle", (req, res) => {
  const scopeId = adminScopeId(req);
  if (!scopeId) return res.status(403).json({ ok: false, error: "Escopo administrativo inválido" });
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ ok: false, error: "ID inválido" });

  const u = db.prepare(`SELECT id, is_active FROM users WHERE id = ? AND (owner_admin_id = ? OR id = ?)`).get(id, scopeId, scopeId);
  if (!u) return res.status(404).json({ ok: false, error: "Usuário não encontrado" });

  const next = u.is_active ? 0 : 1;
  db.prepare(`UPDATE users SET is_active = ? WHERE id = ?`).run(next, id);

  return res.json({ ok: true, is_active: next });
});

// POST /admin/users/:id/reset-password
router.post("/users/:id/reset-password", async (req, res) => {
  const scopeId = adminScopeId(req);
  if (!scopeId) return res.status(403).json({ ok: false, error: "Escopo administrativo inválido" });
  const id = Number(req.params.id);
  const { newPassword } = req.body || {};

  if (!Number.isFinite(id)) return res.status(400).json({ ok: false, error: "ID inválido" });

  const pw = String(newPassword || "123456");
  if (!pw || pw.length < 6) {
    return res.status(400).json({ ok: false, error: "Senha inválida (mín. 6)" });
  }

  const u = db.prepare(`SELECT id FROM users WHERE id = ? AND (owner_admin_id = ? OR id = ?)`).get(id, scopeId, scopeId);
  if (!u) return res.status(404).json({ ok: false, error: "Usuário não encontrado" });

  const hash = await hashPassword(pw);
  db.prepare(`UPDATE users SET password_hash = ?, password_plain = ? WHERE id = ? AND (owner_admin_id = ? OR id = ?)`).run(hash, pw, id, scopeId, scopeId);

  const updatedUser = db.prepare(`
    SELECT id, name, email, role, company_name, cnpj, whatsapp, plan, plan_value, password_plain, is_active, created_at
    FROM users WHERE id = ?
  `).get(id);

  return res.json({ ok: true, user: updatedUser });
});

// DELETE /admin/users/:id
router.delete("/users/:id", (req, res) => {
  const scopeId = adminScopeId(req);
  if (!scopeId) return res.status(403).json({ ok: false, error: "Escopo administrativo inválido" });
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ ok: false, error: "ID inválido" });

  const currentUserId = Number(req.user?.id);
  if (Number.isFinite(currentUserId) && currentUserId === id) {
    return res.status(400).json({ ok: false, error: "Não é permitido excluir o próprio usuário" });
  }

  const u = db.prepare(`SELECT id FROM users WHERE id = ? AND (owner_admin_id = ? OR id = ?)`).get(id, scopeId, scopeId);
  if (!u) return res.status(404).json({ ok: false, error: "Usuário não encontrado" });

  db.prepare(`DELETE FROM users WHERE id = ? AND (owner_admin_id = ? OR id = ?)`).run(id, scopeId, scopeId);
  return res.json({ ok: true });
});

export default router;


