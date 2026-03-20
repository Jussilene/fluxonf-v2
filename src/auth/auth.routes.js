// src/auth/auth.routes.js
import express from "express";
import db from "../db/sqlite.js";
import { verifyPassword, hashPassword } from "./password.js";
import { createSession, deleteSessionByToken } from "./session.store.js";
import { requireAuth } from "./auth.middleware.js";

const router = express.Router();

function resolveRegistrationOwnerAdminId() {
  const preferredAdminEmail = "jvr.solucoes8@gmail.com";
  const preferredAdmin = db
    .prepare(`SELECT id FROM users WHERE lower(trim(email)) = lower(trim(?)) AND upper(trim(role)) = 'ADMIN' LIMIT 1`)
    .get(preferredAdminEmail);

  if (preferredAdmin?.id) return Number(preferredAdmin.id);

  const fallbackAdmin = db
    .prepare(`SELECT id FROM users WHERE upper(trim(role)) = 'ADMIN' ORDER BY id ASC LIMIT 1`)
    .get();

  return Number(fallbackAdmin?.id || 0) || null;
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

function normalizePlanForResponse(plan, planValue, role = "") {
  if (String(role || "").trim().toUpperCase() === "ADMIN") {
    return { plan: null, plan_value: null };
  }

  const p = String(plan || "").trim().toUpperCase();
  const v = Number(planValue || 0);

  if (p === "LANCAMENTO") return { plan: "STARTER", plan_value: 49.9 };
  if (p === "STARTER") return { plan: "STARTER", plan_value: 49.9 };
  if (p === "PRO") return { plan: "EMPRESARIAL", plan_value: 147.0 };

  if (p === "FUNDADORES") return { plan: "STARTER", plan_value: v || 49.9 };
  if (p === "ESSENCIAL") return { plan: "ESSENCIAL", plan_value: v || 49.9 };
  if (p === "PROFISSIONAL") return { plan: "PROFISSIONAL", plan_value: v || 97.0 };
  if (p === "EMPRESARIAL") return { plan: "EMPRESARIAL", plan_value: v || 147.0 };

  return { plan: "ESSENCIAL", plan_value: 49.9 };
}

function cookieOpts(req) {
  // Se estiver atrás de proxy (nginx / load balancer), ele envia x-forwarded-proto
  const xfProto = String(req?.headers?.["x-forwarded-proto"] || "")
    .split(",")[0]
    .trim()
    .toLowerCase();

  const isHttps = Boolean(req?.secure) || xfProto === "https";

  return {
    httpOnly: true,
    sameSite: "lax",
    secure: isHttps, // ✅ só liga Secure quando for HTTPS de verdade
    path: "/",
  };
}

// POST /auth/login
router.post("/login", async (req, res) => {
  const { email, password } = req.body || {};
  if (!email || !password) {
    return res.status(400).json({ ok: false, error: "Email e senha são obrigatórios" });
  }

  const normalizedEmail = String(email).trim().toLowerCase();

  const user = db
    .prepare(`SELECT id, name, email, role, owner_admin_id, is_active, password_hash, company_name, cnpj, whatsapp, plan, plan_value, created_at FROM users WHERE email = ?`)
    .get(normalizedEmail);

  if (!user || !user.is_active) {
    return res.status(401).json({ ok: false, error: "Credenciais inválidas" });
  }

  const ok = await verifyPassword(password, user.password_hash);
  if (!ok) {
    return res.status(401).json({ ok: false, error: "Credenciais inválidas" });
  }

  const { token } = createSession(user.id);

  db.prepare(`UPDATE users SET last_login_at = ? WHERE id = ?`)
    .run(new Date().toISOString(), user.id);

  // ✅ importante: cookie Secure só em HTTPS real
  res.cookie("nfse_session", token, cookieOpts(req));

  const normalizedPlan = normalizePlanForResponse(user.plan, user.plan_value, user.role);

  return res.json({
    ok: true,
    user: {
      id: user.id,
      name: user.name,
      email: user.email,
      role: user.role,
      company_name: user.company_name || "",
      cnpj: user.cnpj || "",
      whatsapp: user.whatsapp || "",
      plan: normalizedPlan.plan,
      plan_value: normalizedPlan.plan_value,
      created_at: user.created_at || null,
    },
  });
});

// POST /auth/register-access
router.post("/register-access", async (req, res) => {
  const {
    company_name,
    cnpj,
    name,
    responsible,
    email,
    whatsapp,
    password,
    confirmPassword,
  } = req.body || {};

  const normalizedCompanyName = String(company_name || "").trim();
  const normalizedCnpj = formatCnpj(cnpj);
  const normalizedName = String(name || "").trim();
  const normalizedResponsible = String(responsible || "").trim();
  const normalizedEmail = String(email || "").trim().toLowerCase();
  const normalizedWhatsapp = formatPhoneNumber(whatsapp);
  const normalizedPassword = String(password || "");
  const normalizedConfirmPassword = String(confirmPassword || "");

  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

  if (!normalizedCompanyName || !normalizedCnpj || !normalizedName || !normalizedEmail || !normalizedWhatsapp || !normalizedPassword || !normalizedConfirmPassword) {
    return res.status(400).json({ ok: false, error: "Preencha razão social, CNPJ, nome, e-mail, celular, senha e confirmação." });
  }

  if (!emailRegex.test(normalizedEmail)) {
    return res.status(400).json({ ok: false, error: "Informe um e-mail válido." });
  }

  if (String(normalizedCnpj || "").replace(/\D/g, "").length !== 14) {
    return res.status(400).json({ ok: false, error: "Informe um CNPJ válido." });
  }

  if (normalizedPassword.length < 6) {
    return res.status(400).json({ ok: false, error: "A senha deve ter pelo menos 6 caracteres." });
  }

  if (normalizedPassword !== normalizedConfirmPassword) {
    return res.status(400).json({ ok: false, error: "A confirmação de senha não confere." });
  }

  const existingUser = db.prepare(`SELECT id FROM users WHERE email = ?`).get(normalizedEmail);
  if (existingUser) {
    return res.status(409).json({ ok: false, error: "Já existe um cadastro com este e-mail." });
  }

  const ownerAdminId = resolveRegistrationOwnerAdminId();
  if (!ownerAdminId) {
    return res.status(500).json({ ok: false, error: "Não foi possível identificar o administrador responsável." });
  }

  const passwordHash = await hashPassword(normalizedPassword);
  const createdAt = new Date().toISOString();

  const info = db.prepare(`
    INSERT INTO users (
      name,
      email,
      password_hash,
      password_plain,
      role,
      is_active,
      company_name,
      cnpj,
      whatsapp,
      plan,
      plan_value,
      created_at,
      owner_admin_id
    )
    VALUES (?, ?, ?, ?, 'USER', 1, ?, ?, ?, 'STARTER', 49.9, ?, ?)
  `).run(
    normalizedResponsible || normalizedName,
    normalizedEmail,
    passwordHash,
    normalizedPassword,
    normalizedCompanyName,
    normalizedCnpj,
    normalizedWhatsapp,
    createdAt,
    ownerAdminId
  );

  return res.status(201).json({
    ok: true,
    user: {
      id: info.lastInsertRowid,
      name: normalizedResponsible || normalizedName,
      email: normalizedEmail,
      company_name: normalizedCompanyName,
      cnpj: normalizedCnpj,
      whatsapp: normalizedWhatsapp,
      role: "USER",
      plan: "STARTER",
      owner_admin_id: ownerAdminId,
    },
    redirectTo: "/index.html",
    message: "Cadastro concluído com sucesso. Faça seu login para acessar o FluxoNF.",
  });
});

// POST /auth/logout
router.post("/logout", (req, res) => {
  const token = req.cookies?.nfse_session || "";
  if (token) deleteSessionByToken(token);

  // ✅ limpa com os mesmos atributos
  res.clearCookie("nfse_session", cookieOpts(req));

  return res.json({ ok: true });
});

// GET /auth/me
router.get("/me", requireAuth, (req, res) => {
  const normalizedPlan = normalizePlanForResponse(req.user?.plan, req.user?.plan_value, req.user?.role);
  return res.json({
    ok: true,
    user: {
      ...req.user,
      plan: normalizedPlan.plan,
      plan_value: normalizedPlan.plan_value,
    },
  });
});

// POST /auth/update-profile  (nome/email)
router.post("/update-profile", requireAuth, (req, res) => {
  const { name, email } = req.body || {};
  const newName = String(name || "").trim();
  const newEmail = String(email || "").trim().toLowerCase();

  if (!newName || !newEmail) {
    return res.status(400).json({ ok: false, error: "Nome e email são obrigatórios" });
  }

  // evita duplicar email
  const exists = db.prepare(`SELECT id FROM users WHERE email = ? AND id <> ?`).get(newEmail, req.user.id);
  if (exists) {
    return res.status(409).json({ ok: false, error: "Esse email já está em uso" });
  }

  db.prepare(`UPDATE users SET name = ?, email = ? WHERE id = ?`).run(newName, newEmail, req.user.id);
  const updated = db.prepare(`
    SELECT id, name, email, role, is_active, last_login_at, company_name, cnpj, whatsapp, plan, plan_value, created_at
    FROM users WHERE id = ?
  `).get(req.user.id);

  res.set("Cache-Control", "no-store");
  return res.json({ ok: true, user: updated || null });
});

// POST /auth/change-password
router.post("/change-password", requireAuth, async (req, res) => {
  const { newPassword } = req.body || {};
  const pw = String(newPassword || "");

  if (!pw || pw.length < 6) {
    return res.status(400).json({ ok: false, error: "Senha inválida (mín. 6 caracteres)" });
  }

  const hash = await hashPassword(pw);
  const id = Number(req.user?.id);
  const email = String(req.user?.email || "").trim().toLowerCase();
  let info = { changes: 0 };

  if (Number.isFinite(id)) {
    info = db.prepare(`UPDATE users SET password_hash = ?, password_plain = ? WHERE id = ?`).run(hash, pw, id);
  }
  if ((!info?.changes || info.changes < 1) && email) {
    info = db.prepare(`UPDATE users SET password_hash = ?, password_plain = ? WHERE lower(trim(email)) = lower(trim(?))`).run(hash, pw, email);
  }

  res.set("Cache-Control", "no-store");
  return res.json({ ok: true, password_plain: pw, updated: Number(info?.changes || 0) });
});

export default router;
