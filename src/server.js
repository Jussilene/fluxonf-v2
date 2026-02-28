// src/server.js
import "dotenv/config";

import express from "express";
import cors from "cors";
import path from "path";
import fs from "fs";
import archiver from "archiver";
import { fileURLToPath } from "url";
import cookieParser from "cookie-parser";

import { runManualDownload, runLoteDownload } from "./bot/nfseBot.js";

// ✅ store único (JSON), agora com suporte a userEmail
import { listarEmpresas, adicionarEmpresa, atualizarEmpresa, removerEmpresa } from "./utils/empresasStore.js";

// ✅ HISTÓRICO
import historicoRoutes from "./emissao/routes/historico.routes.js";

// ✅ rotas da emissão
import emissaoRoutes from "./emissao/routes/emissao.routes.js";

// ✅ garante tabela de emissão no SQLite
import { ensureNfseEmissaoTables } from "./emissao/nfseEmissao.model.js";

// ✅ Auth/Admin (NOVO) — rotas
import authRoutes from "./auth/auth.routes.js";
import adminRoutes from "./admin/admin.routes.js";
import db from "./db/sqlite.js";
import { hashPassword } from "./auth/password.js";

// ✅ Auth/Admin (NOVO) — ler usuário do cookie (sessão)
import { getSessionUser } from "./auth/session.store.js";

// ✅ NOVO (ESQUECI SENHA) — rotas
import passwordResetRoutes from "./routes/passwordReset.routes.js";

// ✅ NOVO (HOTMART WEBHOOK) — rotas (APENAS ESTA)
import hotmartWebhookRoutes from "./routes/webhooks.hotmart.routes.js";

const app = express();
const PORT = process.env.PORT || 3000;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---------------------------
// ✅ Boot: garante tabelas
// ---------------------------
ensureNfseEmissaoTables();

// ---------------------------
// Middlewares
// ---------------------------
app.use(cors());
app.use(cookieParser());
app.use(express.json({ limit: "25mb" }));
app.use(express.urlencoded({ extended: true, limit: "25mb" }));
app.use(express.static(path.join(__dirname, "..", "public")));

// ✅ Lê usuário logado pela sessão (cookie)
// - NÃO muda a lógica do sistema, só define req.user (se logado)
app.use((req, _res, next) => {
  try {
    const user = getSessionUser(req); // lê cookie nfse_session
    if (user) req.user = user;
  } catch {}
  next();
});

// ✅ Middleware multi-tenant (compat + prioridade sessão)
// - prioridade: sessão > header > body > query
app.use((req, _res, next) => {
  const sessionEmail = req.user?.email || "";

  const h = req.headers["x-user-email"];
  const headerEmail = (Array.isArray(h) ? h[0] : h) || "";

  const bodyEmail = req.body?.usuarioEmail || req.body?.userEmail || "";
  const queryEmail = req.query?.usuarioEmail || req.query?.userEmail || "";

  req.userEmail = String(sessionEmail || headerEmail || bodyEmail || queryEmail || "").trim();

  next();
});

// ---------------------------
// ✅ Rotas Auth/Admin (NOVO)
// ---------------------------
app.use("/auth", authRoutes);
app.use("/admin", adminRoutes);

// ✅ NOVO (HOTMART WEBHOOK)
// - registra /webhooks/hotmart
app.use("/webhooks", hotmartWebhookRoutes);

// ✅ NOVO (ESQUECI SENHA)
// - registra /auth/forgot-password e /auth/reset-password
app.use("/auth", passwordResetRoutes);

// ---------------------------
// Pasta pública de ZIPs
// ---------------------------
const ZIP_DIR = path.join(__dirname, "..", "public", "zips");
if (!fs.existsSync(ZIP_DIR)) {
  fs.mkdirSync(ZIP_DIR, { recursive: true });
}

// ---------------------------
// ✅ Empresas (multi-tenant via userEmail)
// ---------------------------
app.get("/api/empresas", (req, res) => {
  const userEmail = req.userEmail || "";
  const empresas = listarEmpresas(userEmail);
  return res.json({ ok: true, empresas });
});

function normalizeCapturePlanCode(rawPlan = "") {
  const p = String(rawPlan || "").trim().toUpperCase();
  if (p === "FUNDADORES") return "STARTER";
  if (p === "LANCAMENTO") return "STARTER";
  if (p === "STARTER") return "STARTER";
  if (p === "PRO") return "EMPRESARIAL";
  if (p === "ESSENCIAL" || p === "PROFISSIONAL" || p === "EMPRESARIAL") return p;
  return "";
}

function capturePlanLimit(planCode = "", role = "") {
  const r = String(role || "").trim().toUpperCase();
  if (r === "ADMIN") return Infinity;

  const p = normalizeCapturePlanCode(planCode);
  if (p === "STARTER") return Infinity;
  if (p === "ESSENCIAL") return 30;
  if (p === "PROFISSIONAL") return 100;
  if (p === "EMPRESARIAL") return 300;
  return Infinity;
}

app.post("/api/empresas", (req, res) => {
  const payload = req.body || {};
  const { nome, cnpj } = payload;
  const userEmail = req.userEmail || "";

  if (!nome || !cnpj) {
    return res.status(400).json({ ok: false, error: "Nome e CNPJ são obrigatórios." });
  }

  // Limite de empresas por plano (captura):
  // Starter ilimitado, Essencial 30, Profissional 100, Empresarial 300.
  let role = String(req.user?.role || "").trim().toUpperCase();
  let plan = String(req.user?.plan || "").trim();

  if ((!role || !plan) && userEmail) {
    const u = db
      .prepare(`SELECT role, plan FROM users WHERE lower(trim(email)) = lower(trim(?))`)
      .get(userEmail);
    role = String(role || u?.role || "").trim().toUpperCase();
    plan = String(plan || u?.plan || "").trim();
  }

  const limit = capturePlanLimit(plan, role);
  if (Number.isFinite(limit) && userEmail) {
    const totalEmpresas = listarEmpresas(userEmail).length;
    if (totalEmpresas >= limit) {
      const planCode = normalizeCapturePlanCode(plan);
      const planLabel =
        planCode === "STARTER"
          ? "Starter"
          : planCode === "EMPRESARIAL"
          ? "Empresarial"
          : planCode === "PROFISSIONAL"
          ? "Profissional"
          : "Essencial";
      return res.status(403).json({
        ok: false,
        error: `${planLabel}: limite de ${limit} empresa(s) atingido.`,
      });
    }
  }

  const nova = adicionarEmpresa({ ...payload, userEmail });

  return res.status(201).json({ ok: true, empresa: nova });
});

app.put("/api/empresas/:id", (req, res) => {
  const { id } = req.params;
  const payload = req.body || {};
  const userEmail = req.userEmail || "";

  if (!payload?.nome || !payload?.cnpj) {
    return res.status(400).json({ ok: false, error: "Nome e CNPJ são obrigatórios." });
  }

  const empresa = atualizarEmpresa(id, payload, userEmail);
  if (!empresa) {
    return res.status(404).json({ ok: false, error: "Empresa não encontrada." });
  }

  return res.json({ ok: true, empresa });
});

app.delete("/api/empresas/:id", (req, res) => {
  const { id } = req.params;
  const userEmail = req.userEmail || "";

  const ok = removerEmpresa(id, userEmail);

  if (!ok) {
    return res.status(404).json({ ok: false, error: "Empresa não encontrada." });
  }

  return res.json({ ok: true });
});

// ---------------------------
// Helper ZIP
// ---------------------------
function zipDirectory(sourceDir, zipFilePath, rootInZip = false) {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver("zip", { zlib: { level: 9 } });

    output.on("close", () => resolve());
    output.on("error", (err) => reject(err));
    archive.on("error", (err) => reject(err));

    archive.pipe(output);
    archive.directory(sourceDir, rootInZip || false);
    archive.finalize();
  });
}

function safeSlug(v = "") {
  return String(v || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\w.-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 100);
}

async function ensureCrmTestUsers() {
  const now = new Date().toISOString();
  const rootAdmin =
    db.prepare(`SELECT id FROM users WHERE lower(trim(email)) = lower(trim(?)) LIMIT 1`).get("jussilene.valim@gmail.com") ||
    db.prepare(`SELECT id FROM users WHERE upper(trim(role)) = 'ADMIN' ORDER BY id ASC LIMIT 1`).get();
  const ownerAdminId = Number(rootAdmin?.id || 0) || null;
  const tests = [
    { name: "Cliente Essencial", email: "essencial@teste.com", plan: "ESSENCIAL", plan_value: 49.9, company_name: "Cliente Essencial LTDA", cnpj: "44.444.444/0001-44", whatsapp: "(11) 94444-4444" },
    { name: "Cliente Starter", email: "lancamento@teste.com", plan: "STARTER", plan_value: 49.9, company_name: "Cliente Starter LTDA", cnpj: "11.111.111/0001-11", whatsapp: "(11) 91111-1111" },
    { name: "Cliente Profissional", email: "starter@teste.com", plan: "PROFISSIONAL", plan_value: 97.0, company_name: "Cliente Profissional LTDA", cnpj: "22.222.222/0001-22", whatsapp: "(11) 92222-2222" },
    { name: "Cliente Empresarial", email: "pro@teste.com", plan: "EMPRESARIAL", plan_value: 147.0, company_name: "Cliente Empresarial LTDA", cnpj: "33.333.333/0001-33", whatsapp: "(11) 93333-3333" },
  ];

  for (const t of tests) {
    const email = String(t.email).trim().toLowerCase();
    const existing = db.prepare(`SELECT id FROM users WHERE email = ?`).get(email);
    const passHash = await hashPassword("123456");
    if (existing?.id) {
      db.prepare(`
        UPDATE users
        SET name = ?, password_hash = ?, password_plain = ?, role = 'USER', is_active = 1, company_name = ?, cnpj = ?, whatsapp = ?, plan = ?, plan_value = ?, owner_admin_id = ?
        WHERE id = ?
      `).run(t.name, passHash, "123456", t.company_name, t.cnpj, t.whatsapp, t.plan, t.plan_value, ownerAdminId, existing.id);
      console.log(`[seed] usuário CRM atualizado: ${email} / senha 123456`);
    } else {
      db.prepare(`
        INSERT INTO users (name, email, password_hash, password_plain, role, is_active, created_at, company_name, cnpj, whatsapp, plan, plan_value, owner_admin_id)
        VALUES (?, ?, ?, ?, 'USER', 1, ?, ?, ?, ?, ?, ?, ?)
      `).run(t.name, email, passHash, "123456", now, t.company_name, t.cnpj, t.whatsapp, t.plan, t.plan_value, ownerAdminId);
      console.log(`[seed] usuário CRM criado: ${email} / senha 123456`);
    }
  }
}

// ---------------------------
// Histórico
// ---------------------------
app.use("/api/historico", historicoRoutes);

// ---------------------------
// Emissão
// ---------------------------
app.use("/api/emissao", emissaoRoutes);

// ---------------------------
// ✅ Validação de período (backend)
// ---------------------------
function assertPeriodo(req, res) {
  const { dataInicial, dataFinal } = req.body || {};
  if (!dataInicial || !dataFinal) {
    res.status(400).json({
      success: false,
      error: "Informe dataInicial e dataFinal (obrigatório).",
    });
    return false;
  }
  return true;
}

// ---------------------------
// Helpers: tipos selecionados
// ---------------------------
function normalizeTipos(processarTipos, tipoNotaFallback) {
  const allow = new Set(["emitidas", "recebidas", "canceladas"]);

  const arr = Array.isArray(processarTipos) ? processarTipos : [];
  const clean = arr.map((t) => String(t).toLowerCase()).filter((t) => allow.has(t));

  if (clean.length) return Array.from(new Set(clean));

  const t = (tipoNotaFallback || "emitidas").toLowerCase();
  return allow.has(t) ? [t] : ["emitidas"];
}

function resolveCertPathForUser({ certPfxPath, empresaId, userEmail }) {
  const raw = String(certPfxPath || "").trim();
  if (!raw) return "";

  if (fs.existsSync(raw)) return raw;

  const maybeRel = path.resolve(process.cwd(), raw);
  if (fs.existsSync(maybeRel)) return maybeRel;

  const base = path.basename(raw);
  const email = String(userEmail || "").trim().toLowerCase();
  const safeEmpresaId = String(empresaId || "").trim().replace(/[^0-9A-Za-z_-]/g, "_");
  const userSlug = email.replace(/[^a-z0-9._-]/g, "_");
  const userDir = path.resolve(process.cwd(), "certs", userSlug);

  const candidates = [
    safeEmpresaId ? path.join(userDir, `captura-empresa-${safeEmpresaId}.pfx`) : "",
    safeEmpresaId ? path.join(userDir, `captura-empresa-${safeEmpresaId}.p12`) : "",
    base ? path.join(userDir, base) : "",
  ].filter(Boolean);

  const found = candidates.find((c) => fs.existsSync(c));
  return found || raw;
}

// ---------------------------
// ROBÔ – MANUAL (multi-tenant: usa req.userEmail como "dono")
// ---------------------------
app.post("/api/nf/manual", async (req, res) => {
  try {
    if (!assertPeriodo(req, res)) return;

    const baixarXml = !!req.body?.baixarXml;
    const baixarPdf = !!req.body?.baixarPdf;

    const tipos = normalizeTipos(req.body?.processarTipos, req.body?.tipoNota);

    const isCertReq =
      req.body?.usarCertificadoA1 === true ||
      String(req.body?.authType || "").toLowerCase().includes("certificado");

    const resolvedCertPath = isCertReq
      ? resolveCertPathForUser({
          certPfxPath: req.body?.certPfxPath || req.body?.pfxPath || "",
          empresaId: req.body?.empresaId || "",
          userEmail: req.body?.usuarioEmail || req.userEmail || "",
        })
      : "";

    const baseBody = {
      ...req.body,
      baixarXml,
      baixarPdf,
      certPfxPath: resolvedCertPath || req.body?.certPfxPath || req.body?.pfxPath || "",
      // ✅ garante que histórico/execuções usem o usuário do header ou sessão
      usuarioEmail: req.body?.usuarioEmail || req.userEmail || "",
      onLog: (msg) => console.log(msg),
    };

    let allLogs = [];
    let rootJobDir = null;

    for (const tipoNota of tipos) {
      const result = await runManualDownload({
        ...baseBody,
        tipoNota,
        jobDir: rootJobDir || undefined,
      });

      (result?.logs || []).forEach((m) => allLogs.push(m));

      if (!rootJobDir) {
        rootJobDir = result?.paths?.jobDir || result?.jobDir || null;
      }
    }

    let downloadZipUrl = null;

    const zipTarget = rootJobDir && fs.existsSync(rootJobDir) ? rootJobDir : null;

    if (zipTarget) {
      const zipName = `nfse-manual-${Date.now()}.zip`;
      const zipPath = path.join(ZIP_DIR, zipName);

      const empresaNome = safeSlug(req.body?.empresaNome || "empresa");
      const empresaCnpj = safeSlug(req.body?.empresaCnpj || req.body?.cnpj || "");
      const zipRootName = [empresaNome, empresaCnpj].filter(Boolean).join("_") || "empresa";
      await zipDirectory(zipTarget, zipPath, zipRootName);
      downloadZipUrl = `/zips/${zipName}`;
    }

    return res.json({
      success: true,
      logs: allLogs,
      downloadZipUrl,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      success: false,
      error: "Erro ao executar download manual",
    });
  }
});

// ---------------------------
// ROBÔ – LOTE (agora lista empresas do próprio usuário)
// ---------------------------
app.post("/api/nf/lote", async (req, res) => {
  try {
    if (!assertPeriodo(req, res)) return;

    const userEmail = req.userEmail || "";
    const empresasBody = Array.isArray(req.body?.empresas) ? req.body.empresas : null;
    const empresas = empresasBody && empresasBody.length ? empresasBody : listarEmpresas(userEmail);

    if (!empresas || empresas.length === 0) {
      return res.status(400).json({
        success: false,
        error: "Nenhuma empresa cadastrada para execução em lote (para este usuário).",
      });
    }

    const baixarXml = !!req.body?.baixarXml;
    const baixarPdf = !!req.body?.baixarPdf;

    const tipos = normalizeTipos(req.body?.processarTipos, req.body?.tipoNota);

    const empresasNormalized = (empresas || []).map((emp) => {
      const isCert =
        emp?.usarCertificadoA1 === true ||
        String(emp?.authType || "").toLowerCase().includes("certificado");
      const certPath = isCert
        ? resolveCertPathForUser({
            certPfxPath: emp?.certPfxPath || emp?.pfxPath || emp?.certFile || "",
            empresaId: emp?.id || emp?.empresaId || "",
            userEmail: req.body?.usuarioEmail || userEmail || "",
          })
        : "";
      return {
        ...emp,
        certPfxPath: certPath || emp?.certPfxPath || emp?.pfxPath || emp?.certFile || "",
      };
    });

    const result = await runLoteDownload(empresasNormalized, {
      ...req.body,
      baixarXml,
      baixarPdf,
      usuarioEmail: req.body?.usuarioEmail || userEmail || "",
      onLog: (msg) => console.log(msg),
      processarTipos: tipos,
    });

    const logs = result?.logs || [];
    const finalDir = result?.paths?.jobDir || result?.jobDir || null;

    let downloadZipUrl = null;

    if (finalDir && fs.existsSync(finalDir)) {
      const zipName = `nfse-lote-${Date.now()}.zip`;
      const zipPath = path.join(ZIP_DIR, zipName);

      await zipDirectory(finalDir, zipPath);
      downloadZipUrl = `/zips/${zipName}`;
    }

    return res.json({
      success: true,
      logs,
      downloadZipUrl,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      success: false,
      error: "Erro ao executar lote",
    });
  }
});

// ---------------------------
// Certificado A1 (captura): salvar PFX/P12 e retornar caminho
// ---------------------------
app.post("/api/nf/certificado", (req, res) => {
  try {
    const userEmail = String(req.userEmail || "").trim();
    const { empresaId, filename, pfxBase64, passphrase } = req.body || {};

    if (!userEmail || !empresaId) {
      return res.status(400).json({ ok: false, error: "usuarioEmail e empresaId são obrigatórios." });
    }
    if (!pfxBase64 || !passphrase) {
      return res.status(400).json({ ok: false, error: "Arquivo do certificado e senha são obrigatórios." });
    }

    const certsDir = path.resolve(process.cwd(), "certs");
    if (!fs.existsSync(certsDir)) fs.mkdirSync(certsDir, { recursive: true });

    const userSlug = String(userEmail).toLowerCase().replace(/[^a-z0-9._-]/g, "_");
    const userDir = path.join(certsDir, userSlug);
    if (!fs.existsSync(userDir)) fs.mkdirSync(userDir, { recursive: true });

    const safeEmpresaId = String(empresaId).replace(/[^0-9A-Za-z_-]/g, "_");
    const ext = String(filename || "").toLowerCase().endsWith(".p12") ? "p12" : "pfx";
    const outPath = path.join(userDir, `captura-empresa-${safeEmpresaId}.${ext}`);

    const buf = Buffer.from(String(pfxBase64), "base64");
    if (!buf || buf.length < 500) {
      return res.status(400).json({ ok: false, error: "Arquivo de certificado inválido." });
    }

    fs.writeFileSync(outPath, buf);

    return res.json({
      ok: true,
      certPfxPath: outPath,
      certPassphrase: String(passphrase),
      message: "Certificado A1 salvo com sucesso.",
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Erro ao salvar certificado A1." });
  }
});

// ---------------------------
// ZIPs: excluir arquivo gerado
// ---------------------------
app.delete("/api/zips/:zipName", (req, res) => {
  try {
    const zipName = String(req.params?.zipName || "").trim();
    if (!zipName || zipName.includes("..") || zipName.includes("/") || zipName.includes("\\")) {
      return res.status(400).json({ ok: false, error: "Nome de ZIP inválido." });
    }

    const zipPath = path.join(ZIP_DIR, zipName);
    if (!fs.existsSync(zipPath)) {
      return res.status(404).json({ ok: false, error: "ZIP não encontrado." });
    }

    fs.unlinkSync(zipPath);
    return res.json({ ok: true });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Erro ao excluir ZIP." });
  }
});

// ---------------------------
// Fallback
// ---------------------------
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "dashboard.html"));
});

ensureCrmTestUsers()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Servidor rodando em http://localhost:${PORT}`);
    });
  })
  .catch((err) => {
    console.error("[seed] erro ao criar usuários CRM de teste:", err?.message || err);
    app.listen(PORT, () => {
      console.log(`Servidor rodando em http://localhost:${PORT}`);
    });
  });
