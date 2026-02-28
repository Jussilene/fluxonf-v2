// src/utils/empresasStore.js
// "Banco de dados" simples em JSON para empresas do lote NFSe (AGORA multiusuário)

import fs from "fs";
import path from "path";

const DB_DIR = path.resolve(process.cwd(), "data");
const DB_FILE = path.join(DB_DIR, "empresas.json");

// Garante que a pasta e o arquivo existem
function ensureDbFile() {
  try {
    if (!fs.existsSync(DB_DIR)) {
      fs.mkdirSync(DB_DIR, { recursive: true });
    }

    // padrão do seu store: objeto { lastId, empresas }
    if (!fs.existsSync(DB_FILE)) {
      fs.writeFileSync(DB_FILE, JSON.stringify({ lastId: 0, empresas: [] }, null, 2));
    }
  } catch (err) {
    console.error("[EMPRESAS_DB] Erro ao garantir arquivo de banco:", err);
  }
}

function readDb() {
  ensureDbFile();
  try {
    const raw = fs.readFileSync(DB_FILE, "utf-8");
    const parsed = JSON.parse(raw);

    // ✅ se o arquivo estiver no formato antigo (array),
    // converte automaticamente para o formato novo (objeto)
    if (Array.isArray(parsed)) {
      const converted = { lastId: parsed.length, empresas: parsed };
      fs.writeFileSync(DB_FILE, JSON.stringify(converted, null, 2));
      return converted;
    }

    if (!parsed || typeof parsed !== "object") throw new Error("Formato inválido");

    if (!Array.isArray(parsed.empresas)) parsed.empresas = [];
    if (typeof parsed.lastId !== "number") parsed.lastId = 0;

    return parsed;
  } catch (err) {
    console.error("[EMPRESAS_DB] Erro ao ler banco. Recriando...", err);
    const reset = { lastId: 0, empresas: [] };
    fs.writeFileSync(DB_FILE, JSON.stringify(reset, null, 2));
    return reset;
  }
}

function writeDb(data) {
  try {
    fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2));
  } catch (err) {
    console.error("[EMPRESAS_DB] Erro ao escrever banco:", err);
  }
}

function normalizeEmpresaPayload(input = {}, base = {}) {
  const cnpjRaw = (input.cnpj ?? base.cnpj ?? "").toString();
  const cleanCnpj = cnpjRaw.replace(/\D/g, "");
  const authType = (input.authType ?? base.authType ?? "Login e Senha").toString();
  const loginPortal = (input.loginPortal ?? input.portalLogin ?? base.loginPortal ?? base.portalLogin ?? cleanCnpj).toString().trim();
  const senhaPortal = (input.senhaPortal ?? input.portalSenha ?? base.senhaPortal ?? base.portalSenha ?? "").toString();

  return {
    nome: (input.nome ?? base.nome ?? "").toString().trim(),
    cnpj: cleanCnpj,
    loginPortal,
    senhaPortal,
    municipio: (input.municipio ?? base.municipio ?? "").toString().trim(),
    userEmail: (input.userEmail ?? base.userEmail ?? "").toString().trim(),
    authType,
    portalLogin: loginPortal,
    portalSenha: senhaPortal,
    razaoSocial: (input.razaoSocial ?? base.razaoSocial ?? "").toString().trim(),
    inscricaoEstadual: (input.inscricaoEstadual ?? base.inscricaoEstadual ?? "").toString().trim(),
    uf: (input.uf ?? base.uf ?? "").toString().trim(),
    cidade: (input.cidade ?? base.cidade ?? "").toString().trim(),
    endereco: (input.endereco ?? base.endereco ?? "").toString().trim(),
    telefone: (input.telefone ?? base.telefone ?? "").toString().trim(),
    email: (input.email ?? base.email ?? "").toString().trim(),
    status: (input.status ?? base.status ?? "ATIVO").toString().trim().toUpperCase(),
    ativa: input.ativa ?? base.ativa ?? true,
    certPfxPath: (input.certPfxPath ?? input.pfxPath ?? input.certFile ?? base.certPfxPath ?? base.pfxPath ?? base.certFile ?? "").toString().trim(),
    certPassphrase: (
      input.certPassphrase ??
      input.certPfxPassphrase ??
      input.passphrase ??
      input.certPass ??
      base.certPassphrase ??
      base.certPfxPassphrase ??
      base.passphrase ??
      base.certPass ??
      ""
    ).toString(),
  };
}

// ✅ Agora aceita userEmail opcional.
// - Se vier userEmail: lista só daquele usuário
// - Se não vier: comportamento antigo (lista tudo)
export function listarEmpresas(userEmail = "") {
  const db = readDb();
  const u = String(userEmail || "").trim().toLowerCase();

  if (!u) return db.empresas;

  return db.empresas.filter((e) => String(e.userEmail || "").trim().toLowerCase() === u);
}

// ✅ ALIAS para compatibilidade
export function readEmpresas(userEmail = "") {
  return listarEmpresas(userEmail);
}

// ✅ Agora aceita userEmail opcional.
// - Se vier userEmail: grava empresa “pertencendo” ao usuário
export function adicionarEmpresa(payload = {}) {
  const db = readDb();
  const now = new Date().toISOString();
  const normalized = normalizeEmpresaPayload(payload);

  const novaEmpresa = {
    id: db.lastId + 1,
    ...normalized,
    ativo: normalized.ativa !== false,
    pfxPath: normalized.certPfxPath,
    passphrase: normalized.certPassphrase,
    certFile: normalized.certPfxPath,
    certPass: normalized.certPassphrase,
    certPfxPassphrase: normalized.certPassphrase,

    createdAt: now,
    updatedAt: now,
  };

  db.lastId = novaEmpresa.id;
  db.empresas.push(novaEmpresa);
  writeDb(db);

  return novaEmpresa;
}

export function atualizarEmpresa(id, payload = {}, userEmail = "") {
  const db = readDb();
  const idNum = Number(id);
  const u = String(userEmail || "").trim().toLowerCase();
  const idx = db.empresas.findIndex((emp) => {
    if (Number(emp.id) !== idNum) return false;
    if (!u) return true;
    const owner = String(emp.userEmail || "").trim().toLowerCase();
    return owner === u;
  });

  if (idx < 0) return null;

  const now = new Date().toISOString();
  const current = db.empresas[idx] || {};
  const normalized = normalizeEmpresaPayload(
    {
      ...payload,
      userEmail: current.userEmail || payload.userEmail || "",
    },
    current
  );

  const updated = {
    ...current,
    ...normalized,
    ativo: normalized.ativa !== false,
    pfxPath: normalized.certPfxPath,
    passphrase: normalized.certPassphrase,
    certFile: normalized.certPfxPath,
    certPass: normalized.certPassphrase,
    certPfxPassphrase: normalized.certPassphrase,
    updatedAt: now,
  };

  db.empresas[idx] = updated;
  writeDb(db);
  return updated;
}

// ✅ Agora aceita userEmail opcional.
// - Se vier userEmail: só remove se a empresa for daquele usuário
// - Se não vier: comportamento antigo (remove global)
export function removerEmpresa(id, userEmail = "") {
  const db = readDb();
  const idNum = Number(id);
  const u = String(userEmail || "").trim().toLowerCase();

  const before = db.empresas.length;

  db.empresas = db.empresas.filter((emp) => {
    if (Number(emp.id) !== idNum) return true;

    // se não tiver userEmail informado, remove como antes
    if (!u) return false;

    // se tiver userEmail, só remove se for dono
    const owner = String(emp.userEmail || "").trim().toLowerCase();
    return owner !== u;
  });

  const after = db.empresas.length;

  if (after !== before) {
    writeDb(db);
    return true;
  }
  return false;
}
