import fs from "fs";
import path from "path";
import { chromium } from "playwright";

const LOGIN_URL =
  process.env.NFSE_PORTAL_URL ||
  "https://www.nfse.gov.br/EmissorNacional/Login?ReturnUrl=%2fEmissorNacional";

const HOME_URL =
  process.env.NFSE_HOME_URL || "https://www.nfse.gov.br/EmissorNacional";

// (mantém como fallback, mas NÃO é o fluxo principal)
const EMISSAO_URL =
  process.env.NFSE_EMISSAO_URL ||
  "https://www.nfse.gov.br/EmissorNacional/DPS/Pessoas";

const DEBUG_DIR = path.join(process.cwd(), "data", "emissao_debug");

function safe(s) {
  return String(s || "").replace(/[^a-z0-9@._-]/gi, "_");
}

function getStatePath({ usuarioEmail, empresaId }) {
  return path.join(process.cwd(), "data", "sessions", safe(usuarioEmail), `${safe(empresaId)}.json`);
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
function ensureDirForFile(fp) {
  ensureDir(path.dirname(fp));
}

function envHeadless() {
  const v = String(process.env.NFSE_HEADLESS ?? process.env.NFS_HEADLESS ?? "1").trim();
  return !(v === "0" || v.toLowerCase() === "false");
}

function envInt(name, defaultValue) {
  const raw = String(process.env[name] ?? "").trim();
  return /^\d+$/.test(raw) ? Number(raw) : defaultValue;
}

async function saveDebug(page, prefix) {
  try {
    ensureDir(DEBUG_DIR);
    const ts = Date.now();
    await page.screenshot({ path: path.join(DEBUG_DIR, `${prefix}-${ts}.png`), fullPage: true });
    const html = await page.content();
    fs.writeFileSync(path.join(DEBUG_DIR, `${prefix}-${ts}.html`), html, "utf8");
  } catch {}
}

function normalizeDateBR(input) {
  if (!input) return "";
  const s = String(input).trim();

  // já está dd/mm/aaaa
  if (/^\d{2}\/\d{2}\/\d{4}$/.test(s)) return s;

  // yyyy-mm-dd -> dd/mm/yyyy
  const m1 = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m1) return `${m1[3]}/${m1[2]}/${m1[1]}`;

  // yyyy/mm/dd -> dd/mm/yyyy
  const m2 = s.match(/^(\d{4})\/(\d{2})\/(\d{2})$/);
  if (m2) return `${m2[3]}/${m2[2]}/${m2[1]}`;

  // dd-mm-yyyy -> dd/mm/yyyy
  const m3 = s.match(/^(\d{2})-(\d{2})-(\d{4})$/);
  if (m3) return `${m3[1]}/${m3[2]}/${m3[3]}`;

  // tenta extrair números (ex: 13.01.2026)
  const m4 = s.match(/^(\d{2})\D(\d{2})\D(\d{4})$/);
  if (m4) return `${m4[1]}/${m4[2]}/${m4[3]}`;

  return s; // fallback
}

async function setInputValueWithEvents(locator, value) {
  const v = String(value ?? "");
  await locator.scrollIntoViewIfNeeded().catch(() => {});
  await locator.waitFor({ state: "visible", timeout: 15000 });
  await locator.evaluate((el, val) => {
    el.focus();
    el.value = val;
    el.dispatchEvent(new Event("input", { bubbles: true }));
    el.dispatchEvent(new Event("change", { bubbles: true }));
    el.blur();
  }, v);
}

async function clickByText(page, txt) {
  const btn = page.getByRole("button", { name: new RegExp(txt, "i") });
  if (await btn.count()) {
    await btn.first().click().catch(() => {});
    return true;
  }
  const link = page.getByRole("link", { name: new RegExp(txt, "i") });
  if (await link.count()) {
    await link.first().click().catch(() => {});
    return true;
  }
  const any = page.locator(`text=${txt}`).first();
  if (await any.count()) {
    await any.click().catch(() => {});
    return true;
  }
  return false;
}

async function isOnLoginPage(page) {
  const url = page.url() || "";
  if (/\/Login\?/i.test(url)) return true;
  const c = await page.locator('button:has-text("Entrar")').count().catch(() => 0);
  const c2 = await page.locator('input[placeholder*="CPF"], input[placeholder*="CNPJ"]').count().catch(() => 0);
  if (c > 0 && c2 > 0) return true;
  return false;
}

async function isLoggedIn(page) {
  if (await isOnLoginPage(page)) return false;
  const OK = [
    'text="Portal Contribuinte"',
    'text="Rascunhos"',
    'text="Emissão Completa"',
    'a:has-text("Sair")',
    'button:has-text("Sair")',
  ];
  for (const sel of OK) {
    const c = await page.locator(sel).count().catch(() => 0);
    if (c > 0) return true;
  }
  return false;
}

async function clickAvancar(page) {
  const btn = page.getByRole("button", { name: /Avançar/i }).first();
  if (await btn.count()) {
    await btn.first().scrollIntoViewIfNeeded().catch(() => {});
    await btn.first().click().catch(() => {});
    return true;
  }
  const any = page.locator('text=Avançar').first();
  if (await any.count()) {
    await any.scrollIntoViewIfNeeded().catch(() => {});
    await any.click().catch(() => {});
    return true;
  }
  return false;
}

async function bringPortalToFront(page) {
  try {
    const session = await page.context().newCDPSession(page);
    const { windowId } = await session.send("Browser.getWindowForTarget");
    await session.send("Browser.setWindowBounds", { windowId, bounds: { windowState: "normal" } }).catch(() => {});
    await session.send("Browser.setWindowBounds", { windowId, bounds: { windowState: "maximized" } }).catch(() => {});
  } catch {}

  await page.bringToFront().catch(() => {});
  await page.evaluate(() => window.focus()).catch(() => {});
  await page.waitForTimeout(300).catch(() => {});
}

async function isHumanValidationVisible(page) {
  try {
    const byText = await page
      .locator("text=/VALIDA..O DE USU.RIO|Sou humano|hCaptcha|HCaptcha|Confirmar|captcha/i")
      .first()
      .isVisible({ timeout: 800 })
      .catch(() => false);
    if (byText) return true;

    const byFrame = await page
      .locator('iframe[src*="hcaptcha"], iframe[title*="hCaptcha"], iframe[title*="captcha" i]')
      .first()
      .isVisible({ timeout: 800 })
      .catch(() => false);

    return !!byFrame;
  } catch {
    return false;
  }
}

async function waitForHumanValidationToFinish(page, onLog, contextLabel = "login") {
  if (!(await isHumanValidationVisible(page))) return false;

  await bringPortalToFront(page);

  const timeoutMs = envInt("NFSE_CAPTCHA_TIMEOUT_MS", 300000);
  onLog(
    `Validação humana detectada (${contextLabel}). Resolva o CAPTCHA na janela aberta e clique em Confirmar. Vou aguardar até ${Math.round(
      timeoutMs / 1000
    )}s.`
  );

  const solved = await page
    .waitForFunction(
      () => {
        const bodyText = document.body ? document.body.innerText || "" : "";
        const stillHasText = /VALIDA..O DE USU.RIO|Sou humano|hCaptcha|HCaptcha|Confirmar|captcha/i.test(bodyText);
        const hasCaptchaFrame = !!document.querySelector(
          'iframe[src*="hcaptcha"], iframe[title*="hCaptcha"], iframe[title*="captcha" i]'
        );
        const visibleDialog = Array.from(document.querySelectorAll(".modal, .modal-dialog, [role='dialog']")).some(
          (el) => {
            const rect = el.getBoundingClientRect();
            const style = window.getComputedStyle(el);
            return rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden";
          }
        );
        return !stillHasText && !hasCaptchaFrame && !visibleDialog;
      },
      undefined,
      { timeout: timeoutMs }
    )
    .then(() => true)
    .catch(() => false);

  onLog(
    solved
      ? `Validação humana concluída (${contextLabel}). Continuando...`
      : `Tempo de espera do CAPTCHA esgotado (${contextLabel}).`
  );
  return solved;
}

/**
 * Dropdown por label:
 * tenta <select>; se não, tenta clicar no campo/trigger e selecionar texto.
 */
async function pickDropdownByLabel(page, labelText, optionText) {
  // tenta achar select depois do label
  const select = page
    .locator(`xpath=//*[contains(normalize-space(.),"${labelText}")]/following::select[1]`)
    .first();

  if (await select.count()) {
    await select.scrollIntoViewIfNeeded().catch(() => {});
    await select.waitFor({ state: "visible", timeout: 15000 }).catch(() => {});
    if (optionText) {
      await select.selectOption({ label: String(optionText) }).catch(() => {});
      await select.selectOption({ value: String(optionText) }).catch(() => {});
    }
    return true;
  }

  // dropdown estilizado: tenta clicar no “campo” logo após o label
  const trigger = page
    .locator(`xpath=//*[contains(normalize-space(.),"${labelText}")]/following::*[self::div or self::span or self::button][1]`)
    .first();

  if (await trigger.count()) {
    await trigger.scrollIntoViewIfNeeded().catch(() => {});
    await trigger.click().catch(() => {});
    await page.waitForTimeout(200);

    if (optionText) {
      const opt = page.getByText(new RegExp(String(optionText).replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i")).first();
      if (await opt.count()) {
        await opt.click().catch(() => {});
        return true;
      }
    }

    // fallback: primeiro item clicável visível
    const optRole = page.locator('[role="option"]:visible').first();
    if ((await optRole.count().catch(() => 0)) > 0) {
      await optRole.click().catch(() => {});
      return true;
    }
    const li = page.locator("li:visible").first();
    if ((await li.count().catch(() => 0)) > 0) {
      await li.click().catch(() => {});
      return true;
    }

    return true;
  }

  return false;
}

/**
 * ✅ fluxo correto:
 * HOME -> abrir menu NFS-e -> clicar Emissão completa -> cair no /DPS/Pessoas
 * e VALIDAR que o emitente está carregado (CNPJ/Razão Social).
 */
async function abrirEmissaoCompletaViaMenu(page, onLog = () => {}) {
  onLog("Abrindo menu NFS-e e clicando Emissão completa...");

  await page.goto(HOME_URL, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(300);

  // tenta abrir o dropdown pelo ícone NFS-e (no topo)
  const openTries = [
    'xpath=//*[contains(normalize-space(.),"NFS-e")][1]',
    'xpath=//*[@title="NFS-e" or contains(@class,"nfse")][1]',
    'xpath=//*[contains(@class,"fa") or contains(@class,"icon")][ancestor::*[contains(normalize-space(.),"NFS")]][1]',
  ];

  // se já aparece “Tipos de emissão”, o menu está aberto
  let menuOpen = (await page.locator("text=Tipos de emissão").count().catch(() => 0)) > 0;

  if (!menuOpen) {
    for (const sel of openTries) {
      const el = page.locator(sel).first();
      if ((await el.count().catch(() => 0)) > 0) {
        await el.click().catch(() => {});
        await page.waitForTimeout(150);
        menuOpen = (await page.locator("text=Tipos de emissão").count().catch(() => 0)) > 0;
        if (menuOpen) break;
      }
    }
  }

  // clica em “Emissão completa”
  const clicked = await clickByText(page, "Emissão completa");
  if (!clicked) {
    // tenta diretamente no item do menu
    const opt = page.locator('xpath=//*[contains(normalize-space(.),"Emissão completa")][1]').first();
    if ((await opt.count().catch(() => 0)) > 0) {
      await opt.click().catch(() => {});
    } else {
      throw new Error("Não consegui clicar em 'Emissão completa'.");
    }
  }

  // aguarda URL certa
  await page.waitForURL(/\/DPS\/Pessoas/i, { timeout: 30000 }).catch(() => {});
  if (!/\/DPS\/Pessoas/i.test(page.url() || "")) {
    // fallback
    await page.goto(EMISSAO_URL, { waitUntil: "domcontentloaded" });
  }

  // ✅ valida emitente carregado: CNPJ do emitente normalmente aparece preenchido
  // (se não estiver, é sinal que entrou sem contexto; tenta de novo)
  const emitenteCnpj = page.locator('input[value][id*="Emitente"], input[id*="Emitente"], input[id*="Prestador"]');
  const okEmitente =
    (await emitenteCnpj.count().catch(() => 0)) > 0 ||
    (await page.locator("text=EMITENTE DA NFS-E").count().catch(() => 0)) > 0;

  if (!okEmitente) {
    onLog("⚠️ Emissão abriu, mas emitente parece não carregado. Tentando novamente via menu...");
    await page.goto(HOME_URL, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(200);
    // tenta novamente
    const clicked2 = await clickByText(page, "Emissão completa");
    if (!clicked2) {
      // fallback final
      await page.goto(EMISSAO_URL, { waitUntil: "domcontentloaded" });
    } else {
      await page.waitForURL(/\/DPS\/Pessoas/i, { timeout: 30000 }).catch(() => {});
    }
  }

  onLog(`Tela de emissão aberta: ${page.url()}`);
}

/**
 * Login assistido: abre navegador para você logar e salva storageState.
 */
export async function iniciarSessaoGovBr({ usuarioEmail, empresaId, onLog = () => {} }) {
  const statePath = getStatePath({ usuarioEmail, empresaId });
  ensureDirForFile(statePath);

  if (fs.existsSync(statePath)) {
    return { sessionExists: true, message: "Sessão já existe. Pode emitir." };
  }

  onLog("Abrindo navegador (login assistido gov.br)...");
  const browser = await chromium.launch({
    headless: false,
    slowMo: 120,
    args: ["--start-maximized", "--window-size=1280,900"],
  });
  const context = await browser.newContext();
  const page = await context.newPage();

  await page.goto(LOGIN_URL, { waitUntil: "domcontentloaded" });

  onLog("Faça login manualmente (captcha/2FA se pedir).");
  onLog("Quando entrar no Emissor Nacional, vou salvar a sessão.");

  await bringPortalToFront(page);

  const deadline = Date.now() + envInt("NFSE_ASSISTED_LOGIN_TIMEOUT_MS", 6 * 60 * 1000);
  let logged = false;

  while (Date.now() < deadline && !logged) {
    logged = await isLoggedIn(page);
    if (!logged && (await isHumanValidationVisible(page))) {
      await waitForHumanValidationToFinish(page, onLog, "login assistido");
      logged = await isLoggedIn(page);
    }
    if (!logged) await page.waitForTimeout(800);
  }

  if (!logged) {
    await saveDebug(page, "login-timeout");
    await browser.close();
    throw new Error("Timeout aguardando login no Emissor Nacional. Verifique captcha/2FA.");
  }

  await context.storageState({ path: statePath });
  await browser.close();

  onLog("Sessão salva com sucesso!");
  return { sessionExists: false, message: "Sessão salva. Agora pode emitir." };
}

/**
 * Login automático (opcional)
 */
async function tentarLoginAutomatico(
  page,
  { loginPortal, senhaPortal, onLog = () => {}, salvarSessaoAuto = true, context, statePath }
) {
  if (!loginPortal || !senhaPortal) return { ok: false, reason: "Sem login/senha" };

  onLog("Sessão não validou login. Tentando login automático...");

  await page.goto(LOGIN_URL, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(300);

  if (await isLoggedIn(page)) {
    onLog("Já estava logado no portal.");
    if (salvarSessaoAuto && context && statePath) {
      ensureDirForFile(statePath);
      await context.storageState({ path: statePath });
      onLog("Sessão salva automaticamente (storageState).");
    }
    return { ok: true };
  }

  const loginSel = page.locator('input[placeholder*="CPF"], input[placeholder*="CNPJ"], input[name="login"], input[autocomplete="username"]').first();
  const passSel = page.locator('input[type="password"], input[name="password"], input[autocomplete="current-password"]').first();

  if ((await loginSel.count().catch(() => 0)) === 0 || (await passSel.count().catch(() => 0)) === 0) {
    await saveDebug(page, "login-auto-nao-achou-campos");
    return { ok: false, reason: "Campos não encontrados" };
  }

  await loginSel.fill(String(loginPortal)).catch(() => {});
  await passSel.fill(String(senhaPortal)).catch(() => {});
  await clickByText(page, "Entrar").catch(() => {});

  const deadline = Date.now() + envInt("NFSE_AUTO_LOGIN_TIMEOUT_MS", 45 * 1000);
  while (Date.now() < deadline) {
    if (await isLoggedIn(page)) {
      onLog("Login automático realizado com sucesso.");
      if (salvarSessaoAuto && context && statePath) {
        ensureDirForFile(statePath);
        await context.storageState({ path: statePath });
        onLog("Sessão salva automaticamente (storageState).");
      }
      return { ok: true };
    }
    if (await isHumanValidationVisible(page)) {
      const solved = await waitForHumanValidationToFinish(page, onLog, "login");
      if (solved && (await isLoggedIn(page))) {
        onLog("Login concluído após validação humana.");
        if (salvarSessaoAuto && context && statePath) {
          ensureDirForFile(statePath);
          await context.storageState({ path: statePath });
          onLog("Sessão salva automaticamente (storageState).");
        }
        return { ok: true };
      }
    }

    await page.waitForTimeout(900);
  }

  await saveDebug(page, "login-auto-timeout");
  return { ok: false, reason: "Timeout" };
}

/**
 * ✅ Helper: pega o primeiro input visível dentre possíveis seletores e preenche.
 */
async function fillFirstVisible(page, selectors, value) {
  for (const sel of selectors) {
    const loc = page.locator(sel);
    const n = await loc.count().catch(() => 0);
    if (!n) continue;

    for (let i = 0; i < n; i++) {
      const el = loc.nth(i);
      const visible = await el.isVisible().catch(() => false);
      if (!visible) continue;

      await el.scrollIntoViewIfNeeded().catch(() => {});
      await el.waitFor({ state: "visible", timeout: 15000 }).catch(() => {});
      // usa evaluate + events (melhor para inputs mascarados)
      await setInputValueWithEvents(el, value);
      return true;
    }
  }
  return false;
}

/**
 * Emite NFS-e
 */
export async function emitirNfseNoPortal(payload, { onLog = () => {} } = {}) {
  const {
    usuarioEmail,
    empresaId,

    dataCompetencia,
    tomadorDocumento,
    tomadorNome,
    tomadorEmail,
    descricaoServico,
    valorServico,

    municipio,
    indicadorMunicipal,

    loginPortal,
    senhaPortal,
    headless,
    salvarSessaoAuto,
  } = payload;

  const statePath = getStatePath({ usuarioEmail, empresaId });
  const resolvedHeadless = typeof headless === "boolean" ? headless : envHeadless();

  onLog(`Iniciando emissão (headless=${resolvedHeadless ? "true" : "false"})...`);

  const hasState = fs.existsSync(statePath);

  const browser = await chromium.launch({
    headless: resolvedHeadless,
    // ✅ mais rápido (ainda dá pra acompanhar)
    slowMo: resolvedHeadless ? 0 : 80,
    args: resolvedHeadless ? [] : ["--start-maximized", "--window-size=1280,900"],
  });

  const context = await browser.newContext(hasState ? { storageState: statePath } : {});
  const page = await context.newPage();

  try {
    onLog("Abrindo portal...");
    await page.goto(HOME_URL, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(200);

    onLog(`URL atual: ${page.url()}`);

    let logged = await isLoggedIn(page);

    if (!logged) {
      const r = await tentarLoginAutomatico(page, {
        loginPortal,
        senhaPortal,
        onLog,
        salvarSessaoAuto: salvarSessaoAuto !== false,
        context,
        statePath,
      });

      if (!r.ok) {
        throw new Error("Não foi possível logar automaticamente. Use 'Salvar sessão (RPA)' se houver 2FA/captcha.");
      }

      await page.goto(HOME_URL, { waitUntil: "domcontentloaded" });
      await page.waitForTimeout(200);
      logged = await isLoggedIn(page);
    } else {
      onLog("Portal já está logado.");
    }

    // ✅ abre emissão pelo menu (fluxo correto)
    await abrirEmissaoCompletaViaMenu(page, onLog);

    // se cair no Login com ReturnUrl, tenta voltar e abrir de novo
    if (await isOnLoginPage(page)) {
      onLog("Detectei redirecionamento para Login (ReturnUrl). Tentando voltar...");
      await page.goto(HOME_URL, { waitUntil: "domcontentloaded" });
      await page.waitForTimeout(200);
      await abrirEmissaoCompletaViaMenu(page, onLog);
    }

    onLog(`URL emissão: ${page.url()}`);

    // ---------------------------
    // PASSO 1: Pessoas
    // ---------------------------

    // ✅ DATA (força dd/mm/aaaa)
    const d = normalizeDateBR(dataCompetencia);
    if (d) {
      // tenta por label e por input comum
      const dateByLabel = page.getByLabel(/Data de Competência/i).first();
      if ((await dateByLabel.count().catch(() => 0)) > 0 && (await dateByLabel.isVisible().catch(() => false))) {
        await setInputValueWithEvents(dateByLabel, d);
      } else {
        // fallback: pega o input antes do ícone calendário (muito comum)
        const dateInput = page.locator('input[type="text"]').filter({ hasText: "" }).first();
        // melhor: procura pelo bloco “Data de Competência” e pega o input dentro
        const dateBlock = page.locator('xpath=//*[contains(normalize-space(.),"Data de Competência")]/following::input[1]').first();
        if ((await dateBlock.count().catch(() => 0)) > 0) await setInputValueWithEvents(dateBlock, d);
        else if ((await dateInput.count().catch(() => 0)) > 0) await setInputValueWithEvents(dateInput, d);
      }
    }

    // Município / Indicador
    await pickDropdownByLabel(page, "Município", municipio || "Curitiba/PR").catch(() => {});
    await pickDropdownByLabel(page, "Indicador Municipal", indicadorMunicipal || "Não informado").catch(() => {});

    // ✅ TOMADOR CPF/CNPJ (usa ID direto e pega o visível)
    onLog("Preenchendo Tomador...");

    // o input do teu log é este:
    // id="Tomador_Inscricao"
    const okCpf = await fillFirstVisible(page, ["input#Tomador_Inscricao", 'input[name="Tomador.Inscricao"]', 'input.cpfcnpj:visible'], tomadorDocumento);

    if (!okCpf) {
      throw new Error("Não consegui preencher o CPF/CNPJ do tomador (campo não visível).");
    }

    // nome/email do tomador (se existirem inputs)
    if (tomadorNome) {
      await fillFirstVisible(page, ['input[name*="Tomador"][name*="Nome"]', 'input[id*="Tomador"][id*="Nome"]', 'input[placeholder*="Razão"]'], tomadorNome).catch(() => {});
    }
    if (tomadorEmail) {
      await fillFirstVisible(page, ['input[type="email"]', 'input[name*="Email"]', 'input[id*="Email"]'], tomadorEmail).catch(() => {});
    }

    // Avançar
    const okAv = await clickAvancar(page);
    if (!okAv) onLog("⚠️ Não encontrei botão Avançar (Pessoas).");
    await page.waitForTimeout(500);

    // ---------------------------
    // PASSO 2: Serviço
    // ---------------------------
    onLog("Preenchendo Serviço...");
    if (descricaoServico) {
      await fillFirstVisible(page, ['textarea', 'textarea[name*="Discrimin"]', 'textarea[id*="Discrimin"]', 'input[name*="Discrimin"]'], descricaoServico).catch(() => {});
    }

    await clickAvancar(page).catch(() => {});
    await page.waitForTimeout(500);

    // ---------------------------
    // PASSO 3: Valores
    // ---------------------------
    onLog("Preenchendo Valores...");
    const v = String(valorServico ?? "").replace(",", ".");
    if (v) {
      await fillFirstVisible(page, ['input[name*="Valor"]', 'input[id*="Valor"]', 'input[placeholder*="Valor"]'], v).catch(() => {});
    }

    await clickAvancar(page).catch(() => {});
    await page.waitForTimeout(600);

    // ---------------------------
    // PASSO 4: Emitir
    // ---------------------------
    onLog("Confirmando emissão...");
    await clickByText(page, "Emitir NFS-e").catch(() => {});
    await clickByText(page, "Emitir").catch(() => {});

    await page.waitForTimeout(1200);

    const body = await page.textContent("body").catch(() => "");
    const sucesso = /gerada com sucesso/i.test(body || "");
    const status = sucesso ? "emitida" : "pendente";

    let numeroNota = "";
    const mNum = (body || "").match(/Número\s*[:\-]?\s*([0-9]+)/i);
    if (mNum?.[1]) numeroNota = mNum[1];

    const mensagem = sucesso
      ? `NFS-e emitida com sucesso${numeroNota ? ` • Nº ${numeroNota}` : ""}`
      : "Fluxo executou, mas não consegui confirmar a emissão na tela (precisa ajuste fino).";

    if (!sucesso) await saveDebug(page, "emitir-nao-confirmou");

    // ✅ pausa bem pequena só pra conferir (evita ficar MUITO devagar)
    if (!resolvedHeadless) await page.waitForTimeout(2500);

    await context.close();
    await browser.close();
    return { status, numeroNota, mensagem, pdfPath: "", xmlPath: "" };
  } catch (e) {
    onLog(`ERRO no bot: ${e?.message || e}`);
    await saveDebug(page, "emitir-erro");

    // pausa curta pra você ver a tela do erro
    if (!resolvedHeadless) {
      try {
        await page.waitForTimeout(2500);
      } catch {}
    }

    await context.close().catch(() => {});
    await browser.close().catch(() => {});
    return {
      status: "erro",
      numeroNota: "",
      mensagem: e?.message || "Erro ao emitir NFS-e",
      pdfPath: "",
      xmlPath: "",
    };
  }
}
