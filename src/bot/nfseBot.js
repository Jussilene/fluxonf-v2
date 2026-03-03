// src/bot/nfseBot.js
import { chromium } from "playwright";
import fs from "fs";
import path from "path";
import { registrarExecucao } from "../models/historico.model.js";

const NFSE_PORTAL_URL =
  process.env.NFSE_PORTAL_URL ||
  // âœ… MantÃ©m o link exatamente como vocÃª informou (ReturnUrl com %2F maiÃºsculo)
  "https://www.nfse.gov.br/EmissorNacional/Login?ReturnUrl=%2FEmissorNacional";

const isLinux = process.platform === "linux";

// âœ… AJUSTE MÃNIMO #1: controlar headless via .env (NFSE_HEADLESS / NFS_HEADLESS)
// - NFSE_HEADLESS=0 -> abre navegador
// - NFSE_HEADLESS=1 -> headless
async function launchNFSEBrowser() {
  const raw = String(process.env.NFSE_HEADLESS ?? process.env.NFS_HEADLESS ?? "").trim();
  const slowMoRaw = String(process.env.NFSE_SLOWMO_MS ?? "").trim();
  const slowMo = /^\d+$/.test(slowMoRaw) ? Number(slowMoRaw) : 0;

  let headless;
  if (raw === "0" || raw.toLowerCase() === "false") headless = false;
  else if (raw === "1" || raw.toLowerCase() === "true") headless = true;
  else headless = isLinux ? true : false;

  console.log(
    `[BOT] Browser launch: headless=${headless} (NFSE_HEADLESS=${process.env.NFSE_HEADLESS || ""} | NFS_HEADLESS=${process.env.NFS_HEADLESS || ""})`
  );

  return await chromium.launch({
    headless,
    slowMo,
    args: isLinux
      ? ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
      : [],
  });
}

// --------------------
// Helpers de data
// --------------------
function formatDateBrFromISO(isoDate) {
  if (!isoDate) return null;
  const [year, month, day] = isoDate.split("-");
  if (!year || !month || !day) return null;
  return `${day}/${month}/${year}`;
}

function buildPeriodoLabel(dataInicial, dataFinal) {
  const di = dataInicial ? formatDateBrFromISO(dataInicial) : null;
  const df = dataFinal ? formatDateBrFromISO(dataFinal) : null;

  if (!di && !df) return "N/D atÃ© N/D";
  if (di && !df) return `${di} atÃ© N/D`;
  if (!di && df) return `N/D atÃ© ${df}`;
  return `${di} atÃ© ${df}`;
}

function parseBrDateToDate(str) {
  if (!str) return null;
  const match = str.match(/(\d{2})\/(\d{2})\/(\d{4})/);
  if (!match) return null;
  const [, dd, mm, yyyy] = match;
  return new Date(Number(yyyy), Number(mm) - 1, Number(dd));
}

function parseIsoToDate(iso) {
  if (!iso) return null;
  const [yyyy, mm, dd] = iso.split("-");
  if (!yyyy || !mm || !dd) return null;
  return new Date(Number(yyyy), Number(mm) - 1, Number(dd));
}

function periodKey(dataInicial, dataFinal) {
  const di = (dataInicial || "sem-data").slice(0, 10);
  const df = (dataFinal || "sem-data").slice(0, 10);
  return `${di}_a_${df}`;
}

// --------------------
// Logger
// --------------------
function createLogger(onLog) {
  const logs = [];
  const pushLog = (msg) => {
    logs.push(msg);
    if (onLog) onLog(msg);
  };
  return { logs, pushLog };
}

// --------------------
// FS helpers
// --------------------
function ensureDir(dirPath) {
  try {
    fs.mkdirSync(dirPath, { recursive: true });
  } catch (err) {
    console.error("[NFSE] Erro ao criar pasta:", dirPath, err);
  }
}

function extractCnpjLike(str) {
  if (!str) return null;
  const match = str.match(/(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})/);
  if (!match) return null;
  return match[1].replace(/\D/g, "");
}

// --------------------
// âœ… estrutura por job (SEM criar pastas de tipo automaticamente)
// downloads/jobs/<periodo>/<timestamp>  (apenas jobDir)
// --------------------
function buildJobPaths(pastaDestino, dataInicial, dataFinal) {
  const baseDir = path.resolve(process.cwd(), pastaDestino || "downloads");
  const jobsRoot = path.join(baseDir, "jobs", periodKey(dataInicial, dataFinal));
  ensureDir(jobsRoot);

  const jobDir = path.join(jobsRoot, String(Date.now()));
  ensureDir(jobDir);

  // â— nÃ£o cria Emitidas/Recebidas/Canceladas aqui
  const emitidasDir = path.join(jobDir, "Emitidas");
  const recebidasDir = path.join(jobDir, "Recebidas");
  const canceladasDir = path.join(jobDir, "Canceladas");

  return { baseDir, jobDir, emitidasDir, recebidasDir, canceladasDir };
}

function getTipoDirFromRoot(rootJobDir, tipoNota) {
  if (tipoNota === "recebidas") return path.join(rootJobDir, "Recebidas");
  if (tipoNota === "canceladas") return path.join(rootJobDir, "Canceladas");
  return path.join(rootJobDir, "Emitidas");
}
function resolveA1CertConfig(params = {}, pushLog = () => {}) {
  try {
    const certPathHint = String(params?.certPfxPath || params?.pfxPath || process.env.NFSE_CERT_PFX_PATH || "").trim();
    const certPassHint = String(params?.certPassphrase || params?.passphrase || process.env.NFSE_CERT_PFX_PASS || "").trim();
    const useA1 =
      params?.usarCertificadoA1 === true ||
      String(params?.authType || "").toLowerCase().includes("certificado") ||
      (!!certPathHint && !!certPassHint);
    if (!useA1) return null;

    const portalOrigin =
      String(params?.certOrigin || process.env.NFSE_CERT_ORIGIN || "https://www.nfse.gov.br").trim() ||
      "https://www.nfse.gov.br";

    let pfxPath = certPathHint;
    const passphrase = certPassHint;
    const empresaId = String(params?.empresaId || "").trim();
    const userEmail = String(params?.usuarioEmail || "").trim().toLowerCase();

    if (!pfxPath) {
      pushLog("[BOT] Modo certificado A1 ativo, mas sem caminho PFX configurado.");
      return null;
    }

    if (!fs.existsSync(pfxPath)) {
      const base = path.basename(pfxPath);
      if (base) {
        const userSlug = userEmail.replace(/[^a-z0-9._-]/g, "_");
        const userDir = path.resolve(process.cwd(), "certs", userSlug);
        const byIdPfx = empresaId ? path.join(userDir, `captura-empresa-${empresaId}.pfx`) : "";
        const byIdP12 = empresaId ? path.join(userDir, `captura-empresa-${empresaId}.p12`) : "";
        const byName = path.join(userDir, base);
        const candidates = [byIdPfx, byIdP12, byName].filter(Boolean);
        const found = candidates.find((c) => fs.existsSync(c));
        if (found) {
          pfxPath = found;
          pushLog(`[BOT] Certificado A1 resolvido automaticamente: ${pfxPath}`);
        }
      }
    }

    if (!fs.existsSync(pfxPath)) {
      pushLog(`[BOT] PFX informado nao existe: ${pfxPath}`);
      return null;
    }

    const pfxBuffer = fs.readFileSync(pfxPath);
    return {
      portalOrigin,
      passphrase,
      pfxPath,
      clientCertificates: [
        {
          origin: portalOrigin,
          pfx: pfxBuffer,
          passphrase: passphrase || undefined,
        },
      ],
    };
  } catch (err) {
    pushLog(`[BOT] Falha ao preparar certificado A1: ${err?.message || err}`);
    return null;
  }
}

// ---------------------------------------------------------------------
// âœ… Canceladas robusto
// Agora funciona com coluna "SituaÃ§Ã£o" sendo ÃCONE (sem texto):
// lÃª title/tooltip/aria-label/data-original-title ou HTML interno.
// ---------------------------------------------------------------------
function normalizeText(s = "") {
  return String(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // remove acentos
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function normalizeStatus(s = "") {
  return normalizeText(s);
}

async function findSituacaoColumnIndex(page) {
  try {
    const headers = page.locator("table thead tr th");
    const count = await headers.count().catch(() => 0);

    for (let i = 0; i < count; i++) {
      const h = normalizeStatus(await headers.nth(i).innerText().catch(() => ""));
      if (h.includes("SITUA") || h.includes("STATUS")) {
        return i;
      }
    }
  } catch {
    // ignore
  }
  return -1;
}

async function readSituacaoSignalsFromCell(cellHandle) {
  // tenta pegar texto + atributos (tooltip) do prÃ³prio TD e de elementos internos
  try {
    const payload = await cellHandle.evaluate((cell) => {
      const pickAttrs = (el) => {
        if (!el) return [];
        const out = [];
        const attrs = [
          "title",
          "aria-label",
          "data-original-title",
          "data-bs-original-title",
          "data-tooltip",
        ];
        for (const a of attrs) {
          const v = el.getAttribute && el.getAttribute(a);
          if (v) out.push(v);
        }
        return out;
      };

      const texts = [];
      const attrs = [];

      // texto do TD
      try {
        const t = (cell.innerText || "").trim();
        if (t) texts.push(t);
      } catch {}

      // attrs do TD
      attrs.push(...pickAttrs(cell));

      // procurar elementos com tooltip
      const els = cell.querySelectorAll(
        "[title],[aria-label],[data-original-title],[data-bs-original-title],[data-tooltip]"
      );

      // limita para nÃ£o explodir
      const max = Math.min(els.length, 15);
      for (let i = 0; i < max; i++) {
        attrs.push(...pickAttrs(els[i]));
      }

      // html bruto (Ã s vezes tem 'cancelada' em classes/labels)
      const html = cell.innerHTML || "";

      return { texts, attrs, html };
    });

    return {
      texts: Array.isArray(payload?.texts) ? payload.texts : [],
      attrs: Array.isArray(payload?.attrs) ? payload.attrs : [],
      html: typeof payload?.html === "string" ? payload.html : "",
    };
  } catch {
    return { texts: [], attrs: [], html: "" };
  }
}

async function isRowCanceladaBySituacaoIdx(rowHandle, situacaoIdx) {
  if (situacaoIdx < 0) {
    return { isCancelled: false, statusRaw: "", statusNorm: "" };
  }

  try {
    const cells = await rowHandle.$$("td");
    const cell = cells?.[situacaoIdx] || null;
    if (!cell) return { isCancelled: false, statusRaw: "", statusNorm: "" };

    // 1) tenta texto direto
    const rawText = ((await cell.innerText().catch(() => "")) || "").trim();

    // 2) se nÃ£o tem texto, lÃª tooltip/attrs/html
    const signals = await readSituacaoSignalsFromCell(cell);

    const allParts = [
      rawText,
      ...(signals.texts || []),
      ...(signals.attrs || []),
      signals.html || "",
    ]
      .filter(Boolean)
      .map((x) => String(x));

    const joinedRaw = allParts.join(" | ").trim();
    const norm = normalizeStatus(joinedRaw);

    // regra de cancelada:
    const isCancelled =
      (norm.includes("CANCELAD") ||
        norm.includes("NFS-E CANCELAD") ||
        norm.includes("NFSE CANCELAD")) &&
      !norm.includes("CANCELAR");

    return {
      isCancelled,
      statusRaw: joinedRaw || rawText || "",
      statusNorm: norm || "",
    };
  } catch {
    return { isCancelled: false, statusRaw: "", statusNorm: "" };
  }
}

// ---------------------------------------------------------------------
// PDF robusto (mantido)
// ---------------------------------------------------------------------
function makeAbsoluteUrl(base, href) {
  try {
    return new URL(href, base).toString();
  } catch {
    return href;
  }
}

async function safeClickHandle(handle) {
  try {
    await handle.scrollIntoViewIfNeeded().catch(() => {});
  } catch {}
  await handle
    .evaluate((el) => {
      if (!el) return;
      try {
        el.scrollIntoView({ block: "center", inline: "center" });
      } catch {}
      if (el instanceof HTMLElement) el.click();
      else el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
    })
    .catch(async () => {
      await handle.click({ force: true }).catch(() => {});
    });
}

async function baixarPdfPorRequest({ context, page, urlPdf, destinoPdf, log }) {
  if (!urlPdf) throw new Error("URL do PDF nÃ£o disponÃ­vel para fallback.");

  const abs = makeAbsoluteUrl(page.url(), urlPdf);
  log?.(`[BOT] (PDF) Tentando fallback via request autenticado: ${abs}`);

  const resp = await context.request.get(abs).catch(() => null);
  if (!resp) throw new Error("Falha ao executar request.get() para o PDF.");

  const ok = resp.ok();
  const status = resp.status();

  if (!ok) {
    throw new Error(`Request do PDF falhou (status ${status}).`);
  }

  const buffer = await resp.body().catch(() => null);
  if (!buffer) throw new Error("Request OK, mas nÃ£o consegui ler body() do PDF.");

  fs.mkdirSync(path.dirname(destinoPdf), { recursive: true });
  fs.writeFileSync(destinoPdf, buffer);

  log?.(`[BOT] PDF (via request) salvo: ${destinoPdf}`);
  return true;
}

async function baixarPdfRobusto({ context, page, clickPdfOption, destinoPdf, log, pdfLinkHandle }) {
  fs.mkdirSync(path.dirname(destinoPdf), { recursive: true });

  const downloadPromise = page.waitForEvent("download", { timeout: 15000 }).catch(() => null);
  const popupPromise = page.waitForEvent("popup", { timeout: 15000 }).catch(() => null);
  const responsePromise = page
    .waitForResponse(
      (r) => {
        const ct = (r.headers()["content-type"] || "").toLowerCase();
        return ct.includes("application/pdf");
      },
      { timeout: 15000 }
    )
    .catch(() => null);

  await clickPdfOption();

  const first = await Promise.race([
    downloadPromise.then((d) => ({ type: "download", d })),
    popupPromise.then((p) => ({ type: "popup", p })),
    responsePromise.then((r) => ({ type: "response", r })),
    new Promise((r) => setTimeout(() => r({ type: "timeout" }), 16000)),
  ]);

  if (first.type === "response" && first.r) {
    const resp = first.r;
    if (!resp.ok()) throw new Error(`Response do PDF nÃ£o OK (status ${resp.status()})`);

    const buffer = await resp.body().catch(() => null);
    if (!buffer) throw new Error("Response abriu, mas nÃ£o consegui ler o body() do PDF.");

    fs.writeFileSync(destinoPdf, buffer);
    log?.(`[BOT] PDF (via response) salvo: ${destinoPdf}`);
    return true;
  }

  if (first.type === "popup" && first.p) {
    const popup = first.p;

    await popup.waitForLoadState("domcontentloaded", { timeout: 15000 }).catch(() => {});
    await popup.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => {});

    const buffer = await popup.pdf({ format: "A4", printBackground: true }).catch(() => null);

    if (buffer) {
      fs.writeFileSync(destinoPdf, buffer);
      log?.(`[BOT] PDF (via popup.pdf) salvo: ${destinoPdf}`);
      await popup.close().catch(() => {});
      return true;
    }

    const respPdf = await popup
      .waitForResponse(
        (r) => (r.headers()["content-type"] || "").toLowerCase().includes("application/pdf"),
        { timeout: 12000 }
      )
      .catch(() => null);

    if (respPdf && respPdf.ok()) {
      const buf2 = await respPdf.body().catch(() => null);
      if (buf2) {
        fs.writeFileSync(destinoPdf, buf2);
        log?.(`[BOT] PDF (via response no popup) salvo: ${destinoPdf}`);
        await popup.close().catch(() => {});
        return true;
      }
    }

    await popup.close().catch(() => {});
    let href = null;
    try {
      href = pdfLinkHandle ? await pdfLinkHandle.getAttribute("href") : null;
    } catch {}
    return await baixarPdfPorRequest({ context, page, urlPdf: href, destinoPdf, log });
  }

  if (first.type === "download" && first.d) {
    const download = first.d;

    const failure = await download.failure().catch(() => null);

    if (failure) {
      if (String(failure).toLowerCase().includes("canceled")) {
        const resp = await responsePromise.catch(() => null);
        if (resp && resp.ok()) {
          const buffer = await resp.body().catch(() => null);
          if (buffer) {
            fs.writeFileSync(destinoPdf, buffer);
            log?.(`[BOT] PDF (via response apÃ³s cancelamento) salvo: ${destinoPdf}`);
            return true;
          }
        }

        const pop = await popupPromise.catch(() => null);
        if (pop) {
          await pop.waitForLoadState("domcontentloaded", { timeout: 12000 }).catch(() => {});
          const buffer = await pop.pdf({ format: "A4", printBackground: true }).catch(() => null);
          if (buffer) {
            fs.writeFileSync(destinoPdf, buffer);
            log?.(`[BOT] PDF (via popup apÃ³s cancelamento) salvo: ${destinoPdf}`);
            await pop.close().catch(() => {});
            return true;
          }
          await pop.close().catch(() => {});
        }

        let href = null;
        try {
          href = pdfLinkHandle ? await pdfLinkHandle.getAttribute("href") : null;
        } catch {}

        const urlFallback = href || (download.url ? download.url() : null) || null;

        return await baixarPdfPorRequest({
          context,
          page,
          urlPdf: urlFallback,
          destinoPdf,
          log,
        });
      }

      throw new Error(`Falha no download do PDF: ${failure}`);
    }

    await download.saveAs(destinoPdf);
    log?.(`[BOT] PDF salvo: ${destinoPdf}`);
    return true;
  }

  let href = null;
  try {
    href = pdfLinkHandle ? await pdfLinkHandle.getAttribute("href") : null;
  } catch {}

  if (href) {
    return await baixarPdfPorRequest({ context, page, urlPdf: href, destinoPdf, log });
  }

  throw new Error("NÃ£o houve evento de download/popup/response para o PDF (timeout).");
}

// ---------------------------------------------------------------------
// Helper: clicar e capturar arquivo usando evento de download do Playwright
// âœ… AJUSTE DE PASTAS: sÃ³ cria a pasta de destino quando realmente vai salvar
// ---------------------------------------------------------------------
async function clickAndCaptureFile({
  page,
  element,
  finalDir,
  tipoNota,
  pushLog,
  extPreferida,
  arquivoIndexRef,
  linhaIndex,
}) {
  try {
    const [download] = await Promise.all([
      page.waitForEvent("download", { timeout: 25000 }).catch(() => null),
      element.evaluate((el) => {
        if (el instanceof HTMLElement) {
          el.click();
        } else {
          el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
        }
      }),
    ]);

    if (!download) {
      pushLog(
        `[BOT] Aviso: nÃ£o foi possÃ­vel identificar um download ${
          extPreferida || "PDF/XML"
        } apÃ³s o clique na linha ${linhaIndex}.`
      );
      return false;
    }

    const failure = await download.failure().catch(() => null);
    if (failure) {
      throw new Error(`Falha no download (${extPreferida || "arquivo"}): ${failure}`);
    }

    let originalName = download.suggestedFilename() || "arquivo";
    originalName = originalName.replace(/[/\\]/g, "_");

    let ext = path.extname(originalName).toLowerCase();
    const expectedExt =
      extPreferida === "pdf" ? ".pdf" : extPreferida === "xml" ? ".xml" : null;

    // âœ… Ajuste: se foi pedido XML, mas veio PDF (ou vice-versa), NÃƒO salva (evita misturar)
    if (expectedExt && ext && ext !== expectedExt) {
      pushLog(
        `[BOT] Aviso: download inesperado na linha ${linhaIndex}. Esperado "${expectedExt}", mas veio "${ext}" (arquivo: "${originalName}"). Ignorando para nÃ£o misturar.`
      );
      return false;
    }

    // Se nÃ£o vier extensÃ£o, tenta assumir pela preferida
    if (!ext) {
      ext = expectedExt || ".bin";
      originalName += ext;
    }

    const cnpj = extractCnpjLike(originalName) || extractCnpjLike(download.url()) || null;

    arquivoIndexRef.value += 1;
    const index = arquivoIndexRef.value;

    const tipoSlug =
      tipoNota === "recebidas"
        ? "recebidas"
        : tipoNota === "canceladas"
        ? "canceladas"
        : "emitidas";

    const cnpjParte = cnpj || `linha${linhaIndex}`;
    const newName = `${tipoSlug}-${cnpjParte}-${index}${ext}`;
    const savePath = path.join(finalDir, newName);

    // âœ… cria sÃ³ aqui, quando realmente vai salvar algo
    ensureDir(finalDir);

    await download.saveAs(savePath);

    pushLog(
      `[BOT] Arquivo #${index} capturado na linha ${linhaIndex}. Original: "${originalName}" -> Novo nome: "${newName}". Caminho final: ${savePath}`
    );

    return true;
  } catch (e) {
    pushLog(`[BOT] Erro ao clicar/capturar arquivo na linha ${linhaIndex}: ${e.message}`);
    return false;
  }
}

// ---------------------------------------------------------------------
// âœ… NavegaÃ§Ã£o por tipo
// - Emitidas: /Notas/Emitidas
// - Recebidas: /Notas/Recebidas
// - Canceladas: ficam na lista de Emitidas (SituaÃ§Ã£o=cancelada)
// ---------------------------------------------------------------------
async function navigateToTipo(page, tipoNota, pushLog) {
  const emitidasUrl =
    process.env.NFSE_EMITIDAS_URL || "https://www.nfse.gov.br/EmissorNacional/Notas/Emitidas";

  const recebidasUrl =
    process.env.NFSE_RECEBIDAS_URL || "https://www.nfse.gov.br/EmissorNacional/Notas/Recebidas";

  if (tipoNota === "recebidas") {
    try {
      pushLog('[BOT] Tentando clicar no Ã­cone "NFS-e Recebidas"...');
      await page.click('[title="NFS-e Recebidas"]', { timeout: 8000 });
      await page.waitForURL("**/Notas/Recebidas", { timeout: 15000 }).catch(() => {});
      pushLog("[BOT] Tela de Recebidas aberta.");
      return;
    } catch {
      pushLog("[BOT] Falha ao clicar Recebidas. Tentando URL direta...");
      await page.goto(recebidasUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
      pushLog(`[BOT] URL atual: ${page.url()}`);
      return;
    }
  }

  if (tipoNota === "canceladas") {
    pushLog(
      '[BOT] Tipo "canceladas": portal costuma listar canceladas dentro de "Emitidas" (coluna SituaÃ§Ã£o). Abrindo Emitidas...'
    );
    await page
      .goto(emitidasUrl, { waitUntil: "domcontentloaded", timeout: 60000 })
      .catch(async () => {
        try {
          await page.click('[title="NFS-e Emitidas"]', { timeout: 8000 });
        } catch {}
      });
    pushLog(`[BOT] URL atual (canceladas via emitidas): ${page.url()}`);
    return;
  }

  // emitidas (default)
  try {
    pushLog('[BOT] Tentando clicar no Ã­cone "NFS-e Emitidas"...');
    await page.click('[title="NFS-e Emitidas"]', { timeout: 8000 });
    await page.waitForURL("**/Notas/Emitidas", { timeout: 15000 }).catch(() => {});
    pushLog("[BOT] Tela de Emitidas aberta.");
    return;
  } catch {
    pushLog("[BOT] Falha ao clicar Emitidas. Tentando URL direta...");
    await page.goto(emitidasUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
    pushLog(`[BOT] URL atual: ${page.url()}`);
    return;
  }
}

// ---------------------------------------------------------------------
// âœ… Filtro de datas (portal -> fallback antigo -> fallback tabela)
// ---------------------------------------------------------------------
async function applyDateFilterIfExists(page, dataInicial, dataFinal, pushLog) {
  let usarFiltroNaTabela = false;

  if (dataInicial || dataFinal) {
    // 1) filtro real do portal (labels + Filtrar)
    try {
      const diBr = formatDateBrFromISO(dataInicial);
      const dfBr = formatDateBrFromISO(dataFinal);

      const inputDataInicial = page.locator(
        `xpath=//label[contains(normalize-space(.),"Data Inicial")]/following::input[1]`
      );
      const inputDataFinal = page.locator(
        `xpath=//label[contains(normalize-space(.),"Data Final")]/following::input[1]`
      );

      const hasIni = (await inputDataInicial.count().catch(() => 0)) > 0;
      const hasFim = (await inputDataFinal.count().catch(() => 0)) > 0;

      pushLog(
        `[BOT] Campos de data detectados? Data Inicial=${hasIni ? "sim" : "nÃ£o"} | Data Final=${
          hasFim ? "sim" : "nÃ£o"
        }`
      );

      if ((hasIni && diBr) || (hasFim && dfBr)) {
        if (hasIni && diBr) {
          await inputDataInicial.first().click();
          await page.keyboard.press("Control+A");
          await page.keyboard.type(diBr);
        }

        if (hasFim && dfBr) {
          await inputDataFinal.first().click();
          await page.keyboard.press("Control+A");
          await page.keyboard.type(dfBr);
        }

        const btnFiltrar = page.getByRole("button", { name: /filtrar/i });

        await Promise.all([
          page.waitForLoadState("networkidle").catch(() => {}),
          btnFiltrar.click().catch(() => {}),
        ]);

        await page.waitForTimeout(250);

        pushLog(
          `[BOT] Filtro de perÃ­odo aplicado no portal (Data Inicial/Data Final): ${buildPeriodoLabel(
            dataInicial,
            dataFinal
          )}.`
        );

        return { usarFiltroNaTabela: false };
      }
    } catch (err) {
      pushLog(
        `[BOT] NÃ£o consegui aplicar filtro do portal (labels/Filtrar): ${err.message}. Vou tentar inputs alternativos e/ou filtrar pela coluna "EmissÃ£o".`
      );
    }

    // 2) fallback antigo (ids/names)
    try {
      const diBr = formatDateBrFromISO(dataInicial);
      const dfBr = formatDateBrFromISO(dataFinal);

      await page.waitForTimeout(150);

      const dataInicialInput =
        (await page.$(
          'input[id*="DataInicio"], input[name*="DataInicio"], input[id*="DataEmissaoInicial"], input[name*="DataEmissaoInicial"]'
        )) ||
        (await page.$('input[id*="DataCompetenciaInicio"], input[name*="DataCompetenciaInicio"]'));

      const dataFinalInput =
        (await page.$(
          'input[id*="DataFim"], input[name*="DataFim"], input[id*="DataEmissaoFinal"], input[name*="DataEmissaoFinal"]'
        )) || (await page.$('input[id*="DataCompetenciaFim"], input[name*="DataCompetenciaFim"]'));

      if ((dataInicialInput && diBr) || (dataFinalInput && dfBr)) {
        if (dataInicialInput && diBr) await dataInicialInput.fill(diBr);
        if (dataFinalInput && dfBr) await dataFinalInput.fill(dfBr);

        const botaoPesquisar =
          (await page.$(
            'button[type="submit"]:has-text("Pesquisar"), button:has-text("Consultar"), button:has-text("Buscar")'
          )) ||
          (await page.$(
            'input[type="submit"][value*="Pesquisar"], input[type="submit"][value*="Consultar"], input[type="submit"][value*="Buscar"]'
          ));

        if (botaoPesquisar) {
          await botaoPesquisar.click();
          await page.waitForTimeout(450);
          pushLog(
            `[BOT] Filtro de perÃ­odo aplicado pelos campos (fallback antigo): ${buildPeriodoLabel(
              dataInicial,
              dataFinal
            )}.`
          );
          return { usarFiltroNaTabela: false };
        }

        usarFiltroNaTabela = true;
      } else {
        usarFiltroNaTabela = true;
      }
    } catch (err2) {
      usarFiltroNaTabela = true;
      pushLog(
        `[BOT] Erro ao aplicar filtro por campos (fallback antigo): ${err2.message}. Vou filtrar pela coluna "EmissÃ£o".`
      );
    }
  }

  if (usarFiltroNaTabela && (dataInicial || dataFinal)) {
    pushLog("[BOT] NÃ£o localizei campos de data. Vou filtrar pela coluna 'EmissÃ£o' da tabela.");
  }

  return { usarFiltroNaTabela };
}

async function getTableSignature(page) {
  const count = await page.locator("table tbody tr").count().catch(() => 0);
  const first = await page
    .locator("table tbody tr")
    .first()
    .innerText()
    .catch(() => "");
  return `${page.url()}|${count}|${String(first || "").trim()}`;
}

async function goToNextTablePage(page, pushLog, currentPage = 1) {
  const before = await getTableSignature(page);

  const clickInfo = await page
    .evaluate((cp) => {
      const isVisible = (el) => {
        if (!el) return false;
        const r = el.getBoundingClientRect();
        const st = window.getComputedStyle(el);
        return (
          r.width > 0 &&
          r.height > 0 &&
          st.display !== "none" &&
          st.visibility !== "hidden" &&
          st.opacity !== "0"
        );
      };
      const isDisabled = (el) => {
        if (!el) return true;
        if (el.matches("[disabled], [aria-disabled='true']")) return true;
        if (el.classList.contains("disabled")) return true;
        const disabledParent = el.closest(".disabled, [aria-disabled='true']");
        return !!disabledParent;
      };

      const all = Array.from(document.querySelectorAll("a, button")).filter(
        (el) => isVisible(el) && !isDisabled(el)
      );

      // Rodapé da lista (onde fica o paginador) para evitar clicar em elementos aleatórios.
      const bottomCandidates = all.filter(
        (el) => el.getBoundingClientRect().top >= window.innerHeight * 0.55
      );

      const nextPageText = String(Number(cp) + 1);
      let target =
        bottomCandidates.find((el) => (el.textContent || "").trim() === nextPageText) || null;
      let strategy = `page-${nextPageText}`;

      if (!target) {
        const nextLabels = ["›", ">", "»", "Próxima", "Proxima", "Próximo", "Proximo", "Next"];
        target =
          bottomCandidates.find((el) =>
            nextLabels.includes((el.textContent || "").trim())
          ) || null;
        strategy = "next-control";
      }

      if (!target) return { clicked: false, reason: "next-target-not-found" };

      target.click();
      return { clicked: true, strategy, label: (target.textContent || "").trim() };
    }, currentPage)
    .catch(() => ({ clicked: false, reason: "evaluate-error" }));

  if (!clickInfo?.clicked) return false;

  await Promise.race([
    page
      .waitForFunction(
        (prevSig) => {
          const rows = document.querySelectorAll("table tbody tr");
          const first = rows.length ? (rows[0].innerText || "").trim() : "";
          const sig = `${location.href}|${rows.length}|${first}`;
          return sig !== prevSig;
        },
        before,
        { timeout: 20000 }
      )
      .catch(() => null),
    page.waitForLoadState("networkidle", { timeout: 20000 }).catch(() => null),
    page.waitForTimeout(250),
  ]).catch(() => null);

  let after = await getTableSignature(page);
  if (after === before) {
    // fallback extra: tenta avançar pelo controle "›" e revalida mudança
    const secondTry = await page
      .evaluate(() => {
        const isVisible = (el) => {
          if (!el) return false;
          const r = el.getBoundingClientRect();
          const st = window.getComputedStyle(el);
          return r.width > 0 && r.height > 0 && st.display !== "none" && st.visibility !== "hidden";
        };
        const isDisabled = (el) => {
          if (!el) return true;
          if (el.matches("[disabled], [aria-disabled='true']")) return true;
          if (el.classList.contains("disabled")) return true;
          const disabledParent = el.closest(".disabled, [aria-disabled='true']");
          return !!disabledParent;
        };
        const nextLabels = ["›", ">", "»", "Próxima", "Proxima", "Próximo", "Proximo", "Next"];
        const nodes = Array.from(document.querySelectorAll("a,button")).filter(
          (el) => isVisible(el) && !isDisabled(el)
        );
        const bottom = nodes.filter((el) => el.getBoundingClientRect().top >= window.innerHeight * 0.45);
        const target = bottom.find((el) => nextLabels.includes((el.textContent || "").trim()));
        if (!target) return false;
        target.click();
        return true;
      })
      .catch(() => false);

    if (!secondTry) return false;

    await Promise.race([
      page
        .waitForFunction(
          (prevSig) => {
            const rows = document.querySelectorAll("table tbody tr");
            const first = rows.length ? (rows[0].innerText || "").trim() : "";
            const sig = `${location.href}|${rows.length}|${first}`;
            return sig !== prevSig;
          },
          before,
          { timeout: 20000 }
        )
        .catch(() => null),
      page.waitForLoadState("networkidle", { timeout: 20000 }).catch(() => null),
      page.waitForTimeout(250),
    ]).catch(() => null);

    after = await getTableSignature(page);
    if (after === before) return false;
  }

  pushLog(
    `[BOT] Avancando para pagina ${currentPage + 1} (${clickInfo.strategy}${clickInfo.label ? `: ${clickInfo.label}` : ""})...`
  );
  return true;
}

// ---------------------------------------------------------------------
// MODO SIMULAÃ‡ÃƒO (mantido) â€“ âœ… AJUSTE: nÃ£o cria pastas de tipo automaticamente
// ---------------------------------------------------------------------
async function runManualDownloadSimulado(params = {}) {
  const { onLog } = params || {};
  const { logs, pushLog } = createLogger(onLog);

  const {
    dataInicial,
    dataFinal,
    tipoNota,
    baixarXml,
    baixarPdf,
    pastaDestino,
    empresaId,
    empresaNome,
    modoExecucao,
    jobDir,

    // âœ… AJUSTE: usuÃ¡rio para histÃ³rico (multiusuÃ¡rio)
    usuarioEmail,
    usuarioNome,
  } = params;

  const periodoLabel = buildPeriodoLabel(dataInicial, dataFinal);

  const jobPaths = buildJobPaths(pastaDestino, dataInicial, dataFinal);

  const rootJobDir = jobDir ? path.resolve(process.cwd(), jobDir) : jobPaths.jobDir;
  ensureDir(rootJobDir);

  const finalDir = getTipoDirFromRoot(rootJobDir, tipoNota);

  pushLog(
    `[BOT] (Debug) Modo SIMULAÃ‡ÃƒO ativo. NFSE_USE_PORTAL = "${
      process.env.NFSE_USE_PORTAL || "nÃ£o definido"
    }".`
  );
  pushLog("[BOT] Iniciando robÃ´ (SIMULAÃ‡ÃƒO)...");
  pushLog(`[BOT] Tipo de nota: ${tipoNota}`);
  pushLog(`[BOT] PerÃ­odo: ${periodoLabel}`);
  pushLog(`[BOT] Pasta final: ${finalDir}`);

  try {
    registrarExecucao({
      // âœ… AJUSTE: salvar usuÃ¡rio no histÃ³rico
      usuarioEmail: usuarioEmail || null,
      usuarioNome: usuarioNome || null,

      empresaId: empresaId || null,
      empresaNome: empresaNome || null,
      tipo: modoExecucao || "manual",
      totalArquivos: 0,
      status: "simulado",
      detalhes: `SimulaÃ§Ã£o - tipoNota=${tipoNota}, perÃ­odo=${periodoLabel}.`,
      logs,
    });
  } catch (err) {
    console.error("[BOT] Erro ao registrar histÃ³rico (simulaÃ§Ã£o):", err);
  }

  return {
    logs,
    paths: { jobDir: rootJobDir, finalDir },
    ok: true,
    totalArquivos: 0,
    error: null,
  };
}

// ---------------------------------------------------------------------
// MODO PORTAL (Playwright)
// âœ… AJUSTE: sÃ³ cria a pasta (Emitidas/Recebidas/Canceladas) se houver download real
// ---------------------------------------------------------------------
async function runManualDownloadPortal(params = {}) {
  const { onLog } = params || {};
  const { logs, pushLog } = createLogger(onLog);

  const {
    dataInicial,
    dataFinal,
    tipoNota,
    baixarXml,
    baixarPdf,
    pastaDestino,
    login: loginParam,
    senha: senhaParam,
    empresaId,
    empresaNome,
    modoExecucao,
    jobDir,

    // âœ… AJUSTE: usuÃ¡rio para histÃ³rico (multiusuÃ¡rio)
    usuarioEmail,
    usuarioNome,
  } = params;

  const periodoLabel = buildPeriodoLabel(dataInicial, dataFinal);

  const certCfg = resolveA1CertConfig(params, pushLog);
  const certRequested =
    params?.usarCertificadoA1 === true ||
    String(params?.authType || "").toLowerCase().includes("certificado");
  const login = loginParam || process.env.NFSE_USER;
  const senha = senhaParam || process.env.NFSE_PASSWORD;
  const hasCreds = !!(login && senha);
  const useA1 = !!certCfg;

  if (certRequested && !useA1 && !hasCreds) {
    throw new Error("Modo certificado A1 selecionado, mas certificado invÃ¡lido/ausente. Verifique arquivo e senha do certificado.");
  }

  if (!hasCreds && !useA1) {
    pushLog("[BOT] Login/senha nÃ£o informados e sem certificado A1. Voltando para SIMULAÃ‡ÃƒO.");
    const simResult = await runManualDownloadSimulado({ ...params, modoExecucao, onLog });
    return { logs: logs.concat(simResult.logs), paths: simResult.paths };
  }

  const jobPaths = buildJobPaths(pastaDestino, dataInicial, dataFinal);

  const rootJobDir = jobDir ? path.resolve(process.cwd(), jobDir) : jobPaths.jobDir;
  ensureDir(rootJobDir);

  // âœ… NÃƒO cria as 3 pastas aqui.
  // SÃ³ define qual seria a pasta final (e cria quando realmente salvar algum arquivo).
  const finalDir = getTipoDirFromRoot(rootJobDir, tipoNota);

  pushLog(`[BOT] JobDir: ${rootJobDir}`);
  pushLog(`[BOT] Tipo: ${tipoNota} | Pasta final: ${finalDir}`);

  const browser = await launchNFSEBrowser();
  let useA1Active = useA1;
  let context = null;
  try {
    context = await browser.newContext({
      acceptDownloads: true,
      ...(useA1Active ? { clientCertificates: certCfg.clientCertificates } : {}),
    });
  } catch (ctxErr) {
    if (useA1Active && hasCreds) {
      pushLog(`[BOT] Falha ao carregar certificado A1 (${ctxErr.message}). Continuando com login/senha.`);
      useA1Active = false;
      context = await browser.newContext({ acceptDownloads: true });
    } else {
      throw ctxErr;
    }
  }
  const page = await context.newPage();

  const arquivoIndexRef = { value: 0 };
  let teveErro = false;
  let erroExecucao = "";

  try {
    // 1) Abrir login
    pushLog("[BOT] Abrindo portal nacional da NFS-e...");
    await page.goto(NFSE_PORTAL_URL, { waitUntil: "domcontentloaded" });
    pushLog("[BOT] PÃ¡gina de login carregada.");

    // 2) Autenticacao: se houver certificado e credenciais, tenta A1 primeiro e faz fallback para login/senha.
    const isStillOnLogin = () => /\/Login(\?|$)/i.test(page.url());

    const attemptLoginWithCreds = async () => {
      const tentativasCredenciais = [];
      const seenCreds = new Set();
      const addTentativa = (l, s, label = "") => {
        const loginNorm = String(l || "").trim();
        const senhaNorm = String(s || "");
        if (!loginNorm || !senhaNorm) return;
        const key = `${loginNorm}||${senhaNorm}`;
        if (seenCreds.has(key)) return;
        seenCreds.add(key);
        tentativasCredenciais.push({ login: loginNorm, senha: senhaNorm, label });
      };

      const loginRaw = String(login || "").trim();
      const loginDigits = loginRaw.replace(/\D/g, "");
      addTentativa(loginRaw, senha, "empresa");
      if (loginDigits && loginDigits !== loginRaw) addTentativa(loginDigits, senha, "empresa-sem-mascara");

      const envLoginRaw = String(process.env.NFSE_USER || "").trim();
      const envSenhaRaw = String(process.env.NFSE_PASSWORD || "");
      const envLoginDigits = envLoginRaw.replace(/\D/g, "");
      addTentativa(envLoginRaw, envSenhaRaw, "env");
      if (envLoginDigits && envLoginDigits !== envLoginRaw) addTentativa(envLoginDigits, envSenhaRaw, "env-sem-mascara");

      for (let i = 0; i < tentativasCredenciais.length; i += 1) {
        const tentativa = tentativasCredenciais[i];
        if (!isStillOnLogin()) {
          await page.goto(NFSE_PORTAL_URL, { waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {});
        }

        await page.fill('input[name="Login"], input[id="Login"], input[type="text"]', "");
        await page.fill('input[name="Senha"], input[id="Senha"], input[type="password"]', "");
        await page.type('input[name="Login"], input[id="Login"], input[type="text"]', tentativa.login, { delay: 20 });
        pushLog(
          `[BOT] Login preenchido${i > 0 ? ` (tentativa ${i + 1}/${tentativasCredenciais.length}, ${tentativa.label})` : ""}.`
        );
        await page.type('input[name="Senha"], input[id="Senha"], input[type="password"]', tentativa.senha, { delay: 15 });
        pushLog("[BOT] Senha preenchida.");
        await page.click(
          'button[type="submit"], input[type="submit"], button:has-text("Entrar"), button:has-text("Acessar")',
          { timeout: 12000, noWaitAfter: true }
        );
        pushLog("[BOT] BotÃ£o de login clicado. Aguardando...");
        await Promise.race([
          page.waitForURL((url) => !/\/Login(\?|$)/i.test(url.toString()), { timeout: 22000 }),
          page.waitForTimeout(22000),
        ]).catch(() => {});
        if (!isStillOnLogin()) return true;
        await page.waitForTimeout(800);
      }
      pushLog("[BOT] Login nÃ£o autenticado apÃ³s tentativas.");
      return false;
    };

    const attemptA1 = async () => {
      if (!useA1Active) return false;
      pushLog(`[BOT] Modo Certificado A1 ativo (PFX: ${certCfg.pfxPath}). Tentando autenticar por certificado...`);
      const certSelectorsRetry = [
        'a[href*="/EmissorNacional/Certificado"]',
        'a[href*="/Certificado"]',
        "a.img-certificado",
        'a:has-text("Acesso via certificado digital")',
      ];
      for (let tentativa = 1; tentativa <= 2; tentativa++) {
        let clicked = false;
        for (const sel of certSelectorsRetry) {
          const link = page.locator(sel).first();
          const exists = await link.count().catch(() => 0);
          if (!exists) continue;
          await Promise.all([
            page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 }).catch(() => null),
            link.click({ timeout: 5000 }).catch(() => null),
          ]).catch(() => null);
          clicked = true;
          pushLog(`[BOT] Retentativa A1 #${tentativa}: clique no acesso por certificado (${sel}).`);
          break;
        }
        if (!clicked) {
          const certUrlRetry = new URL("/EmissorNacional/Certificado", page.url()).toString();
          pushLog(`[BOT] Retentativa A1 #${tentativa}: link nao encontrado. Tentando URL direta: ${certUrlRetry}`);
          await page.goto(certUrlRetry, { waitUntil: "domcontentloaded", timeout: 25000 }).catch(() => {});
        }
        await Promise.race([
          page.waitForURL((url) => !/\/Login(\?|$)/i.test(url.toString()), { timeout: 25000 }),
          page.waitForTimeout(25000),
        ]).catch(() => {});
        if (!isStillOnLogin()) {
          pushLog(`[BOT] Retentativa A1 #${tentativa}: autenticacao concluida.`);
          return true;
        }
      }
      return false;
    };

    if (useA1Active) {
      const okA1 = await attemptA1();
      if (!okA1 && hasCreds) {
        pushLog("[BOT] A1 falhou. Tentando fallback por login/senha...");
        await attemptLoginWithCreds();
      }
    } else if (hasCreds) {
      await attemptLoginWithCreds();
    }

    // âœ… AJUSTE MÃNIMO #2: se ainda estiver no Login, salva evidÃªncias e aborta (pra nÃ£o gerar ZIP vazio)
    if (isStillOnLogin()) {
      pushLog("[BOT] (Alerta) Ainda na tela de login. Vou salvar debug e abortar.");
      let invalidCreds = false;
      try {
        const txt = await page.textContent("body").catch(() => "");
        invalidCreds = /usu[aá]rio\s*e\/ou\s*senha\s*inv[aá]lidos?/i.test(String(txt || ""));
      } catch {}

      try {
        const evidDir = path.join(rootJobDir, "_debug");
        ensureDir(evidDir);

        const png = path.join(evidDir, `${tipoNota}-still-login.png`);
        const html = path.join(evidDir, `${tipoNota}-still-login.html`);

        await page.screenshot({ path: png, fullPage: true }).catch(() => {});
        const content = await page.content().catch(() => "");
        fs.writeFileSync(html, content || "", "utf8");

        pushLog(`[BOT] Debug salvo: ${png}`);
        pushLog(`[BOT] Debug salvo: ${html}`);
      } catch {}

      if (!useA1Active && invalidCreds) {
        throw new Error("Usuário e/ou senha inválidos no portal NFS-e. Atualize as credenciais da empresa.");
      }
      throw new Error(useA1Active ? "NÃ£o autenticou com certificado A1 (permaneceu em /Login). Verifique PFX/senha e debug." : "NÃ£o autenticou no portal (permaneceu em /Login). Verifique o print/HTML em _debug.");
    } else {
      pushLog("[BOT] Login OK (URL mudou).");
    }

    // 3) Navega para o tipo
    await navigateToTipo(page, tipoNota, pushLog);

    // 4) Filtro de data
    const { usarFiltroNaTabela } = await applyDateFilterIfExists(page, dataInicial, dataFinal, pushLog);

    // âœ… SituaÃ§Ã£o/Status:
    // - canceladas: usado para INCLUIR somente canceladas
    // - emitidas: usado para EXCLUIR canceladas do meio
    let situacaoIdx = -1;

    if (tipoNota === "canceladas") {
      situacaoIdx = await findSituacaoColumnIndex(page);
      if (situacaoIdx < 0) {
        pushLog(
          '[BOT] âš ï¸ NÃ£o encontrei coluna "SituaÃ§Ã£o/Status". Para evitar erro, canceladas ficarÃ¡ vazio nesta execuÃ§Ã£o.'
        );
      } else {
        pushLog(`[BOT] Coluna SituaÃ§Ã£o/Status detectada (idx=${situacaoIdx}).`);
      }
    }

    // âœ… NOVO (sem quebrar nada): em "emitidas", pula linhas canceladas
    let situacaoIdxEmitidas = -1;
    if (tipoNota === "emitidas") {
      situacaoIdxEmitidas = await findSituacaoColumnIndex(page);
      if (situacaoIdxEmitidas >= 0) {
        pushLog(
          `[BOT] Coluna SituaÃ§Ã£o/Status detectada para filtro de emitidas (idx=${situacaoIdxEmitidas}). Canceladas serÃ£o ignoradas em Emitidas.`
        );
      } else {
        pushLog(
          `[BOT] (Info) NÃ£o encontrei coluna SituaÃ§Ã£o/Status para filtro de emitidas. Seguindo sem filtrar canceladas em Emitidas.`
        );
      }
    }

    // 5) Validar â€œnenhum registroâ€
    const textoPagina = (await page.textContent("body").catch(() => "")) || "";
    if (textoPagina.includes("Nenhum registro encontrado")) {
      pushLog("[BOT] Nenhuma nota encontrada (Nenhum registro encontrado).");
      pushLog(`[BOT] PÃ¡gina atual validada: ${page.url()}`);
    } else {
      const dataInicialDate = dataInicial ? parseIsoToDate(dataInicial) : null;
      const dataFinalDate = dataFinal ? parseIsoToDate(dataFinal) : null;

      if (!baixarXml && !baixarPdf) {
        pushLog("[BOT] Nenhum formato selecionado (XML/PDF). Nada serÃ¡ baixado.");
      } else {
        let linhaIndex = 0;
        let paginaAtual = 1;

        while (true) {
          await page.waitForSelector("table tbody tr", { timeout: 30000 }).catch(() => {});
          const rowHandles = await page.$$("table tbody tr");
          const rowCount = rowHandles.length;

          pushLog(
            paginaAtual === 1
              ? `[BOT] Tabela carregada. Linhas: ${rowCount}.`
              : `[BOT] Tabela carregada (pÃ¡gina ${paginaAtual}). Linhas: ${rowCount}.`
          );

          if (rowCount === 0) {
            if (paginaAtual === 1) pushLog("[BOT] Nenhuma nota na tabela para o perÃ­odo.");
            break;
          }

          for (const row of rowHandles) {
            linhaIndex += 1;

            try {
              const allCells = await row.$$("td");

              // fallback por coluna EmissÃ£o
              if (usarFiltroNaTabela && (dataInicialDate || dataFinalDate)) {
                const emissaoCell = allCells[0] || null;
                let emissaoTexto = "";

                if (emissaoCell) {
                  emissaoTexto = ((await emissaoCell.innerText().catch(() => "")) || "").trim();
                }

                const emissaoDate = parseBrDateToDate(emissaoTexto);

                if (emissaoDate) {
                  if (
                    (dataInicialDate && emissaoDate < dataInicialDate) ||
                    (dataFinalDate && emissaoDate > dataFinalDate)
                  ) {
                    pushLog(
                      `[BOT] Linha ${linhaIndex}: emissÃ£o ${emissaoTexto} fora do perÃ­odo. Ignorando.`
                    );
                    continue;
                  }
                }
              }

              // âœ… CANCELADAS: filtra pela coluna SituaÃ§Ã£o/Status (Ã­cone/tooltip)
              if (tipoNota === "canceladas") {
                if (situacaoIdx < 0) continue;

                const r = await isRowCanceladaBySituacaoIdx(row, situacaoIdx);

                pushLog(
                  `[BOT] Linha ${linhaIndex}: SituaÃ§Ã£o="${r.statusNorm || "?"}" (raw="${
                    r.statusRaw || ""
                  }")`
                );

                if (!r.isCancelled) continue;

                pushLog(`[BOT] Linha ${linhaIndex}: nota CANCELADA detectada. Processando...`);
              }

              // âœ… EMITIDAS: ignorar canceladas no meio
              if (tipoNota === "emitidas" && situacaoIdxEmitidas >= 0) {
                const rEmi = await isRowCanceladaBySituacaoIdx(row, situacaoIdxEmitidas);
                if (rEmi.isCancelled) {
                  pushLog(
                    `[BOT] Linha ${linhaIndex}: (Emitidas) nota CANCELADA detectada na lista. Ignorando para nÃ£o misturar.`
                  );
                  continue;
                }
              }

              const acaoCell = allCells.length > 0 ? allCells[allCells.length - 1] : null;

              if (!acaoCell) {
                pushLog(`[BOT] Linha ${linhaIndex}: nÃ£o encontrei coluna de aÃ§Ãµes.`);
                continue;
              }

              const menuWrapper = (await acaoCell.$(".menu-suspenso-tabela")) || acaoCell;
              let menu =
                (await menuWrapper.$(".menu-content")) || (await menuWrapper.$(".list-group"));
              let menuOpened = false;
              const ensureMenu = async () => {
                if (menuOpened && menu) return true;
                const trigger = await menuWrapper.$(".icone-trigger");
                if (!trigger) {
                  pushLog(
                    `[BOT] Linha ${linhaIndex}: nÃ£o encontrei o Ã­cone do menu (.icone-trigger).`
                  );
                  return false;
                }
                await trigger.click({ force: true });
                menuOpened = true;
                await page.waitForTimeout(20);
                menu =
                  (await menuWrapper.$(".menu-content")) || (await menuWrapper.$(".list-group"));
                if (!menu) {
                  pushLog(`[BOT] Linha ${linhaIndex}: menu suspenso nÃ£o encontrado apÃ³s clique.`);
                  return false;
                }
                return true;
              };

              // XML
              if (baixarXml) {
                let xmlLink =
                  (await menu.$('a:has-text("Download XML")')) ||
                  (await menu.$('a:has-text("XML")')) ||
                  (await menu.$('a[href*="DownloadXml"]')) ||
                  (await menu.$('a[href*="xml"]'));

                if (!xmlLink) {
                  const menuOk = await ensureMenu();
                  if (menuOk) {
                    xmlLink =
                      (await menu.$('a:has-text("Download XML")')) ||
                      (await menu.$('a:has-text("XML")')) ||
                      (await menu.$('a[href*="DownloadXml"]')) ||
                      (await menu.$('a[href*="xml"]'));
                  }
                }

                if (xmlLink) {
                  pushLog(`[BOT] Linha ${linhaIndex}: baixando XML...`);
                  await clickAndCaptureFile({
                    page,
                    element: xmlLink,
                    finalDir,
                    tipoNota,
                    pushLog,
                    extPreferida: "xml",
                    arquivoIndexRef,
                    linhaIndex,
                  });
                } else {
                  pushLog(`[BOT] Linha ${linhaIndex}: XML nÃ£o encontrado no menu.`);
                }
              }

              // PDF
              if (baixarPdf) {
                let pdfLink =
                  (await menu.$('a:has-text("Download DANFS-e")')) ||
                  (await menu.$('a:has-text("Download DANFS")')) ||
                  (await menu.$('a:has-text("DANFS-e")')) ||
                  (await menu.$('a:has-text("DANFS")')) ||
                  (await menu.$('a:has-text("PDF")')) ||
                  (await menu.$('a[href*="DANFS"]')) ||
                  (await menu.$('a[href*="pdf"]'));

                if (!pdfLink) {
                  const menuOk = await ensureMenu();
                  if (menuOk) {
                    pdfLink =
                      (await menu.$('a:has-text("Download DANFS-e")')) ||
                      (await menu.$('a:has-text("Download DANFS")')) ||
                      (await menu.$('a:has-text("DANFS-e")')) ||
                      (await menu.$('a:has-text("DANFS")')) ||
                      (await menu.$('a:has-text("PDF")')) ||
                      (await menu.$('a[href*="DANFS"]')) ||
                      (await menu.$('a[href*="pdf"]'));
                  }
                }

                if (pdfLink) {
                  pushLog(`[BOT] Linha ${linhaIndex}: baixando PDF...`);

                  const tipoSlug =
                    tipoNota === "recebidas"
                      ? "recebidas"
                      : tipoNota === "canceladas"
                      ? "canceladas"
                      : "emitidas";

                  const destinoPdfPreview = `${tipoSlug}-linha${linhaIndex}-${arquivoIndexRef.value + 1}.pdf`;
                  const destinoPdf = path.join(finalDir, destinoPdfPreview);

                  try {
                    // Fast-path: tenta request autenticado direto quando o href do DANFS/PDF estiver disponivel.
                    let ok = false;
                    let hrefDireto = null;
                    try {
                      hrefDireto = await pdfLink.getAttribute("href");
                    } catch {}

                    if (hrefDireto && /download|danfs|pdf/i.test(hrefDireto)) {
                      try {
                        ok = await baixarPdfPorRequest({
                          context,
                          page,
                          urlPdf: hrefDireto,
                          destinoPdf,
                          log: (m) => pushLog(m),
                        });
                        if (ok) {
                          pushLog(`[BOT] Linha ${linhaIndex}: PDF obtido por fast-path (request autenticado).`);
                        }
                      } catch (fastErr) {
                        pushLog(
                          `[BOT] Linha ${linhaIndex}: fast-path PDF falhou (${fastErr.message}). Tentando modo robusto...`
                        );
                      }
                    }

                    if (!ok) {
                      ok = await baixarPdfRobusto({
                        context,
                        page,
                        destinoPdf,
                        log: (m) => pushLog(m),
                        pdfLinkHandle: pdfLink,
                        clickPdfOption: async () => {
                          await safeClickHandle(pdfLink);
                        },
                      });
                    }

                    if (ok) {
                      arquivoIndexRef.value += 1;
                      pushLog(`[BOT] PDF registrado como arquivo #${arquivoIndexRef.value}.`);
                    }
                  } catch (e) {
                    pushLog(`[BOT] Erro ao capturar PDF na linha ${linhaIndex}: ${e.message}`);
                  }
                } else {
                  pushLog(`[BOT] Linha ${linhaIndex}: PDF/DANFS nÃ£o encontrado no menu.`);
                }
              }

              await page.waitForTimeout(5);
            } catch (linhaErr) {
              pushLog(`[BOT] Erro ao processar linha ${linhaIndex}: ${linhaErr.message}`);
            }
          }

          const avancouPagina = await goToNextTablePage(page, pushLog, paginaAtual);
          if (!avancouPagina) break;
          paginaAtual += 1;
        }
      }
    }

    pushLog(`[BOT] Finalizado (${tipoNota}). Total capturado: ${arquivoIndexRef.value}.`);
  } catch (err) {
    console.error("Erro no robÃ´ Playwright:", err);
    erroExecucao = err?.message || "Erro nÃ£o identificado";
    pushLog(`[BOT] ERRO: ${err.message}`);
    teveErro = true;
  } finally {
    await browser.close().catch(() => {});
    pushLog("[BOT] Navegador fechado.");

    try {
      registrarExecucao({
        // âœ… AJUSTE: salvar usuÃ¡rio no histÃ³rico
        usuarioEmail: usuarioEmail || null,
        usuarioNome: usuarioNome || null,

        empresaId: empresaId || null,
        empresaNome: empresaNome || null,
        tipo: modoExecucao || "manual",
        totalArquivos: arquivoIndexRef.value,
        status: teveErro ? "erro" : "sucesso",
        erros: teveErro ? [{ message: erroExecucao || "Verificar logs desta execuÃ§Ã£o" }] : null,
        detalhes: teveErro
          ? `Portal nacional - tipoNota=${tipoNota}, perÃ­odo=${periodoLabel}. Erro: ${erroExecucao || "nÃ£o informado"}.`
          : `Portal nacional - tipoNota=${tipoNota}, perÃ­odo=${periodoLabel}.`,
        logs,
      });
    } catch (histErr) {
      console.error("[BOT] Erro ao registrar histÃ³rico:", histErr);
    }
  }

  return {
    logs,
    paths: { jobDir: rootJobDir, finalDir },
    ok: !teveErro,
    totalArquivos: arquivoIndexRef.value,
    error: teveErro ? erroExecucao || "Falha na captura" : null,
  };
}

// ---------------------------------------------------------------------
// FunÃ§Ã£o usada pelo backend â€“ escolhe modo conforme .env
// ---------------------------------------------------------------------
export async function runManualDownload(params = {}) {
  const usePortal = process.env.NFSE_USE_PORTAL === "true";

  if (usePortal) {
    return runManualDownloadPortal({ ...params, modoExecucao: params.modoExecucao || "manual" });
  }

  return runManualDownloadSimulado({ ...params, modoExecucao: params.modoExecucao || "manual" });
}

// ---------------------------------------------------------------------
// ExecuÃ§Ã£o em LOTE
// Agora aceita processarTipos: ["emitidas","recebidas","canceladas"]
// âœ… AJUSTE: dentro de cada empresa, sÃ³ cria pasta do(s) tipo(s) realmente executado(s) e que baixou arquivo
// ---------------------------------------------------------------------
export async function runLoteDownload(empresas = [], options = {}) {
  const {
    onLog,
    baixarXml = true,
    baixarPdf = true,
    tipoNota = "emitidas",
    dataInicial,
    dataFinal,
    pastaDestino,
    processarTipos,

    // âœ… AJUSTE: usuÃ¡rio para histÃ³rico (multiusuÃ¡rio)
    usuarioEmail,
    usuarioNome,
  } = options || {};

  const { logs, pushLog } = createLogger(onLog);
  const usePortal = process.env.NFSE_USE_PORTAL === "true";

  // âœ… Ajuste: normaliza + remove duplicados
  const tiposRaw =
    Array.isArray(processarTipos) && processarTipos.length ? processarTipos : [tipoNota];
  const tipos = [...new Set(tiposRaw.map((t) => String(t).trim().toLowerCase()).filter(Boolean))];

  pushLog(`[BOT] Iniciando execuÃ§Ã£o em lote (${usePortal ? "REAL (portal)" : "SIMULAÃ‡ÃƒO"})...`);
  pushLog(`[BOT] Tipos no lote: ${tipos.join(", ")}`);
  pushLog(`[BOT] PerÃ­odo: ${buildPeriodoLabel(dataInicial, dataFinal)}`);

  if (!Array.isArray(empresas) || empresas.length === 0) {
    pushLog("[BOT] Nenhuma empresa cadastrada para executar em lote.");
    return { logs, paths: {} };
  }

  const loteJobPaths = buildJobPaths(pastaDestino, dataInicial, dataFinal);
  pushLog(`[BOT] Lote JobDir: ${loteJobPaths.jobDir}`);
  const resumo = {
    totalEmpresas: empresas.length,
    empresasOk: 0,
    empresasFalha: 0,
    empresasComArquivos: 0,
    totalArquivos: 0,
    falhas: [],
  };

  for (const emp of empresas) {
    try {
      pushLog("--------------------------------------------------------------");
      pushLog(`[BOT] Empresa: ${emp.nome} (CNPJ: ${emp.cnpj})`);

      const login = emp.loginPortal || emp.cnpj || null;
      const senha = emp.senhaPortal || null;
      const authType = String(emp.authType || "");
      const isCert = authType.toLowerCase().includes("certificado");
      const hasCreds = !!(login && senha);
      const empFalhas = [];
      let empArquivos = 0;

      if (usePortal && !hasCreds && !isCert) {
        const motivo = "Login/senha da empresa nÃ£o configurados.";
        pushLog(`[BOT] ${motivo} Pulando.`);
        empFalhas.push({ tipo: "autenticacao", motivo });
        resumo.empresasFalha += 1;
        resumo.falhas.push({ empresa: emp.nome || "", cnpj: emp.cnpj || "", motivos: empFalhas });
        continue;
      }

      // âœ… SÃ³ cria a pasta da empresa (nÃ£o cria Emitidas/Recebidas/Canceladas aqui)
      const empresaDir = path.join(
        loteJobPaths.jobDir,
        `${String(emp.nome || "empresa").replace(/[^\w\-]+/g, "_")}_${String(
          emp.cnpj || emp.id || ""
        ).slice(-8)}`
      );
      ensureDir(empresaDir);

      for (const t of tipos) {
        let result = null;
        if (usePortal) {
          try {
            result = await runManualDownloadPortal({
              dataInicial,
              dataFinal,
              tipoNota: t,
              baixarXml,
              baixarPdf,
              pastaDestino,
              login,
              senha,
              authType,
              usarCertificadoA1: isCert,
              certPfxPath: emp.certPfxPath || emp.pfxPath || null,
              certPassphrase: emp.certPassphrase || emp.passphrase || null,
              empresaId: emp.id || emp.cnpj,
              empresaNome: emp.nome,
              modoExecucao: "lote",
              onLog: (msg) => pushLog(msg),
              jobDir: empresaDir,

              // âœ… AJUSTE: repassa usuÃ¡rio para histÃ³rico
              usuarioEmail: usuarioEmail || null,
              usuarioNome: usuarioNome || null,
            });
          } catch (e) {
            const motivo = e?.message || "Erro inesperado ao executar captura.";
            pushLog(`[BOT] ERRO geral em ${emp.nome} (${t}): ${motivo}`);
            empFalhas.push({ tipo: t, motivo });
          }
        } else {
          result = await runManualDownloadSimulado({
            dataInicial,
            dataFinal,
            tipoNota: t,
            baixarXml,
            baixarPdf,
            pastaDestino,
            empresaId: emp.id || emp.cnpj,
            empresaNome: emp.nome,
            modoExecucao: "lote",
            onLog: (msg) => pushLog(msg),
            jobDir: empresaDir,

            // âœ… AJUSTE: repassa usuÃ¡rio para histÃ³rico
            usuarioEmail: usuarioEmail || null,
            usuarioNome: usuarioNome || null,
          });
        }

        if (result) {
          empArquivos += Number(result?.totalArquivos || 0);
          if (result?.ok === false) {
            empFalhas.push({ tipo: t, motivo: result?.error || "Falha na captura." });
          }
        }

        // Pequena pausa entre tipos/empresas para reduzir bloqueio do portal por muitas autenticações sequenciais.
        await new Promise((r) => setTimeout(r, 500));
      }

      resumo.totalArquivos += empArquivos;
      if (empArquivos > 0) resumo.empresasComArquivos += 1;
      if (empFalhas.length > 0) {
        resumo.empresasFalha += 1;
        resumo.falhas.push({ empresa: emp.nome || "", cnpj: emp.cnpj || "", motivos: empFalhas });
        const motivosTxt = empFalhas.map((f) => `${f.tipo}: ${f.motivo}`).join(" | ");
        pushLog(`[BOT] [ERRO] ${emp.nome} (${emp.cnpj}): ${motivosTxt}`);
      } else {
        resumo.empresasOk += 1;
      }
    } catch (fatalEmpErr) {
      const motivo = fatalEmpErr?.message || "Falha inesperada ao processar empresa.";
      pushLog(`[BOT] [ERRO] ${emp?.nome || "empresa"} (${emp?.cnpj || ""}): ${motivo}`);
      resumo.empresasFalha += 1;
      resumo.falhas.push({
        empresa: emp?.nome || "",
        cnpj: emp?.cnpj || "",
        motivos: [{ tipo: "fatal", motivo }],
      });
      continue;
    }
  }

  pushLog("--------------------------------------------------------------");
  pushLog(
    `[BOT] Resumo lote: ${resumo.empresasComArquivos} com arquivos, ${resumo.empresasFalha} com falha, total arquivos=${resumo.totalArquivos}.`
  );
  pushLog(`[BOT] ExecuÃ§Ã£o em lote finalizada.`);

  return { logs, paths: { jobDir: loteJobPaths.jobDir }, resumo };
}


