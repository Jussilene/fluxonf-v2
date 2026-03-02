// src/emissao/routes/historico.routes.js
import { Router } from "express";
import db from "../../db/sqlite.js";

const router = Router();

/**
 * GET /api/historico
 * Agora: por padrão filtra pelo usuário do header (x-user-email),
 * mas mantém compatibilidade com ?usuarioEmail=...
 */
router.get("/", (req, res) => {
  try {
    const headerUser = (req.userEmail || "").toString().trim();
    const usuarioEmailQuery = (req.query.usuarioEmail || "").toString().trim();

    const usuarioEmail = headerUser || usuarioEmailQuery;

    const limitRaw = Number(req.query.limit ?? 200);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(1000, limitRaw)) : 200;

    let rows = [];

    // ✅ FIX: sua tabela não tem "arquivosCount". Ela tem totalArquivos/qtdXml/qtdPdf.
    // então a gente faz alias para não quebrar o front que espera "arquivosCount".
    if (usuarioEmail) {
      const stmt = db.prepare(`
        SELECT
          id,
          usuarioEmail,
          usuarioNome,
          empresaId,
          empresaNome,
          tipo,
          dataHora,
          status,
          COALESCE(totalArquivos, (COALESCE(qtdXml,0) + COALESCE(qtdPdf,0))) AS arquivosCount,
          qtdXml,
          qtdPdf,
          totalArquivos,
          logsJson,
          detalhes
        FROM historico_execucoes
        WHERE usuarioEmail = ?
        ORDER BY id DESC
        LIMIT ?
      `);
      rows = stmt.all(usuarioEmail, limit);
    } else {
      const stmt = db.prepare(`
        SELECT
          id,
          usuarioEmail,
          usuarioNome,
          empresaId,
          empresaNome,
          tipo,
          dataHora,
          status,
          COALESCE(totalArquivos, (COALESCE(qtdXml,0) + COALESCE(qtdPdf,0))) AS arquivosCount,
          qtdXml,
          qtdPdf,
          totalArquivos,
          logsJson,
          detalhes
        FROM historico_execucoes
        ORDER BY id DESC
        LIMIT ?
      `);
      rows = stmt.all(limit);
    }

    // ✅ compatibilidade dupla: front antigo (ok/historico) e novo (success/items)
    return res.json({
      ok: true,
      historico: rows,
      success: true,
      items: rows,
    });
  } catch (err) {
    console.error("[HISTORICO] erro ao listar:", err);
    return res.status(500).json({
      ok: false,
      historico: [],
      success: false,
      items: [],
      error: "Erro ao listar histórico.",
    });
  }
});

/**
 * GET /api/historico/:id
 * (mantém compatibilidade; se tiver userEmail no header, só permite ver do próprio usuário)
 */
router.get("/:id", (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) {
      return res.status(400).json({ ok: false, success: false, error: "ID inválido." });
    }

    const row = db
      .prepare(`
        SELECT
          id,
          usuarioEmail,
          usuarioNome,
          empresaId,
          empresaNome,
          tipo,
          dataHora,
          status,
          COALESCE(totalArquivos, (COALESCE(qtdXml,0) + COALESCE(qtdPdf,0))) AS arquivosCount,
          qtdXml,
          qtdPdf,
          totalArquivos,
          logsJson,
          detalhes
        FROM historico_execucoes
        WHERE id = ?
        LIMIT 1
      `)
      .get(id);

    if (!row) return res.status(404).json({ ok: false, success: false, error: "Registro não encontrado." });

    const headerUser = (req.userEmail || "").toString().trim();
    if (headerUser && String(row.usuarioEmail || "").trim() !== headerUser) {
      return res.status(403).json({ ok: false, success: false, error: "Acesso negado." });
    }

    return res.json({ ok: true, success: true, item: row });
  } catch (err) {
    console.error("[HISTORICO] erro ao buscar:", err);
    return res.status(500).json({ ok: false, success: false, error: "Erro ao buscar histórico." });
  }
});

/**
 * DELETE /api/historico/:id
 * Remove registro do histórico (respeitando multiusuário via header).
 */
router.delete("/:id", (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) {
      return res.status(400).json({ ok: false, success: false, error: "ID inválido." });
    }

    const headerUser = (req.userEmail || "").toString().trim();
    let info;
    if (headerUser) {
      info = db
        .prepare(
          `DELETE FROM historico_execucoes
           WHERE id = ? AND usuarioEmail = ?`
        )
        .run(id, headerUser);
    } else {
      info = db
        .prepare(
          `DELETE FROM historico_execucoes
           WHERE id = ?`
        )
        .run(id);
    }

    if (!info || info.changes < 1) {
      return res.status(404).json({ ok: false, success: false, error: "Registro não encontrado." });
    }

    return res.json({ ok: true, success: true });
  } catch (err) {
    console.error("[HISTORICO] erro ao excluir:", err);
    return res.status(500).json({ ok: false, success: false, error: "Erro ao excluir histórico." });
  }
});

export default router;
