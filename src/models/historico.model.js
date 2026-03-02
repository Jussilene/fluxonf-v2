// src/models/historico.model.js
import db from "../db/sqlite.js";

export function registrarExecucao({
  // ✅ NOVO: multi-tenant (se vier, salva; se não vier, mantém null)
  usuarioEmail,
  usuarioNome,

  empresaId,
  empresaNome,
  tipo,
  qtdXml,
  qtdPdf,
  totalArquivos,
  status,
  erros,
  detalhes,
  logs,
}) {
  const stmt = db.prepare(`
    INSERT INTO historico_execucoes (
      usuarioEmail,
      usuarioNome,
      empresaId,
      empresaNome,
      tipo,
      dataHora,
      qtdXml,
      qtdPdf,
      totalArquivos,
      status,
      erros,
      detalhes,
      logsJson
    ) VALUES (
      @usuarioEmail,
      @usuarioNome,
      @empresaId,
      @empresaNome,
      @tipo,
      @dataHora,
      @qtdXml,
      @qtdPdf,
      @totalArquivos,
      @status,
      @erros,
      @detalhes,
      @logsJson
    )
  `);

  const dataHora = new Date().toISOString();

  stmt.run({
    usuarioEmail: usuarioEmail ? String(usuarioEmail).trim() : null,
    usuarioNome: usuarioNome ? String(usuarioNome).trim() : null,

    empresaId: empresaId || null,
    empresaNome: empresaNome || null,
    tipo: tipo || "manual",
    dataHora,
    qtdXml: qtdXml ?? 0,
    qtdPdf: qtdPdf ?? 0,
    totalArquivos: totalArquivos ?? (qtdXml ?? 0) + (qtdPdf ?? 0),
    status: status || "sucesso",
    erros: erros ? JSON.stringify(erros) : null,
    detalhes: detalhes || null,
    logsJson: Array.isArray(logs) ? JSON.stringify(logs.slice(-2000)) : null,
  });
}

export function listarHistorico({ empresaId, tipo, dataDe, dataAte } = {}) {
  let sql = `SELECT * FROM historico_execucoes WHERE 1=1`;
  const params = {};

  if (empresaId) {
    sql += ` AND empresaId = @empresaId`;
    params.empresaId = empresaId;
  }

  if (tipo) {
    sql += ` AND tipo = @tipo`;
    params.tipo = tipo;
  }

  if (dataDe) {
    sql += ` AND dataHora >= @dataDe`;
    params.dataDe = dataDe;
  }

  if (dataAte) {
    sql += ` AND dataHora <= @dataAte`;
    params.dataAte = dataAte;
  }

  sql += ` ORDER BY datetime(dataHora) DESC LIMIT 200`;

  const stmt = db.prepare(sql);
  return stmt.all(params);
}
