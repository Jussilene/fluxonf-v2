// src/routes/webhooks.hotmart.routes.js
import express from "express";
import crypto from "crypto";
import nodemailer from "nodemailer";

import {
  findUserByEmail,
  createUser,
  setUserActiveByEmail,
} from "../utils/usersStore.js";

import { hashPassword } from "../auth/password.js";

const router = express.Router();

/* =========================
   Helpers Hotmart
========================= */

function pickEmail(payload) {
  return (
    payload?.data?.buyer?.email ||
    payload?.buyer?.email ||
    payload?.purchase?.buyer?.email ||
    ""
  )
    .toString()
    .trim()
    .toLowerCase();
}

function pickName(payload) {
  return (
    payload?.data?.buyer?.name ||
    payload?.data?.buyer?.first_name ||
    ""
  )
    .toString()
    .trim();
}

function pickEvent(payload) {
  return (payload?.event || "")
    .toString()
    .trim()
    .toUpperCase();
}

function genTempPassword() {
  // senha temporária segura
  return crypto.randomBytes(9).toString("base64url"); // ~12 chars
}

/* =========================
   SMTP (igual ao passwordReset)
========================= */

function makeTransport() {
  const host = String(process.env.SMTP_HOST || "").trim();
  const port = Number(process.env.SMTP_PORT || 587);
  const secure = String(process.env.SMTP_SECURE || "false") === "true";
  const user = String(process.env.SMTP_USER || "").trim();
  const pass = String(process.env.SMTP_PASS || "").trim();

  return nodemailer.createTransport({
    host,
    port,
    secure,
    auth: { user, pass },
    requireTLS: !secure,
    tls: {
      rejectUnauthorized: false,
      minVersion: "TLSv1.2",
    },
  });
}

async function sendHotmartAccessEmail({ to, name, tempPassword }) {
  const base = (process.env.APP_BASE_URL || "http://localhost:3000").replace(/\/+$/, "");
  const loginUrl = `${base}/index.html`;

  const transport = makeTransport();

  // Se SMTP estiver mal configurado, não quebra o webhook
  try {
    await transport.verify();
  } catch (e) {
    console.error("[SMTP] verify FALHOU (Hotmart email):", e?.message || e);
    return { ok: false };
  }

  const safeName = name || "Olá";

  const info = await transport.sendMail({
    from: process.env.MAIL_FROM || process.env.SMTP_USER,
    to,
    subject: "Seu acesso ao FluxoNF foi liberado ✅",
    html: `
      <div style="font-family:Arial,sans-serif;line-height:1.5">
        <h2>Acesso liberado ✅</h2>
        <p>${safeName}, seu acesso ao <b>FluxoNF</b> foi criado com sucesso.</p>

        <p><b>Link de acesso:</b><br/>
        <a href="${loginUrl}">${loginUrl}</a></p>

        <p><b>Login:</b> ${to}<br/>
        <b>Senha temporária:</b> ${tempPassword}</p>

        <p style="margin-top:14px">
          <b>Importante:</b> ao entrar, vá em <b>Configurações → Alterar senha</b> para definir uma senha só sua.
        </p>

        <p style="color:#666;font-size:12px;margin-top:18px">
          Se você não realizou essa compra, ignore este e-mail.
        </p>
      </div>
    `,
  });

  console.log("[SMTP] Hotmart access email OK:", {
    messageId: info?.messageId,
    accepted: info?.accepted,
    rejected: info?.rejected,
  });

  return { ok: true };
}

/* =========================
   Webhook Hotmart
========================= */

router.post("/webhooks/hotmart", async (req, res) => {
  try {
    const hottok =
      req.headers["x-hotmart-hottok"] ||
      req.headers["x-hotmart-hottoken"] ||
      "";

    if (String(hottok).trim() !== String(process.env.HOTMART_HOTTOK || "").trim()) {
      console.warn("❌ Webhook Hotmart rejeitado: HOTTOK inválido");
      return res.status(401).json({ ok: false });
    }

    const payload = req.body || {};
    const event = pickEvent(payload);
    const email = pickEmail(payload);
    const name = pickName(payload);

    console.log("✅ Hotmart webhook recebido:", { event, email });

    if (!email) {
      console.warn("⚠️ Webhook sem email de comprador. Ignorando.");
      return res.json({ ok: true });
    }

    // ✅ 1) APROVADO: cria user NORMAL ou reativa
    if (event === "PURCHASE_APPROVED") {
      const exists = findUserByEmail(email);

      // se já existe, só reativa e não reenvia email (evita spam)
      if (exists) {
        setUserActiveByEmail(email, true);
        console.log("✅ Usuário reativado:", email);
        return res.json({ ok: true });
      }

      const tempPassword = genTempPassword();
      const passwordHash = await hashPassword(tempPassword); // ✅ mesmo hash do auth

      createUser({
        email,
        passwordHash,
        role: "user", // ✅ GARANTE: NÃO ADM
        plan: "STARTER",
        planValue: 49.9,
      });

      console.log("✅ Usuário criado (role=user):", email);

      // ✅ envia email com acesso
      await sendHotmartAccessEmail({ to: email, name, tempPassword });

      return res.json({ ok: true });
    }

    // ✅ 2) BLOQUEIOS
    if (
      event === "PURCHASE_CANCELED" ||
      event === "PURCHASE_REFUNDED" ||
      event === "PURCHASE_CHARGEBACK"
    ) {
      const u = setUserActiveByEmail(email, false);
      if (u) console.log("⛔ Usuário bloqueado:", email);
      else console.log("⚠️ Evento recebido, mas usuário não encontrado:", email);
      return res.json({ ok: true });
    }

    return res.json({ ok: true });
  } catch (err) {
    console.error("[HOTMART] error:", err?.message || err);
    return res.status(500).json({ ok: false });
  }
});

export default router;
