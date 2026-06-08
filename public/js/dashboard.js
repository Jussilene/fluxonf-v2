// public/js/dashboard.js

// =======================================================
// 0) Helpers básicos
// =======================================================
function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function $(id) {
  return document.getElementById(id);
}

function pickElByIds(ids = []) {
  for (const id of ids) {
    const el = document.getElementById(id);
    if (el) return el;
  }
  return null;
}

function getValByIds(ids = [], fallback = "") {
  const el = pickElByIds(ids);
  if (!el) return fallback;
  return (el.value ?? "").toString();
}

function setText(el, text) {
  if (el) el.textContent = text;
}

function isLocalhostHost() {
  const h = String(window.location.hostname || "").toLowerCase();
  return h === "localhost" || h === "127.0.0.1" || h === "::1";
}

// =======================================================
// 1) Sessão/Usuário: garante que tem usuário logado
//    - se não tiver localStorage, tenta /auth/me
// =======================================================
let currentUser = {};

function readLocalUser() {
  const rawUser = localStorage.getItem("nfseUser");
  if (!rawUser) return null;

  try {
    const parsed = JSON.parse(rawUser);
    if (parsed && typeof parsed === "object") return parsed;
    return { email: String(rawUser) };
  } catch {
    return { email: String(rawUser) };
  }
}

async function fetchMe() {
  try {
    const res = await fetch("/auth/me", { credentials: "include" });
    if (!res.ok) return null;
    const data = await res.json().catch(() => ({}));
    return data?.user || null;
  } catch {
    return null;
  }
}

async function ensureLoggedUserOrRedirect() {
  // tenta local
  const local = readLocalUser();
  if (local && (local.email || local.name || local.displayName)) {
    currentUser = local;
    return true;
  }

  // tenta sessão no backend
  const me = await fetchMe();
  if (me && (me.email || me.name)) {
    currentUser = {
      email: me.email,
      name: me.name,
      displayName: me.name,
      role: me.role,
    };
    try {
      localStorage.setItem("nfseUser", JSON.stringify(currentUser));
    } catch {}
    return true;
  }

  // sem sessão: volta pro login
  window.location.href = "/index.html";
  return false;
}

// ✅ headers padrão (multi-tenant)
function apiHeaders(extra = {}) {
  const email = (currentUser?.email || "").toString().trim();
  const h = { ...extra };
  if (email) h["x-user-email"] = email;
  return h;
}

// =======================================================
// 2) Logout
// =======================================================
async function doLogout() {
  try {
    await fetch("/auth/logout", { method: "POST", credentials: "include" });
  } catch {}

  try {
    localStorage.removeItem("nfseUser");
  } catch {}

  window.location.href = "/index.html";
}

// =======================================================
// 3) Ajustes visuais pedidos (menu do usuário + modal config)
//    (mantive sua lógica, só deixei mais estável)
// =======================================================
const userNameDisplay = $("userNameDisplay");
const userAvatar = $("userAvatar");

function hideTopLogoutIfAny() {
  const logoutBtnTop = $("logoutBtn");
  // Nao esconder o botao de sair quando ele pertence ao menu novo do usuario.
  if (logoutBtnTop && !logoutBtnTop.closest("#userMenuDropdown")) {
    logoutBtnTop.style.display = "none";
  }
}

let settingsModalEl = null;

function ensureUserMenuUI() {
  if (!userNameDisplay) return;
  if ($("userMenuBtn")) return;

  let nameToShow =
    currentUser.displayName ||
    currentUser.name ||
    (currentUser.email ? currentUser.email.split("@")[0] : "Usuário");

  // topo sem avatar
  if (userAvatar) userAvatar.style.display = "none";

  // wrapper
  const wrapper = document.createElement("div");
  wrapper.style.position = "relative";
  wrapper.style.display = "inline-flex";
  wrapper.style.alignItems = "center";
  wrapper.style.gap = "8px";

  // btn
  const btn = document.createElement("button");
  btn.type = "button";
  btn.id = "userMenuBtn";
  btn.className =
    "inline-flex items-center gap-2 px-2 py-1 rounded-lg hover:bg-slate-100 transition";
  btn.setAttribute("aria-haspopup", "true");
  btn.setAttribute("aria-expanded", "false");
  btn.title = "Menu do usuário";

  const label = document.createElement("span");
  label.id = "userMenuLabel";
  label.className = "text-sm font-medium";
  label.textContent = nameToShow;

  const burger = document.createElement("span");
  burger.className = "text-lg leading-none opacity-80";
  burger.textContent = "≡";

  btn.appendChild(label);
  btn.appendChild(burger);

  // dropdown claro
  const menu = document.createElement("div");
  menu.id = "userMenuDropdown";
  menu.className =
    "hidden absolute right-0 mt-2 w-56 rounded-xl border border-slate-200 bg-white shadow-lg overflow-hidden z-50";
  menu.style.top = "100%";

  menu.innerHTML = `
    <div class="px-4 py-3 border-b border-slate-100">
      <div class="text-sm font-semibold text-slate-900">${escapeHtml(nameToShow)}</div>
      <div class="text-xs text-slate-500">${escapeHtml(currentUser.email || "")}</div>
    </div>

    <button id="userMenuConfigBtn" class="w-full text-left px-4 py-3 text-sm hover:bg-slate-50">
      ⚙️ Configurações
    </button>

    <button id="userMenuLogoutBtn" class="w-full text-left px-4 py-3 text-sm hover:bg-slate-50">
      ↩️ Sair
    </button>
  `;

  const parent = userNameDisplay.parentElement || userNameDisplay;
  parent.insertBefore(wrapper, userNameDisplay);
  wrapper.appendChild(btn);

  userNameDisplay.style.display = "none";
  wrapper.appendChild(menu);

  function closeMenu() {
    menu.classList.add("hidden");
    btn.setAttribute("aria-expanded", "false");
  }
  function toggleMenu() {
    const willOpen = menu.classList.contains("hidden");
    if (willOpen) {
      menu.classList.remove("hidden");
      btn.setAttribute("aria-expanded", "true");
    } else {
      closeMenu();
    }
  }

  btn.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    toggleMenu();
  });

  // fecha ao clicar fora (bind único)
  if (!document.documentElement.dataset._userMenuDocClickBound) {
    document.documentElement.dataset._userMenuDocClickBound = "1";
    document.addEventListener("click", () => {
      const dropdown = $("userMenuDropdown");
      const btnNow = $("userMenuBtn");
      if (dropdown && btnNow) {
        dropdown.classList.add("hidden");
        btnNow.setAttribute("aria-expanded", "false");
      }
    });
  }

  // ações
  const logoutBtn = menu.querySelector("#userMenuLogoutBtn");
  const configBtn = menu.querySelector("#userMenuConfigBtn");

  if (logoutBtn) {
    logoutBtn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      closeMenu();
      doLogout();
    });
  }

  if (configBtn) {
    configBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      closeMenu();
      await openSettingsModal();
    });
  }
}

async function openSettingsModal() {
  if (settingsModalEl) {
    settingsModalEl.classList.remove("hidden");
    return;
  }

  const me = (await fetchMe()) || {
    name: currentUser.name || currentUser.displayName || "Usuário",
    email: currentUser.email || "",
    role: currentUser.role || "USER",
  };

  const isAdmin = false;

  settingsModalEl = document.createElement("div");
  settingsModalEl.id = "settingsModal";
  settingsModalEl.className =
    "fixed inset-0 z-[9999] flex items-stretch justify-stretch bg-black/50";

  // tela cheia, sempre claro
  settingsModalEl.innerHTML = `
    <div class="w-screen h-screen max-w-none max-h-none rounded-none bg-white border border-slate-200 shadow-xl overflow-hidden flex flex-col">
      <div class="flex items-center justify-between px-5 py-4 border-b border-slate-100">
        <div>
          <div class="text-base font-semibold text-slate-900">Configurações</div>
          <div class="text-xs text-slate-500">Gerencie seus dados e acessos</div>
        </div>
        <button id="settingsCloseBtn" class="px-3 py-1 rounded-lg hover:bg-slate-100">✕</button>
      </div>

      <div class="flex flex-1 min-h-0 overflow-hidden">
        <div class="w-72 border-r border-slate-100 p-3 space-y-2">
          <button data-tab="account" class="settings-tab w-full text-left px-3 py-2 rounded-lg hover:bg-slate-50 font-medium">
            👤 Minha conta
          </button>
          ${
            isAdmin
              ? `<button data-tab="users" class="settings-tab w-full text-left px-3 py-2 rounded-lg hover:bg-slate-50 font-medium">
                  🧩 Usuários
                </button>`
              : ""
          }
        </div>

        <div class="flex-1 p-5 overflow-y-auto">
          <div id="settingsTab-account" class="settings-panel">
            <div class="text-sm font-semibold mb-3 text-slate-900">Minha conta</div>

            <div class="grid gap-3 max-w-2xl">
              <label class="text-sm text-slate-700">
                Nome
                <input id="meName" class="mt-1 w-full px-3 py-2 rounded-lg border border-slate-200 bg-white"
                  value="${escapeHtml(me.name || "")}">
              </label>

              <label class="text-sm text-slate-700">
                Email
                <input id="meEmail" class="mt-1 w-full px-3 py-2 rounded-lg border border-slate-200 bg-white"
                  value="${escapeHtml(me.email || "")}">
              </label>

              <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
                <label class="text-sm text-slate-700">
                  Nova senha
                  <input id="mePass" type="password" class="mt-1 w-full px-3 py-2 rounded-lg border border-slate-200 bg-white"
                    placeholder="Digite a nova senha">
                </label>

                <label class="text-sm text-slate-700">
                  Confirmar senha
                  <input id="mePass2" type="password" class="mt-1 w-full px-3 py-2 rounded-lg border border-slate-200 bg-white"
                    placeholder="Confirme a nova senha">
                </label>
              </div>

              <div class="flex items-center gap-2">
                <button id="saveMeBtn" class="px-4 py-2 rounded-lg bg-slate-900 text-white hover:opacity-90">
                  Salvar
                </button>
                <span id="saveMeMsg" class="text-sm text-slate-600"></span>
              </div>
            </div>
          </div>

          ${
            isAdmin
              ? `
          <div id="settingsTab-users" class="settings-panel hidden">
            <div class="flex items-center justify-between">
              <div>
                <div class="text-sm font-semibold text-slate-900">Usuários</div>
                <div class="text-xs text-slate-500">Criar, desativar e resetar senha</div>
              </div>

              <div class="text-xs text-slate-500">
                <span id="usersCount">—</span>
              </div>
            </div>

            <div class="mt-4 grid md:grid-cols-2 gap-4">
              <div class="rounded-xl border border-slate-200 p-4">
                <div class="text-sm font-semibold mb-2">Criar usuário</div>

                <div class="grid gap-2">
                  <input id="newUserName" class="px-3 py-2 rounded-lg border border-slate-200 bg-white" placeholder="Nome">
                  <input id="newUserEmail" class="px-3 py-2 rounded-lg border border-slate-200 bg-white" placeholder="Email">
                  <input id="newUserPass" type="password" class="px-3 py-2 rounded-lg border border-slate-200 bg-white" placeholder="Senha temporária">
                  <select id="newUserRole" class="px-3 py-2 rounded-lg border border-slate-200 bg-white">
                    <option value="USER">USER</option>
                    <option value="ADMIN">ADMIN</option>
                  </select>

                  <button id="createUserBtn" class="mt-2 px-4 py-2 rounded-lg bg-slate-900 text-white hover:opacity-90">
                    Criar
                  </button>
                  <div id="createUserMsg" class="text-sm text-slate-600"></div>
                </div>
              </div>

              <div class="rounded-xl border border-slate-200 p-4">
                <div class="text-sm font-semibold mb-2">Lista</div>

                <div class="max-h-[70vh] overflow-auto border border-slate-100 rounded-lg">
                  <table class="w-full text-sm">
                    <thead class="sticky top-0 bg-slate-50 border-b border-slate-100">
                      <tr>
                        <th class="text-left px-3 py-2">Nome</th>
                        <th class="text-left px-3 py-2">Email</th>
                        <th class="text-left px-3 py-2">Role</th>
                        <th class="text-left px-3 py-2">Status</th>
                        <th class="text-left px-3 py-2">Ações</th>
                      </tr>
                    </thead>
                    <tbody id="usersTbody"></tbody>
                  </table>
                </div>

                <div id="usersMsg" class="text-sm text-slate-600 mt-2"></div>
              </div>
            </div>
          </div>
          `
              : ""
          }
        </div>
      </div>
    </div>
  `;

  document.body.appendChild(settingsModalEl);

  // close
  const closeBtn = settingsModalEl.querySelector("#settingsCloseBtn");
  if (closeBtn)
    closeBtn.addEventListener("click", () => settingsModalEl.classList.add("hidden"));

  // clique fora fecha
  settingsModalEl.addEventListener("click", (e) => {
    if (e.target === settingsModalEl) settingsModalEl.classList.add("hidden");
  });

  // tabs
  const tabBtns = settingsModalEl.querySelectorAll(".settings-tab");
  tabBtns.forEach((b) => {
    b.addEventListener("click", () => {
      const tab = b.getAttribute("data-tab");
      settingsModalEl
        .querySelectorAll(".settings-panel")
        .forEach((p) => p.classList.add("hidden"));
      const panel = settingsModalEl.querySelector(`#settingsTab-${tab}`);
      if (panel) panel.classList.remove("hidden");
    });
  });

  // salvar
  const saveMeBtn = settingsModalEl.querySelector("#saveMeBtn");
  if (saveMeBtn) {
    saveMeBtn.addEventListener("click", async () => {
      const msg = settingsModalEl.querySelector("#saveMeMsg");
      if (msg) msg.textContent = "Salvando...";

      const name = settingsModalEl.querySelector("#meName")?.value?.trim() || "";
      const email = settingsModalEl.querySelector("#meEmail")?.value?.trim() || "";
      const pass = settingsModalEl.querySelector("#mePass")?.value || "";
      const pass2 = settingsModalEl.querySelector("#mePass2")?.value || "";

      if (!name || !email) {
        if (msg) msg.textContent = "Preencha nome e email.";
        return;
      }
      if ((pass || pass2) && pass !== pass2) {
        if (msg) msg.textContent = "As senhas não conferem.";
        return;
      }

      // update profile
      try {
        const up = await fetch("/auth/update-profile", {
          method: "POST",
          credentials: "include",
          cache: "no-store",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name, email }),
        });
        const upJson = await up.json().catch(() => ({}));
        if (!up.ok || upJson?.ok === false) {
          if (msg) msg.textContent = upJson?.message || upJson?.error || "Erro ao salvar perfil.";
          return;
        }
      } catch {
        if (msg) msg.textContent = "Falha ao salvar no servidor (update-profile).";
        return;
      }

      // change password
      let changedPassword = false;
      if (pass) {
        try {
          const pw = await fetch("/auth/change-password", {
            method: "POST",
            credentials: "include",
            cache: "no-store",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ newPassword: pass }),
          });
          const pwJson = await pw.json().catch(() => ({}));
          if (!pw.ok || pwJson?.ok === false) {
            if (msg) msg.textContent = pwJson?.message || pwJson?.error || "Erro ao alterar senha.";
            return;
          }
          changedPassword = true;
        } catch {
          if (msg) msg.textContent = "Falha ao alterar senha no servidor (change-password).";
          return;
        }
      }

      // espelha no localStorage só depois do backend
      currentUser.name = name;
      currentUser.displayName = name;
      currentUser.email = email;
      try {
        localStorage.setItem("nfseUser", JSON.stringify(currentUser));
      } catch {}

      const label = $("userMenuLabel");
      if (label) label.textContent = name;

      // limpa senhas
      const p1 = settingsModalEl.querySelector("#mePass");
      const p2 = settingsModalEl.querySelector("#mePass2");
      if (p1) p1.value = "";
      if (p2) p2.value = "";

      if (changedPassword) {
        if (msg) msg.textContent = "Senha alterada. Faça login novamente com a nova senha...";
        setTimeout(() => doLogout(), 900);
        return;
      }

      if (msg) msg.textContent = "Salvo com sucesso.";
    });
  }

  if (isAdmin) {
    await adminLoadUsersIntoModal();
    wireAdminActions();
  }
}

async function adminLoadUsersIntoModal() {
  const tbody = settingsModalEl?.querySelector("#usersTbody");
  const countEl = settingsModalEl?.querySelector("#usersCount");
  const msgEl = settingsModalEl?.querySelector("#usersMsg");
  if (!tbody) return;

  tbody.innerHTML = "";
  if (msgEl) msgEl.textContent = "Carregando...";

  try {
    const res = await fetch("/admin/users", { credentials: "include", cache: "no-store" });
    if (!res.ok) {
      const t = await res.text().catch(() => "");
      if (msgEl) msgEl.textContent = t || "Erro ao listar usuários.";
      return;
    }

    const data = await res.json().catch(() => ({}));
    const list = Array.isArray(data?.users) ? data.users : [];
    const totals = data?.totals || null;

    if (countEl && totals) {
      countEl.textContent = `Ativos: ${totals.active} | Inativos: ${totals.inactive} | Total: ${totals.total}`;
    }

    list.forEach((u) => {
      const tr = document.createElement("tr");
      tr.className = "border-t border-slate-100";
      tr.innerHTML = `
        <td class="px-3 py-2">${escapeHtml(u.name || "—")}</td>
        <td class="px-3 py-2">${escapeHtml(u.email || "—")}</td>
        <td class="px-3 py-2">${escapeHtml(u.role || "USER")}</td>
        <td class="px-3 py-2">${u.is_active ? "Ativo" : "Inativo"}</td>
        <td class="px-3 py-2">
          <button data-action="toggle" data-id="${u.id}" class="px-2 py-1 rounded-lg border border-slate-200 hover:bg-slate-50">
            ${u.is_active ? "Desativar" : "Ativar"}
          </button>
          <button data-action="reset" data-id="${u.id}" class="ml-1 px-2 py-1 rounded-lg border border-slate-200 hover:bg-slate-50">
            Reset senha
          </button>
        </td>
      `;
      tbody.appendChild(tr);
    });

    if (msgEl) msgEl.textContent = "";
  } catch (err) {
    console.error(err);
    if (msgEl) msgEl.textContent = "Erro inesperado ao listar usuários.";
  }
}

function wireAdminActions() {
  const createBtn = settingsModalEl?.querySelector("#createUserBtn");
  const createMsg = settingsModalEl?.querySelector("#createUserMsg");

  if (createBtn && createBtn.dataset.bound !== "1") {
    createBtn.dataset.bound = "1";
    createBtn.addEventListener("click", async () => {
      const name = settingsModalEl.querySelector("#newUserName")?.value?.trim() || "";
      const email = settingsModalEl.querySelector("#newUserEmail")?.value?.trim() || "";
      const pass = settingsModalEl.querySelector("#newUserPass")?.value || "";
      const role = settingsModalEl.querySelector("#newUserRole")?.value || "USER";

      if (!name || !email || !pass) {
        if (createMsg) createMsg.textContent = "Preencha nome, email e senha.";
        return;
      }

      if (createMsg) createMsg.textContent = "Criando...";

      try {
        const res = await fetch("/admin/users", {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name, email, password: pass, role }),
        });

        if (!res.ok) {
          const t = await res.text().catch(() => "");
          if (createMsg) createMsg.textContent = t || "Erro ao criar usuário.";
          return;
        }

        if (createMsg) createMsg.textContent = "Usuário criado.";
        settingsModalEl.querySelector("#newUserName").value = "";
        settingsModalEl.querySelector("#newUserEmail").value = "";
        settingsModalEl.querySelector("#newUserPass").value = "";
        settingsModalEl.querySelector("#newUserRole").value = "USER";

        await adminLoadUsersIntoModal();
      } catch (err) {
        console.error(err);
        if (createMsg) createMsg.textContent = "Erro inesperado ao criar usuário.";
      }
    });
  }

  const tbody = settingsModalEl?.querySelector("#usersTbody");
  if (tbody && tbody.dataset.bound !== "1") {
    tbody.dataset.bound = "1";
    tbody.addEventListener("click", async (ev) => {
      const btn = ev.target.closest("button");
      if (!btn) return;

      const action = btn.getAttribute("data-action");
      const id = btn.getAttribute("data-id");
      if (!action || !id) return;

      if (action === "toggle") {
        try {
          const res = await fetch(`/admin/users/${encodeURIComponent(id)}/toggle`, {
            method: "POST",
            credentials: "include",
          });
          if (!res.ok) return;
          await adminLoadUsersIntoModal();
        } catch {}
      }

      if (action === "reset") {
        const newPass = prompt("Digite a nova senha temporária para esse usuário:");
        if (!newPass) return;

        try {
          const res = await fetch(`/admin/users/${encodeURIComponent(id)}/reset-password`, {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ newPassword: newPass }),
          });
          if (!res.ok) return;
          alert("Senha resetada com sucesso.");
        } catch {}
      }
    });
  }
}

// =======================================================
// 4) Tema claro/escuro
// =======================================================
const themeToggleBtn = $("themeToggleBtn");
const themeToggleKnob = $("themeToggleKnob");
const themeSunIcon = $("themeSunIcon");
const themeMoonIcon = $("themeMoonIcon");

function applyThemeUI(isDark) {
  document.body.classList.toggle("dark-mode", isDark);
  document.documentElement.classList.toggle("dark", isDark);

  localStorage.setItem("nfseTheme", isDark ? "dark" : "light");
  if (themeToggleBtn) themeToggleBtn.setAttribute("aria-checked", String(isDark));

  if (themeToggleKnob) {
    themeToggleKnob.classList.toggle("translate-x-[40px]", isDark);

    themeToggleKnob.classList.toggle("bg-slate-900", isDark);
    themeToggleKnob.classList.toggle("border-slate-700", isDark);

    themeToggleKnob.classList.toggle("bg-white", !isDark);
    themeToggleKnob.classList.toggle("border-slate-200", !isDark);
  }

  if (themeSunIcon) {
    themeSunIcon.classList.toggle("text-slate-700", !isDark);
    themeSunIcon.classList.toggle("text-slate-400", isDark);
  }

  if (themeMoonIcon) {
    themeMoonIcon.classList.toggle("text-slate-400", !isDark);
    themeMoonIcon.classList.toggle("text-slate-200", isDark);
  }
}

function initTheme() {
  const savedTheme = localStorage.getItem("nfseTheme") || "light";
  applyThemeUI(savedTheme === "dark");
  if (themeToggleBtn && !themeToggleBtn.dataset.bound) {
    themeToggleBtn.dataset.bound = "1";
    themeToggleBtn.addEventListener("click", () => {
      const willBeDark =
        !document.documentElement.classList.contains("dark") &&
        !document.body.classList.contains("dark-mode");
      applyThemeUI(willBeDark);
    });
  }
}

// =======================================================
// 5) Ocultar aba emissão no servidor (mantive)
// =======================================================
function hideEmissaoTabInProd() {
  if (isLocalhostHost()) return;

  const emissaoBtn = document.querySelector('.tab-btn[data-tab="emissao"]');
  if (emissaoBtn) emissaoBtn.remove();

  const emissaoPanel = $("tab-emissao");
  if (emissaoPanel) emissaoPanel.remove();
}

// =======================================================
// 6) Tabs
// =======================================================
function initTabs() {
  const tabButtons = document.querySelectorAll(".tab-btn");
  const tabPanels = document.querySelectorAll(".tab-panel");

  function activateTab(tabName) {
    if (tabName === "emissao" && !isLocalhostHost()) tabName = "download";

    tabButtons.forEach((btn) => {
      const isActive = btn.dataset.tab === tabName;
      btn.classList.toggle("border-slate-900", isActive);
      btn.classList.toggle("text-slate-900", isActive);
      btn.classList.toggle("bg-slate-100", isActive);
    });

    tabPanels.forEach((panel) => {
      panel.classList.toggle("hidden", panel.id !== `tab-${tabName}`);
    });
  }

  if (tabButtons.length) {
    activateTab(isLocalhostHost() ? "download" : "download");

    tabButtons.forEach((btn) => {
      if (btn.dataset.bound) return;
      btn.dataset.bound = "1";
      btn.addEventListener("click", () => activateTab(btn.dataset.tab));
    });
  }
}

// =======================================================
// 7) Remover duplicações de títulos/subtítulos nas páginas
// =======================================================
function removeDuplicateHeadings() {
  const seen = new Set();

  const candidates = Array.from(document.querySelectorAll("h1, h2, p"))
    .filter((el) => {
      const t = (el.textContent || "").trim();
      if (!t) return false;
      return t.length <= 80;
    });

  candidates.forEach((el) => {
    const t = (el.textContent || "").trim().replace(/\s+/g, " ");
    const key = `${el.tagName}:${t}`;
    if (seen.has(key)) {
      el.remove();
    } else {
      seen.add(key);
    }
  });
}

// =======================================================
// 8) Logs helpers
// =======================================================
const logsDownload = $("logsDownload");
const logsLote = $("logsLote");

function addLog(element, message) {
  if (!element) return;
  const line = document.createElement("div");
  line.textContent = message;
  element.appendChild(line);
  element.scrollTop = element.scrollHeight;
}

function clearLogs(element) {
  if (!element) return;
  element.innerHTML = "";
}

function hideServerPathUI() {
  const idsToHide = [
    "serverPathManual",
    "copyServerPathManual",
    "serverPathLote",
    "copyServerPathLote",
  ];

  idsToHide.forEach((id) => {
    const el = $(id);
    if (!el) return;

    el.style.display = "none";

    const maybeContainer =
      el.closest(".flex") ||
      el.closest(".grid") ||
      el.closest(".space-y-2") ||
      el.parentElement;

    if (maybeContainer && maybeContainer !== document.body) {
      maybeContainer.style.display = "none";
    }
  });
}

function triggerZipDownload(zipUrl) {
  if (!zipUrl) return;
  const a = document.createElement("a");
  a.href = zipUrl;
  a.download = "";
  document.body.appendChild(a);
  a.click();
  a.remove();
}

// =======================================================
// 9) Tipos/Período (mantive sua lógica)
// =======================================================
function getSelectedTipos(prefix = "") {
  const idEmit = `${prefix}TipoEmitidas`;
  const idRec = `${prefix}TipoRecebidas`;
  const idCan = `${prefix}TipoCanceladas`;
  const idAll = `${prefix}TipoTodas`;

  const elEmit = $(idEmit);
  const elRec = $(idRec);
  const elCan = $(idCan);
  const elAll = $(idAll);

  const hasNewUI = !!(elEmit || elRec || elCan || elAll);

  if (hasNewUI) {
    if (elAll && elAll.checked) return ["emitidas", "recebidas", "canceladas"];

    const tipos = [];
    if (elEmit && elEmit.checked) tipos.push("emitidas");
    if (elRec && elRec.checked) tipos.push("recebidas");
    if (elCan && elCan.checked) tipos.push("canceladas");

    return tipos.length ? tipos : ["emitidas"];
  }

  const radioName = prefix ? "loteTipoNota" : "tipoNota";
  const tipoNotaRadio = document.querySelector(`input[name='${radioName}']:checked`);
  const tipoNota = tipoNotaRadio ? tipoNotaRadio.value : "emitidas";

  if (String(tipoNota).toLowerCase() === "todas") {
    return ["emitidas", "recebidas", "canceladas"];
  }

  return [tipoNota];
}

function wireTodasCheckbox(prefix = "") {
  const elAll = $(`${prefix}TipoTodas`);
  const elEmit = $(`${prefix}TipoEmitidas`);
  const elRec = $(`${prefix}TipoRecebidas`);
  const elCan = $(`${prefix}TipoCanceladas`);

  if (!elAll || (!elEmit && !elRec && !elCan)) return;
  if (elAll.dataset.bound) return;
  elAll.dataset.bound = "1";

  elAll.addEventListener("change", () => {
    const v = elAll.checked;
    if (elEmit) elEmit.checked = v;
    if (elRec) elRec.checked = v;
    if (elCan) elCan.checked = v;
  });

  const refreshAll = () => {
    const allChecked =
      (!!elEmit ? elEmit.checked : true) &&
      (!!elRec ? elRec.checked : true) &&
      (!!elCan ? elCan.checked : true);

    elAll.checked = allChecked;
  };

  [elEmit, elRec, elCan].filter(Boolean).forEach((el) => {
    if (el.dataset.bound) return;
    el.dataset.bound = "1";
    el.addEventListener("change", refreshAll);
  });

  refreshAll();
}

function parseISODateInput(v) {
  if (!v) return null;
  const parts = String(v).split("-");
  if (parts.length !== 3) return null;
  const y = Number(parts[0]);
  const m = Number(parts[1]);
  const d = Number(parts[2]);
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null;
  return new Date(y, m - 1, d);
}

function formatDateInputValue(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function setCurrentMonthPeriodoDefaults() {
  const now = new Date();
  const firstDay = formatDateInputValue(new Date(now.getFullYear(), now.getMonth(), 1));
  const lastDay = formatDateInputValue(new Date(now.getFullYear(), now.getMonth() + 1, 0));

  [
    ["dataInicial", firstDay],
    ["dataFinal", lastDay],
    ["loteDataInicial", firstDay],
    ["loteDataFinal", lastDay],
    ["loteDataInicio", firstDay],
    ["loteDataFim", lastDay],
    ["ciDataInicio", firstDay],
    ["ciDataFim", lastDay],
  ].forEach(([id, value]) => {
    const el = $(id);
    if (el && !el.value) el.value = value;
  });
}

function maybeSwapPeriodoInUI({ dataInicialId, dataFinalId, logsEl }) {
  const diEl = $(dataInicialId);
  const dfEl = $(dataFinalId);
  if (!diEl || !dfEl) return;

  const di = parseISODateInput(diEl.value);
  const df = parseISODateInput(dfEl.value);

  if (!di || !df) return;

  if (di.getTime() > df.getTime()) {
    addLog(
      logsEl,
      "[AVISO] Período invertido (Data inicial > Data final). Corrigindo automaticamente (trocando as datas)."
    );
    const tmp = diEl.value;
    diEl.value = dfEl.value;
    dfEl.value = tmp;
  }
}

function validatePeriodo(config, logsEl) {
  if (!config.dataInicial || !config.dataFinal) {
    addLog(logsEl, "[ERRO] Data inicial e Data final são obrigatórias.");
    return false;
  }

  if (!config.baixarXml && !config.baixarPdf) {
    addLog(logsEl, "[ERRO] Selecione pelo menos um formato: XML e/ou PDF.");
    return false;
  }

  if (!Array.isArray(config.processarTipos) || config.processarTipos.length === 0) {
    addLog(logsEl, "[ERRO] Selecione pelo menos um tipo (Emitidas/Recebidas/Canceladas).");
    return false;
  }

  const di = parseISODateInput(config.dataInicial);
  const df = parseISODateInput(config.dataFinal);
  if (di && df && di.getTime() > df.getTime()) {
    addLog(logsEl, "[ERRO] Período inválido: Data inicial > Data final.");
    return false;
  }

  return true;
}

// =======================================================
// ✅ 9.5) CAPTURA INDIVIDUAL — UI Base44 (APENAS FRONT)
//     - substitui placeholder pela estrutura igual ao Base44
//     - mantém IDs que o código já usa (não quebra lógica)
// =======================================================
function findCaptureIndividualMount() {
  // opção 1: um mount explícito (se você quiser adicionar depois)
  const byId = $("captureIndividualMount");
  if (byId) return byId;

  // opção 2: procurar pelo texto do placeholder que aparece na tua tela
  const needle = "(Tela placeholder)";
  const all = Array.from(document.querySelectorAll("div, section, main, article, p"));
  const hit = all.find((el) => (el.textContent || "").includes(needle));
  if (!hit) return null;

  // pega um container “bom” pra trocar
  return hit.closest("section") || hit.closest("main") || hit.parentElement || hit;
}

function ensureCaptureIndividualBase44UI() {
  const mount = findCaptureIndividualMount();
  if (!mount) return false;

  // evita re-render
  if ($("captureIndividualCard")) return true;

  mount.innerHTML = `
    <div id="captureIndividualCard" class="max-w-3xl">
      <div class="rounded-2xl border border-white/10 bg-white/5 p-6 md:p-7 shadow-[0_0_0_1px_rgba(255,255,255,0.02)]">
        <div class="space-y-1">
          <div class="text-xs font-semibold tracking-widest text-white/60">EMPRESA</div>
          <div class="relative">
            <select id="empresaSelect"
              class="w-full appearance-none rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white/90 outline-none focus:border-white/20">
              <option value="">Selecione a empresa</option>
            </select>
            <div class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-white/60">▾</div>
          </div>
          <div class="flex items-center justify-between">
            <div class="text-xs text-white/45"></div>
            <button id="btnOpenCadastroEmpresa" type="button"
              class="hidden text-xs text-white/70 hover:text-white underline underline-offset-4">
              Cadastrar empresa
            </button>
          </div>
        </div>

        <div class="mt-6">
          <div class="text-xs font-semibold tracking-widest text-white/60">PERÍODO (MÁX. 30 DIAS)</div>
          <div class="mt-3 grid grid-cols-1 md:grid-cols-2 gap-4">
            <label class="block">
              <div class="text-xs text-white/60 mb-2">Data Início</div>
              <input id="dataInicial" type="date"
                class="w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white/90 outline-none focus:border-white/20" />
            </label>

            <label class="block">
              <div class="text-xs text-white/60 mb-2">Data Fim</div>
              <input id="dataFinal" type="date"
                class="w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white/90 outline-none focus:border-white/20" />
            </label>
          </div>
        </div>

        <div class="mt-6">
          <div class="text-xs font-semibold tracking-widest text-white/60">TIPOS DE NOTA</div>

          <div class="mt-3 flex flex-wrap gap-4 text-sm text-white/80">
            <label class="inline-flex items-center gap-2 cursor-pointer select-none">
              <input id="TipoRecebidas" type="checkbox" class="h-4 w-4 rounded border-white/20 bg-white/10" checked />
              Recebidas
            </label>

            <label class="inline-flex items-center gap-2 cursor-pointer select-none">
              <input id="TipoEmitidas" type="checkbox" class="h-4 w-4 rounded border-white/20 bg-white/10" checked />
              Emitidas
            </label>

            <label class="inline-flex items-center gap-2 cursor-pointer select-none">
              <input id="TipoCanceladas" type="checkbox" class="h-4 w-4 rounded border-white/20 bg-white/10" checked />
              Canceladas
            </label>

            <!-- opcional: 'todas' invisível (pra wireTodasCheckbox não quebrar se existir em outros lugares) -->
            <input id="TipoTodas" type="checkbox" class="hidden" />
          </div>
        </div>

        <div class="mt-6">
          <div class="text-xs font-semibold tracking-widest text-white/60">FORMATO</div>
          <div class="mt-3 relative">
            <select id="formatoDownload"
              class="w-full appearance-none rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white/90 outline-none focus:border-white/20">
              <option value="pdf+xml" selected>PDF + XML</option>
              <option value="pdf">Somente PDF</option>
              <option value="xml">Somente XML</option>
            </select>
            <div class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-white/60">▾</div>
          </div>
        </div>

        <!-- mantém compatibilidade com sua lógica atual -->
        <div class="hidden">
          <input id="manualLoginPortal" />
          <input id="manualSenhaPortal" type="password" />
          <input id="pastaDestino" value="downloads" />
          <input id="baixarPdf" type="checkbox" checked />
          <input id="baixarXml" type="checkbox" checked />
        </div>

        <button id="iniciarDownloadBtn" type="button"
          class="mt-8 w-full rounded-xl bg-indigo-600/40 hover:bg-indigo-600/50 border border-white/10 px-4 py-3 text-sm font-semibold text-white flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed">
          <span aria-hidden="true">⤓</span>
          Iniciar Captura
        </button>

        <div id="logsDownload" class="mt-4 text-xs text-white/70 space-y-1 max-h-40 overflow-auto"></div>
      </div>
    </div>
  `;

  // bind formato -> espelha em baixarPdf/baixarXml (mantém backend igual)
  const formatoEl = $("formatoDownload");
  const pdfEl = $("baixarPdf");
  const xmlEl = $("baixarXml");
  if (formatoEl && pdfEl && xmlEl && !formatoEl.dataset.bound) {
    formatoEl.dataset.bound = "1";
    const apply = () => {
      const v = (formatoEl.value || "pdf+xml").toLowerCase();
      if (v === "pdf+xml") {
        pdfEl.checked = true;
        xmlEl.checked = true;
      } else if (v === "pdf") {
        pdfEl.checked = true;
        xmlEl.checked = false;
      } else if (v === "xml") {
        pdfEl.checked = false;
        xmlEl.checked = true;
      }
    };
    formatoEl.addEventListener("change", apply);
    apply();
  }

  return true;
}

// =======================================================
// 10) Captura Manual / Lote (corrigido: credentials include)
// =======================================================
function getDownloadConfig() {
  const dataInicialEl = $("dataInicial");
  const dataFinalEl = $("dataFinal");
  const baixarXmlEl = $("baixarXml");
  const baixarPdfEl = $("baixarPdf");
  const pastaDestinoEl = $("pastaDestino");

  const manualLoginEl = $("manualLoginPortal");
  const manualSenhaEl = $("manualSenhaPortal");

  const processarTipos = getSelectedTipos("");
  const tipoNota = processarTipos[0] || "emitidas";

  return {
    dataInicial: dataInicialEl ? dataInicialEl.value || null : null,
    dataFinal: dataFinalEl ? dataFinalEl.value || null : null,

    tipoNota,
    processarTipos,

    baixarXml: !!(baixarXmlEl && baixarXmlEl.checked),
    baixarPdf: !!(baixarPdfEl && baixarPdfEl.checked),

    pastaDestino: pastaDestinoEl && pastaDestinoEl.value ? pastaDestinoEl.value : "downloads",
    login: manualLoginEl && manualLoginEl.value.trim() ? manualLoginEl.value.trim() : null,
    senha: manualSenhaEl && manualSenhaEl.value ? manualSenhaEl.value : null,
  };
}

function initCaptureButtons() {
  const iniciarDownloadBtn = $("iniciarDownloadBtn");
  if (iniciarDownloadBtn && !iniciarDownloadBtn.dataset.bound) {
    iniciarDownloadBtn.dataset.bound = "1";
    iniciarDownloadBtn.addEventListener("click", async () => {
      clearLogs(logsDownload);

      maybeSwapPeriodoInUI({
        dataInicialId: "dataInicial",
        dataFinalId: "dataFinal",
        logsEl: logsDownload,
      });

      const config = getDownloadConfig();
      if (!validatePeriodo(config, logsDownload)) return;

      if (!config.login || !config.senha) {
        addLog(
          logsDownload,
          "[ERRO] A empresa selecionada não possui Login/Senha do Portal salvos. Cadastre isso na empresa para a captura funcionar."
        );
        return;
      }

      addLog(logsDownload, "[INFO] Enviando requisição para o robô...");

      try {
        const res = await fetch("/api/nf/manual", {
          method: "POST",
          credentials: "include",
          headers: apiHeaders({ "Content-Type": "application/json" }),
          body: JSON.stringify(config),
        });

        if (!res.ok) {
          addLog(logsDownload, `[ERRO] Falha: ${res.status} ${res.statusText}`);
          const txt = await res.text().catch(() => "");
          if (txt) addLog(logsDownload, txt);
          return;
        }

        const data = await res.json().catch(() => ({}));

        if (!data.success) {
          addLog(logsDownload, "[ERRO] O robô não conseguiu concluir.");
          if (data.error) addLog(logsDownload, `Detalhe: ${data.error}`);
          return;
        }

        if (Array.isArray(data.logs) && data.logs.length > 0) {
          data.logs.forEach((msg) => addLog(logsDownload, msg));
        } else {
          addLog(logsDownload, "[OK] Concluído (sem logs detalhados).");
        }

        if (data.downloadZipUrl) {
          addLog(logsDownload, `[OK] ZIP gerado. Baixando: ${data.downloadZipUrl}`);
          triggerZipDownload(data.downloadZipUrl);
        } else {
          addLog(logsDownload, "[AVISO] Nenhum ZIP retornado.");
        }

        hideServerPathUI();
      } catch (err) {
        console.error(err);
        addLog(logsDownload, "[ERRO] Erro inesperado ao comunicar com o servidor.");
      }
    });
  }

  const baixarTudoBtn = $("baixarTudoBtn");
  if (baixarTudoBtn && !baixarTudoBtn.dataset.bound) {
    baixarTudoBtn.dataset.bound = "1";
    baixarTudoBtn.addEventListener("click", async () => {
      clearLogs(logsLote);

      maybeSwapPeriodoInUI({
        dataInicialId: "loteDataInicial",
        dataFinalId: "loteDataFinal",
        logsEl: logsLote,
      });

      const config = getDownloadConfig();

      // sobrescreve config com lote se existir
      const loteDataInicialEl = $("loteDataInicial");
      const loteDataFinalEl = $("loteDataFinal");
      const loteBaixarXmlEl = $("loteBaixarXml");
      const loteBaixarPdfEl = $("loteBaixarPdf");
      const lotePastaDestinoInput = $("lotePastaDestino");

      if (loteDataInicialEl && loteDataInicialEl.value) config.dataInicial = loteDataInicialEl.value;
      if (loteDataFinalEl && loteDataFinalEl.value) config.dataFinal = loteDataFinalEl.value;

      if (loteBaixarXmlEl) config.baixarXml = !!loteBaixarXmlEl.checked;
      if (loteBaixarPdfEl) config.baixarPdf = !!loteBaixarPdfEl.checked;

      const tiposLote = getSelectedTipos("lote");
      config.processarTipos = tiposLote;
      config.tipoNota = tiposLote[0] || "emitidas";

      if (lotePastaDestinoInput && lotePastaDestinoInput.value.trim()) {
        config.pastaDestino = lotePastaDestinoInput.value.trim();
      }

      if (!validatePeriodo(config, logsLote)) return;

      addLog(logsLote, "[INFO] Enviando requisição para execução em lote...");

      try {
        const res = await fetch("/api/nf/lote", {
          method: "POST",
          credentials: "include",
          headers: apiHeaders({ "Content-Type": "application/json" }),
          body: JSON.stringify(config),
        });

        if (!res.ok) {
          const txt = await res.text().catch(() => "");
          addLog(logsLote, `[ERRO] Falha: ${res.status} ${res.statusText}`);
          if (txt) addLog(logsLote, `Detalhe: ${txt}`);
          return;
        }

        const data = await res.json().catch(() => ({}));

        if (!data.success) {
          addLog(logsLote, "[ERRO] O robô não conseguiu concluir o lote.");
          if (data.error) addLog(logsLote, `Detalhe: ${data.error}`);
          return;
        }

        if (Array.isArray(data.logs) && data.logs.length > 0) {
          data.logs.forEach((msg) => addLog(logsLote, msg));
        } else {
          addLog(logsLote, "[OK] Lote concluído (sem logs detalhados).");
        }

        if (data.downloadZipUrl) {
          addLog(logsLote, `[OK] ZIP do lote gerado. Baixando: ${data.downloadZipUrl}`);
          triggerZipDownload(data.downloadZipUrl);
        } else {
          addLog(logsLote, "[AVISO] Nenhum ZIP retornado.");
        }

        hideServerPathUI();
      } catch (err) {
        console.error(err);
        addLog(logsLote, "[ERRO] Erro inesperado ao comunicar com o servidor.");
      }
    });
  }
}

// =======================================================
// 11) Empresas: suporta (A) tabela antiga e (B) cards novos
// =======================================================
let empresas = [];
let empresaSelecionadaId = null;

function normalizeEmpresa(emp) {
  return {
    id: emp?.id ?? emp?.empresaId ?? emp?._id ?? emp?.cnpj ?? "",
    nome: emp?.nome ?? emp?.empresaNome ?? emp?.razaoSocial ?? emp?.fantasia ?? "",
    cnpj: emp?.cnpj ?? emp?.empresaCnpj ?? emp?.documento ?? "",
    raw: emp,
  };
}

// ---- (A) Tabela antiga ----
const empresasTableBody = $("empresasTableBody");
const removerEmpresaBtn = $("removerEmpresaBtn");

function renderEmpresasTabela() {
  if (!empresasTableBody) return;

  empresasTableBody.innerHTML = "";

  if (empresas.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 3;
    td.className = "px-3 py-3 text-center text-sm text-slate-400";
    td.textContent = "Nenhuma empresa cadastrada.";
    tr.appendChild(td);
    empresasTableBody.appendChild(tr);
    empresaSelecionadaId = null;
    if (removerEmpresaBtn) removerEmpresaBtn.disabled = true;
    return;
  }

  empresas.forEach((empRaw) => {
    const emp = normalizeEmpresa(empRaw);

    const tr = document.createElement("tr");
    tr.className = "border-t border-slate-100 hover:bg-sky-50 cursor-pointer";
    tr.dataset.id = String(emp.id || "");

    tr.innerHTML = `
      <td class="px-3 py-2 text-slate-600">${escapeHtml(emp.id || "—")}</td>
      <td class="px-3 py-2 text-slate-800">${escapeHtml(emp.nome || "—")}</td>
      <td class="px-3 py-2 text-slate-600">${escapeHtml(emp.cnpj || "—")}</td>
    `;

    empresasTableBody.appendChild(tr);
  });
}

function bindTabelaSelect() {
  if (!empresasTableBody || empresasTableBody.dataset._bound === "1") return;
  empresasTableBody.dataset._bound = "1";

  empresasTableBody.addEventListener("click", (ev) => {
    const tr = ev.target.closest("tr");
    if (!tr || !empresasTableBody.contains(tr)) return;

    const id = (tr.dataset.id || "").trim();
    if (!id || id === "—") return;

    empresaSelecionadaId = id;

    Array.from(empresasTableBody.querySelectorAll("tr")).forEach((row) => {
      row.classList.remove("bg-sky-100");
    });

    tr.classList.add("bg-sky-100");
    if (removerEmpresaBtn) removerEmpresaBtn.disabled = false;
  });
}

async function deleteEmpresaById(id) {
  const ok = confirm("Deseja remover a empresa selecionada?");
  if (!ok) return;

  try {
    const res = await fetch(`/api/empresas/${encodeURIComponent(id)}`, {
      method: "DELETE",
      credentials: "include",
      headers: apiHeaders(),
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.error("Erro ao remover empresa:", res.status, res.statusText, txt);
      addLog(logsLote, `[ERRO] Falha ao remover (HTTP ${res.status}).`);
      if (txt) addLog(logsLote, txt);
      return;
    }

    addLog(logsLote, "[OK] Empresa removida com sucesso.");
    await loadEmpresasFromAPI();
  } catch (err) {
    console.error("Erro ao remover empresa:", err);
    addLog(logsLote, "[ERRO] Erro inesperado ao remover empresa.");
  }
}

function bindRemoverEmpresaTabela() {
  if (!removerEmpresaBtn || removerEmpresaBtn.dataset.bound) return;
  removerEmpresaBtn.dataset.bound = "1";

  removerEmpresaBtn.addEventListener("click", async () => {
    if (!empresaSelecionadaId) {
      addLog(logsLote, "[AVISO] Selecione uma empresa na tabela antes de remover.");
      return;
    }
    await deleteEmpresaById(empresaSelecionadaId);
    empresaSelecionadaId = null;
    removerEmpresaBtn.disabled = true;
  });
}

// ---- (B) Cards novos ----
const empresasCardsContainer =
  $("empresasList") ||
  $("empresasCards") ||
  document.querySelector("[data-empresas-container]") ||
  document.querySelector(".empresas-cards") ||
  null;

function renderEmpresasCards() {
  if (!empresasCardsContainer) return;

  const shouldRender =
    empresasCardsContainer.hasAttribute("data-render-js") ||
    empresasCardsContainer.children.length === 0;

  if (!shouldRender) return;

  empresasCardsContainer.innerHTML = "";

  empresas.forEach((eRaw) => {
    const e = normalizeEmpresa(eRaw);
    const card = document.createElement("div");
    card.className =
      "rounded-2xl border border-white/10 bg-white/5 p-4 flex items-center justify-between";
    card.dataset.empresaId = e.id;

    card.innerHTML = `
      <div>
        <div class="text-sm font-semibold text-white">${escapeHtml(e.nome || "—")}</div>
        <div class="text-xs text-white/60">${escapeHtml(e.cnpj || "")}</div>
      </div>
      <div class="flex items-center gap-2">
        <button type="button" data-action="edit-empresa" data-id="${escapeHtml(e.id)}"
          class="px-3 py-2 rounded-xl bg-white/10 hover:bg-white/15 text-white text-xs">
          Editar
        </button>
        <button type="button" data-action="delete-empresa" data-id="${escapeHtml(e.id)}"
          class="px-3 py-2 rounded-xl bg-red-500/20 hover:bg-red-500/30 text-white text-xs">
          Excluir
        </button>
      </div>
    `;

    empresasCardsContainer.appendChild(card);
  });
}

function bindCardsDelegation() {
  if (!empresasCardsContainer) return;
  if (empresasCardsContainer.dataset.bound === "1") return;
  empresasCardsContainer.dataset.bound = "1";

  empresasCardsContainer.addEventListener("click", async (ev) => {
    const btn = ev.target.closest("button");
    if (!btn) return;

    const action = btn.getAttribute("data-action") || "";
    const id =
      btn.getAttribute("data-id") ||
      btn.closest("[data-empresa-id]")?.getAttribute("data-empresa-id");

    const aria = (btn.getAttribute("aria-label") || "").toLowerCase();
    const className = (btn.className || "").toLowerCase();

    const isDelete =
      action === "delete-empresa" ||
      aria.includes("excluir") ||
      aria.includes("remover") ||
      className.includes("trash") ||
      className.includes("delete");

    if (isDelete && id) {
      await deleteEmpresaById(id);
      return;
    }
  });
}

// ✅ Select de empresa (Captura Individual Base44)
function bindEmpresaSelectFillCreds() {
  const sel = $("empresaSelect");
  if (!sel || sel.dataset.bound) return;
  sel.dataset.bound = "1";

  sel.addEventListener("change", () => {
    const v = (sel.value || "").trim();
    const emp = empresas.find((x) => String(normalizeEmpresa(x).id) === String(v));
    const raw = emp ? emp : null;

    const loginEl = $("manualLoginPortal");
    const senhaEl = $("manualSenhaPortal");

    const login =
      raw?.loginPortal ??
      raw?.portalLogin ??
      raw?.login ??
      raw?.usuario ??
      raw?.cnpj ??
      null;

    const senha =
      raw?.senhaPortal ??
      raw?.portalSenha ??
      raw?.senha ??
      raw?.password ??
      null;

    if (loginEl) loginEl.value = login ? String(login) : "";
    if (senhaEl) senhaEl.value = senha ? String(senha) : "";

    // log discreto (se existir)
    const logEl = $("logsDownload");
    if (logEl) {
      clearLogs(logEl);
      if (!v) {
        addLog(logEl, "[INFO] Selecione uma empresa para carregar credenciais.");
      } else if (!senha) {
        addLog(logEl, "[AVISO] Empresa selecionada sem senha salva. Cadastre Login/Senha na empresa.");
      } else {
        addLog(logEl, "[OK] Empresa selecionada. Credenciais carregadas.");
      }
    }
  });
}

function renderEmpresaSelectOptions() {
  const sel = $("empresaSelect");
  if (!sel) return;

  const current = sel.value || "";
  sel.innerHTML = `<option value="">Selecione a empresa</option>`;

  empresas.forEach((empRaw) => {
    const e = normalizeEmpresa(empRaw);
    const opt = document.createElement("option");
    opt.value = String(e.id || "");
    opt.textContent = e.nome ? `${e.nome}` : `${e.cnpj || e.id || "Empresa"}`;
    sel.appendChild(opt);
  });

  // tenta restaurar seleção
  if (current) sel.value = current;

  // se só tem 1 empresa, seleciona automático
  if (!sel.value && empresas.length === 1) {
    const only = normalizeEmpresa(empresas[0]);
    sel.value = String(only.id || "");
    sel.dispatchEvent(new Event("change"));
  }
}

// ---- Carregar empresas ----
async function loadEmpresasFromAPI() {
  if (!empresasTableBody && !empresasCardsContainer && !$("empresaSelect")) return;

  try {
    const res = await fetch("/api/empresas", {
      credentials: "include",
      headers: apiHeaders(),
    });

    if (!res.ok) {
      console.error("Erro ao carregar empresas:", res.status, res.statusText);
      return;
    }

    const data = await res.json().catch(() => ({}));
    const list = Array.isArray(data) ? data : Array.isArray(data?.empresas) ? data.empresas : [];

    empresas = list;
    empresaSelecionadaId = null;
    if (removerEmpresaBtn) removerEmpresaBtn.disabled = true;

    renderEmpresasTabela();
    renderEmpresasCards();

    // ✅ atualiza dropdown da captura individual (se existir)
    renderEmpresaSelectOptions();
    bindEmpresaSelectFillCreds();
  } catch (err) {
    console.error("Erro ao carregar empresas:", err);
  }
}

// ---- Criar empresa (mantive) ----
async function createEmpresaFromUI() {
  const nome = getValByIds(["nomeEmpresa", "nomeEmpresaModal", "empresaNome", "nome_da_empresa"]).trim();
  const cnpj = getValByIds(["cnpjEmpresa", "cnpjEmpresaModal", "empresaCnpj", "cnpj"]).trim();

  const loginPortal = getValByIds(["loginPortal", "loginPortalEmpresa", "portalLogin", "empresaLoginPortal"]).trim();
  const senhaPortal = getValByIds(["senhaPortal", "senhaPortalEmpresa", "portalSenha", "empresaSenhaPortal"]).trim();

  const uf = getValByIds(["ufEmpresa", "uf", "empresaUF"]).trim();
  const cidade = getValByIds(["cidadeEmpresa", "cidade", "empresaCidade"]).trim();
  const regime = getValByIds(["regimeTributario", "regime", "empresaRegime"]).trim();
  const tipoAuth = getValByIds(["tipoAutenticacao", "tipoAuth", "empresaTipoAuth"]).trim();

  const feedback =
    pickElByIds(["feedbackEmpresa", "empresaFeedback", "msgEmpresa"]) || null;

  if (!nome || !cnpj) {
    if (feedback) {
      feedback.textContent = "Preencha nome e CNPJ para salvar.";
      feedback.classList.remove("hidden");
    }
    return;
  }

  if (!senhaPortal && !loginPortal) {
    if (feedback) {
      feedback.textContent =
        "Dica: preencha Login do Portal e Senha do Portal para a captura funcionar.";
      feedback.classList.remove("hidden");
    }
  }

  try {
    const payload = {
      nome,
      cnpj,
      loginPortal: loginPortal || null,
      senhaPortal: senhaPortal || null,
      uf: uf || null,
      cidade: cidade || null,
      regimeTributario: regime || null,
      tipoAutenticacao: tipoAuth || null,
    };

    const res = await fetch("/api/empresas", {
      method: "POST",
      credentials: "include",
      headers: apiHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(payload),
    });

    const json = await res.json().catch(() => ({}));

    if (!res.ok || json?.ok === false) {
      const msg = json?.message || json?.error || "Erro ao salvar empresa no servidor.";
      if (feedback) {
        feedback.textContent = msg;
        feedback.classList.remove("hidden");
      }
      return;
    }

    if (feedback) {
      feedback.textContent = "Empresa salva com sucesso.";
      feedback.classList.remove("hidden");
    }

    ["nomeEmpresa", "nomeEmpresaModal", "empresaNome", "cnpjEmpresa", "cnpjEmpresaModal", "empresaCnpj",
     "loginPortal", "loginPortalEmpresa", "portalLogin", "senhaPortal", "senhaPortalEmpresa", "portalSenha"]
      .forEach((id) => {
        const el = $(id);
        if (el) el.value = "";
      });

    await loadEmpresasFromAPI();
  } catch (err) {
    console.error("Erro ao salvar empresa:", err);
    if (feedback) {
      feedback.textContent = "Erro inesperado ao comunicar com o servidor.";
      feedback.classList.remove("hidden");
    }
  }
}

function bindCreateEmpresaButtons() {
  const salvarEmpresaBtn = $("salvarEmpresaBtn");
  if (salvarEmpresaBtn && !salvarEmpresaBtn.dataset.bound) {
    salvarEmpresaBtn.dataset.bound = "1";
    salvarEmpresaBtn.addEventListener("click", async () => {
      await createEmpresaFromUI();
    });
  }

  const cadastrarBtn =
    pickElByIds(["cadastrarEmpresaBtn", "btnCadastrarEmpresa", "empresaCadastrarBtn"]) ||
    document.querySelector("button[type='button'][data-action='cadastrar-empresa']") ||
    null;

  if (cadastrarBtn && !cadastrarBtn.dataset.bound) {
    cadastrarBtn.dataset.bound = "1";
    cadastrarBtn.addEventListener("click", async () => {
      await createEmpresaFromUI();
    });
  }

  const form =
    pickElByIds(["empresaForm", "formEmpresa", "cadastrarEmpresaForm"]) ||
    document.querySelector("form[data-empresa-form]") ||
    null;

  if (form && !form.dataset.bound) {
    form.dataset.bound = "1";
    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      await createEmpresaFromUI();
    });
  }
}

// =======================================================
// 12) Init geral
// =======================================================
async function initDashboard() {
  const ok = await ensureLoggedUserOrRedirect();
  if (!ok) return;

  hideTopLogoutIfAny();
  initTheme();
  hideEmissaoTabInProd();
  initTabs();

  ensureUserMenuUI();

  // ✅ monta UI Base44 da Captura Individual (se estiver nessa tela)
  ensureCaptureIndividualBase44UI();
  setCurrentMonthPeriodoDefaults();

  // tipos
  wireTodasCheckbox("");
  wireTodasCheckbox("lote");

  // botões captura
  initCaptureButtons();

  // empresas
  bindTabelaSelect();
  bindRemoverEmpresaTabela();
  bindCardsDelegation();
  bindCreateEmpresaButtons();
  await loadEmpresasFromAPI();

  hideServerPathUI();
  removeDuplicateHeadings();
}

document.addEventListener("DOMContentLoaded", initDashboard);

