const registerForm = document.getElementById("registerForm");
const registerFeedback = document.getElementById("registerFeedback");
const registerSubmitBtn = document.getElementById("registerSubmitBtn");
const companyNameInput = document.getElementById("companyName");
const nameInput = document.getElementById("name");
const responsibleInput = document.getElementById("responsible");
const cnpjInput = document.getElementById("cnpj");
const signedAtInput = document.getElementById("signedAt");
const whatsappInput = document.getElementById("whatsapp");
const passwordInput = document.getElementById("password");
const confirmPasswordInput = document.getElementById("confirmPassword");
const togglePasswordBtn = document.getElementById("togglePasswordBtn");
const toggleConfirmPasswordBtn = document.getElementById("toggleConfirmPasswordBtn");
const eyeClosedIconPassword = document.getElementById("eyeClosedIconPassword");
const eyeOpenIconPassword = document.getElementById("eyeOpenIconPassword");
const eyeClosedIconConfirm = document.getElementById("eyeClosedIconConfirm");
const eyeOpenIconConfirm = document.getElementById("eyeOpenIconConfirm");

function showRegisterFeedback(message, type = "error") {
  if (!registerFeedback) return;

  registerFeedback.textContent = message || "";
  registerFeedback.classList.remove(
    "hidden",
    "border-red-200",
    "bg-red-50",
    "text-red-600",
    "border-green-200",
    "bg-green-50",
    "text-green-700"
  );

  if (type === "success") {
    registerFeedback.classList.add("border-green-200", "bg-green-50", "text-green-700");
    return;
  }

  registerFeedback.classList.add("border-red-200", "bg-red-50", "text-red-600");
}

function hideRegisterFeedback() {
  if (!registerFeedback) return;
  registerFeedback.classList.add("hidden");
  registerFeedback.textContent = "";
}

function normalizeWhatsapp(value) {
  return String(value || "").replace(/\D/g, "").slice(0, 11);
}

function normalizeCnpj(value) {
  return String(value || "").replace(/\D/g, "").slice(0, 14);
}

function formatCnpj(value) {
  const digits = normalizeCnpj(value);
  if (!digits) return "";
  if (digits.length <= 2) return digits;
  if (digits.length <= 5) return `${digits.slice(0, 2)}.${digits.slice(2)}`;
  if (digits.length <= 8) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5)}`;
  if (digits.length <= 12) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8)}`;
  return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12, 14)}`;
}

function formatWhatsapp(value) {
  const digits = normalizeWhatsapp(value);

  if (!digits) return "";
  if (digits.length <= 2) return `(${digits}`;
  if (digits.length <= 6) return `(${digits.slice(0, 2)}) ${digits.slice(2)}`;
  if (digits.length <= 10) return `(${digits.slice(0, 2)}) ${digits.slice(2, 6)}-${digits.slice(6)}`;
  return `(${digits.slice(0, 2)}) ${digits.slice(2, 7)}-${digits.slice(7, 11)}`;
}

function setupPasswordToggle(button, input, closedIcon, openIcon) {
  if (!button || !input) return;

  let showing = false;

  function sync() {
    input.type = showing ? "text" : "password";
    if (closedIcon) closedIcon.classList.toggle("hidden", showing);
    if (openIcon) openIcon.classList.toggle("hidden", !showing);
    button.setAttribute("aria-label", showing ? "Ocultar senha" : "Mostrar senha");
    button.setAttribute("title", showing ? "Ocultar senha" : "Mostrar senha");
  }

  sync();

  button.addEventListener("click", () => {
    showing = !showing;
    sync();
    input.focus();
  });
}

if (cnpjInput) {
  cnpjInput.addEventListener("input", () => {
    cnpjInput.value = formatCnpj(cnpjInput.value);
  });
}

if (whatsappInput) {
  whatsappInput.addEventListener("input", () => {
    whatsappInput.value = formatWhatsapp(whatsappInput.value);
  });
}

if (nameInput && responsibleInput) {
  nameInput.addEventListener("input", () => {
    if (!responsibleInput.dataset.userEdited || responsibleInput.dataset.userEdited !== "true") {
      responsibleInput.value = nameInput.value;
    }
  });

  responsibleInput.addEventListener("input", () => {
    responsibleInput.dataset.userEdited = responsibleInput.value.trim() ? "true" : "false";
  });
}

if (signedAtInput) {
  const today = new Date();
  const day = String(today.getDate()).padStart(2, "0");
  const month = String(today.getMonth() + 1).padStart(2, "0");
  const year = String(today.getFullYear());
  signedAtInput.value = `${day}/${month}/${year}`;
}

setupPasswordToggle(
  togglePasswordBtn,
  passwordInput,
  eyeClosedIconPassword,
  eyeOpenIconPassword
);

setupPasswordToggle(
  toggleConfirmPasswordBtn,
  confirmPasswordInput,
  eyeClosedIconConfirm,
  eyeOpenIconConfirm
);

if (registerForm) {
  registerForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    hideRegisterFeedback();

    const formData = new FormData(registerForm);
    const payload = {
      company_name: String(formData.get("company_name") || "").trim(),
      name: String(formData.get("name") || "").trim(),
      cnpj: formatCnpj(String(formData.get("cnpj") || "").trim()),
      responsible: String(formData.get("responsible") || "").trim(),
      signedAt: String(formData.get("signedAt") || "").trim(),
      email: String(formData.get("email") || "").trim().toLowerCase(),
      whatsapp: formatWhatsapp(String(formData.get("whatsapp") || "").trim()),
      password: String(formData.get("password") || ""),
      confirmPassword: String(formData.get("confirmPassword") || ""),
    };

    if (!payload.company_name || !payload.name || !payload.cnpj || !payload.responsible || !payload.signedAt || !payload.email || !payload.whatsapp || !payload.password || !payload.confirmPassword) {
      showRegisterFeedback("Preencha todos os campos obrigatórios.");
      return;
    }

    if (normalizeCnpj(payload.cnpj).length !== 14) {
      showRegisterFeedback("Informe um CNPJ válido.");
      return;
    }

    if (normalizeWhatsapp(payload.whatsapp).length < 10) {
      showRegisterFeedback("Informe um telefone ou celular válido.");
      return;
    }

    if (payload.password.length < 6) {
      showRegisterFeedback("A senha deve ter pelo menos 6 caracteres.");
      return;
    }

    if (payload.password !== payload.confirmPassword) {
      showRegisterFeedback("A confirmação de senha não confere.");
      return;
    }

    const originalText = registerSubmitBtn ? registerSubmitBtn.textContent : "";
    if (registerSubmitBtn) {
      registerSubmitBtn.disabled = true;
      registerSubmitBtn.textContent = "Criando acesso...";
    }

    try {
      const response = await fetch("/auth/register-access", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(payload),
      });

      const data = await response.json().catch(() => ({}));

      if (!response.ok || !data?.ok) {
        showRegisterFeedback(data?.error || "Não foi possível concluir seu cadastro agora.");
        return;
      }

      showRegisterFeedback(
        data?.message || "Cadastro concluído com sucesso. Redirecionando para o login...",
        "success"
      );

      registerForm.reset();
      if (signedAtInput) {
        const today = new Date();
        const day = String(today.getDate()).padStart(2, "0");
        const month = String(today.getMonth() + 1).padStart(2, "0");
        const year = String(today.getFullYear());
        signedAtInput.value = `${day}/${month}/${year}`;
      }
      if (responsibleInput) responsibleInput.dataset.userEdited = "false";

      window.setTimeout(() => {
        window.location.href = data?.redirectTo || "/index.html";
      }, 1400);
    } catch (error) {
      console.error(error);
      showRegisterFeedback("Erro ao conectar com o servidor. Tente novamente.");
    } finally {
      if (registerSubmitBtn) {
        registerSubmitBtn.disabled = false;
        registerSubmitBtn.textContent = originalText || "Criar meu acesso";
      }
    }
  });
}
