// Lead Scraper — Apify-inspired frontend

let currentJobId = null;
let currentMode = "keyword";
let leads = [];
let sortCol = null;
let sortAsc = true;
let eventSource = null;
let durationTimer = null;
let startTime = null;

const $ = (s) => document.querySelector(s);
const $$ = (s) => document.querySelectorAll(s);

// ── Tab navigation ───────────────────────────────────────────────────

$$(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    $$(".tab").forEach((t) => t.classList.remove("active"));
    tab.classList.add("active");
    $$(".panel").forEach((p) => (p.hidden = true));
    $(`#panel-${tab.dataset.tab}`).hidden = false;
  });
});

function switchTab(name) {
  $$(".tab").forEach((t) => {
    t.classList.toggle("active", t.dataset.tab === name);
  });
  $$(".panel").forEach((p) => (p.hidden = true));
  $(`#panel-${name}`).hidden = false;
}

// ── Input mode toggle ────────────────────────────────────────────────

let importedData = null;

$$(".mode-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    $$(".mode-btn").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    currentMode = btn.dataset.mode;
    $("#keyword-fields").hidden = currentMode !== "keyword";
    $("#maps-fields").hidden = currentMode !== "maps";
    $("#url-fields").hidden = currentMode !== "url";
    $("#import-fields").hidden = currentMode !== "import";
  });
});

// ── File import (drag & drop + browse) ───────────────────────────────

const fileDrop = $("#file-drop");
const fileInput = $("#import-file");

$("#file-browse").addEventListener("click", (e) => { e.preventDefault(); fileInput.click(); });
fileDrop.addEventListener("click", () => fileInput.click());

fileDrop.addEventListener("dragover", (e) => { e.preventDefault(); fileDrop.classList.add("dragover"); });
fileDrop.addEventListener("dragleave", () => fileDrop.classList.remove("dragover"));
fileDrop.addEventListener("drop", (e) => {
  e.preventDefault();
  fileDrop.classList.remove("dragover");
  if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]);
});
fileInput.addEventListener("change", () => { if (fileInput.files.length) handleFile(fileInput.files[0]); });

function handleFile(file) {
  if (!file.name.endsWith(".json")) {
    showToast("Please upload a .json file", "warn");
    return;
  }
  const reader = new FileReader();
  reader.onload = (e) => {
    try {
      importedData = JSON.parse(e.target.result);
      if (!Array.isArray(importedData)) importedData = [importedData];
      const count = importedData.length;
      const withUrl = importedData.filter(r => r.website || r.url).length;
      $("#file-name").textContent = `${file.name} — ${count} records, ${withUrl} with URLs`;
      $("#file-name").hidden = false;
      $(".file-drop-text").hidden = true;
      showToast(`Loaded ${count} records from ${file.name}`);
    } catch {
      showToast("Invalid JSON file", "error");
    }
  };
  reader.readAsText(file);
}

// ── Start run ────────────────────────────────────────────────────────

$("#start-btn").addEventListener("click", async () => {
  let endpoint, payload;

  // Gather advanced options
  const advancedOpts = {
    stealth: $("#opt-stealth").checked,
    google_maps: $("#opt-google-maps").checked,
    deep_crawl: $("#opt-deep-crawl").checked,
    concurrency: parseInt($("#opt-concurrency").value) || 3,
    proxies: $("#opt-proxies").value.trim(),
  };

  if (currentMode === "keyword") {
    const keyword = $("#keyword").value.trim();
    const cities = $("#cities").value.trim();
    const num = parseInt($("#num-results").value) || 50;
    if (!keyword || !cities) {
      showToast("Enter a keyword and at least one city", "warn");
      return;
    }
    endpoint = "/api/search";
    payload = { keyword, cities, num, ...advancedOpts };
  } else if (currentMode === "maps") {
    const keyword = $("#maps-keyword").value.trim();
    const city = $("#maps-city").value.trim();
    const maxResults = parseInt($("#maps-max").value) || 100;
    const enrich = $("#maps-enrich").checked;
    if (!keyword) {
      showToast("Enter a business type to search for", "warn");
      return;
    }
    endpoint = "/api/maps";
    payload = { keyword, city, max_results: maxResults, enrich_websites: enrich, ...advancedOpts };
  } else if (currentMode === "import") {
    if (!importedData || !importedData.length) {
      showToast("Upload a JSON file first", "warn");
      return;
    }
    endpoint = "/api/import";
    payload = { records: importedData, ...advancedOpts };
  } else {
    const url = $("#direct-url").value.trim();
    if (!url) {
      showToast("Enter a URL to scrape", "warn");
      return;
    }
    endpoint = "/api/scrape";
    payload = { url, ...advancedOpts };
  }

  // Reset state
  leads = [];
  clearLogs();
  setStatus("pending");
  $("#run-count").textContent = "0";
  $("#progress-fill").style.width = "0%";
  $("#progress-pct").textContent = "0%";
  $("#cancel-btn").hidden = false;
  $("#dataset-empty").hidden = false;
  $("#table-wrap").hidden = true;
  $("#results-count").textContent = "";

  // Switch to Run tab
  switchTab("run");

  try {
    const resp = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (data.error) {
      showToast(data.error, "error");
      setStatus("error");
      return;
    }
    currentJobId = data.job_id;
    $("#run-id").hidden = false;
    $("#run-id-value").textContent = data.job_id;
    startTime = Date.now();
    startDurationTimer();
    setStatus("running");
    connectSSE(currentJobId);
  } catch (err) {
    showToast("Failed to start: " + err.message, "error");
    setStatus("error");
  }
});

// ── Cancel ───────────────────────────────────────────────────────────

$("#cancel-btn").addEventListener("click", async () => {
  if (!currentJobId) return;
  await fetch(`/api/cancel/${currentJobId}`, { method: "POST" });
  $("#cancel-btn").hidden = true;
  addLog("Cancelling...", "warn");
});

// ── SSE progress ─────────────────────────────────────────────────────

function connectSSE(jobId) {
  if (eventSource) eventSource.close();
  eventSource = new EventSource(`/api/progress/${jobId}`);

  eventSource.onmessage = (e) => {
    const data = JSON.parse(e.data);

    // Progress bar
    $("#progress-fill").style.width = data.progress_pct + "%";
    $("#progress-pct").textContent = data.progress_pct + "%";

    // Results count
    if (data.count > 0) {
      $("#run-count").textContent = data.count;
    }

    // Log lines
    if (data.logs) {
      data.logs.forEach((log) => addLog(log.msg, log.level));
    }

    // Status updates
    if (data.status === "searching") setStatus("searching");
    else if (data.status === "scraping") setStatus("scraping");

    // Terminal states
    if (data.status === "done" || data.status === "error" || data.status === "cancelled") {
      eventSource.close();
      eventSource = null;
      stopDurationTimer();
      $("#cancel-btn").hidden = true;

      if (data.status === "done") {
        setStatus("done");
        fetchResults(jobId);
      } else if (data.status === "cancelled") {
        setStatus("cancelled");
      } else {
        setStatus("error");
      }
    }
  };

  eventSource.onerror = () => {
    eventSource.close();
    eventSource = null;
    addLog("Connection lost, retrying...", "warn");
    setTimeout(() => {
      if (currentJobId === jobId) connectSSE(jobId);
    }, 2000);
  };
}

// ── Results ──────────────────────────────────────────────────────────

async function fetchResults(jobId) {
  try {
    const resp = await fetch(`/api/results/${jobId}`);
    const data = await resp.json();
    leads = data.leads || [];
    renderResults(leads);
    $("#dataset-empty").hidden = leads.length > 0;
    $("#table-wrap").hidden = leads.length === 0;
    updateResultsCount(leads.length, leads.length);

    // Auto-switch to dataset tab
    if (leads.length > 0) {
      switchTab("dataset");
    }
  } catch (err) {
    addLog("Failed to load results: " + err.message, "error");
  }
}

function renderResults(data) {
  const body = $("#results-body");
  body.innerHTML = "";
  data.forEach((lead) => {
    const tr = document.createElement("tr");
    if (lead.error) {
      tr.className = "error-row";
      tr.innerHTML = `<td colspan="12">Error: ${esc(lead.error)} — ${esc(lead.url)}</td>`;
    } else {
      const emails = (lead.emails || []).join("; ");
      const phones = (lead.phones || []).join("; ");
      const socials = Object.entries(lead.socials || {})
        .map(([p, u]) => `<a href="${esc(u)}" target="_blank" rel="noopener">${esc(p)}</a>`)
        .join(" ");
      const urlDisplay = lead.url ? `<a href="${esc(lead.url)}" target="_blank" rel="noopener">${esc(truncUrl(lead.url))}</a>` : "";
      const mapsDisplay = lead.maps_url ? `<a href="${esc(lead.maps_url)}" target="_blank" rel="noopener">View</a>` : "";

      tr.innerHTML = `
        <td class="cell-copy" title="Click to copy">${esc(lead.company || "")}</td>
        <td class="cell-copy" title="Click to copy">${esc(lead.category || "")}</td>
        <td>${urlDisplay}</td>
        <td class="cell-copy" title="Click to copy">${esc(emails)}</td>
        <td class="cell-copy" title="Click to copy">${esc(phones)}</td>
        <td class="cell-copy" title="Click to copy">${esc(lead.address || "")}</td>
        <td class="cell-copy" title="Click to copy">${esc(lead.hours || "")}</td>
        <td class="cell-copy" title="Click to copy">${lead.google_reviews != null ? esc(String(lead.google_reviews)) : ""}</td>
        <td class="cell-copy" title="Click to copy">${lead.google_rating != null ? esc(String(lead.google_rating)) : ""}</td>
        <td class="cell-copy" title="Click to copy">${esc(lead.price_level || "")}</td>
        <td class="cell-socials">${socials}</td>
        <td>${mapsDisplay}</td>
      `;
      tr.querySelectorAll(".cell-copy").forEach((td) => {
        td.addEventListener("click", () => {
          const text = td.textContent.trim();
          if (text) {
            navigator.clipboard.writeText(text);
            showToast("Copied: " + text.slice(0, 50));
          }
        });
      });
    }
    body.appendChild(tr);
  });
}

// ── Sorting ──────────────────────────────────────────────────────────

$$("th.sortable").forEach((th) => {
  th.addEventListener("click", () => {
    const col = th.dataset.col;
    if (sortCol === col) {
      sortAsc = !sortAsc;
    } else {
      sortCol = col;
      sortAsc = true;
    }
    $$("th.sortable").forEach((h) => h.classList.remove("sort-asc", "sort-desc"));
    th.classList.add(sortAsc ? "sort-asc" : "sort-desc");

    const sorted = [...leads].sort((a, b) => {
      const va = cellVal(a, col);
      const vb = cellVal(b, col);
      return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
    });
    renderResults(sorted);
  });
});

function cellVal(lead, col) {
  if (col === "emails") return (lead.emails || []).join("; ").toLowerCase();
  if (col === "phones") return (lead.phones || []).join("; ");
  return (lead[col] || "").toString().toLowerCase();
}

// ── Filtering ────────────────────────────────────────────────────────

$("#filter-input").addEventListener("input", () => {
  const q = $("#filter-input").value.toLowerCase();
  if (!q) {
    renderResults(leads);
    updateResultsCount(leads.length, leads.length);
    return;
  }
  const filtered = leads.filter((lead) => {
    const text = [
      lead.company, lead.url, lead.category || "",
      (lead.emails || []).join(" "),
      (lead.phones || []).join(" "),
      lead.address || "", lead.hours || "",
      lead.price_level || "",
      Object.keys(lead.socials || {}).join(" "),
    ].join(" ").toLowerCase();
    return text.includes(q);
  });
  renderResults(filtered);
  updateResultsCount(filtered.length, leads.length);
});

// ── Export ────────────────────────────────────────────────────────────

$("#export-csv-btn").addEventListener("click", () => {
  if (currentJobId) window.location.href = `/api/export/${currentJobId}?format=csv`;
});

$("#export-json-btn").addEventListener("click", () => {
  if (currentJobId) window.location.href = `/api/export/${currentJobId}?format=json`;
});

$("#copy-emails-btn").addEventListener("click", () => {
  const all = new Set();
  leads.forEach((l) => (l.emails || []).forEach((e) => all.add(e)));
  if (!all.size) return showToast("No emails to copy", "warn");
  navigator.clipboard.writeText([...all].sort().join("\n"));
  showToast(`Copied ${all.size} email${all.size !== 1 ? "s" : ""}`);
});

// ── Logs ─────────────────────────────────────────────────────────────

function clearLogs() {
  $("#logs-container").innerHTML = "";
}

function addLog(msg, level = "info") {
  const container = $("#logs-container");
  // Remove empty placeholder
  const empty = container.querySelector(".log-empty");
  if (empty) empty.remove();

  const line = document.createElement("div");
  line.className = `log-line log-${level}`;
  const ts = new Date().toLocaleTimeString();
  line.innerHTML = `<span class="log-ts">${ts}</span><span class="log-msg">${esc(msg)}</span>`;
  container.appendChild(line);
  container.scrollTop = container.scrollHeight;
}

// ── Status badge ─────────────────────────────────────────────────────

function setStatus(status) {
  const labels = {
    idle: "Idle", pending: "Pending", running: "Running",
    searching: "Searching", scraping: "Scraping",
    done: "Succeeded", error: "Failed", cancelled: "Cancelled",
  };
  const colors = {
    idle: "badge-idle", pending: "badge-pending", running: "badge-running",
    searching: "badge-running", scraping: "badge-running",
    done: "badge-success", error: "badge-error", cancelled: "badge-cancelled",
  };

  const label = labels[status] || status;
  const cls = colors[status] || "badge-idle";

  // Top bar badge
  const topBadge = $("#status-badge");
  topBadge.textContent = label;
  topBadge.className = "badge " + cls;

  // Run tab badge
  const runBadge = $("#run-status");
  runBadge.textContent = label;
  runBadge.className = "badge " + cls;
}

// ── Duration timer ───────────────────────────────────────────────────

function startDurationTimer() {
  stopDurationTimer();
  durationTimer = setInterval(() => {
    if (startTime) {
      const secs = Math.floor((Date.now() - startTime) / 1000);
      const m = Math.floor(secs / 60);
      const s = secs % 60;
      $("#run-duration").textContent = m > 0 ? `${m}m ${s}s` : `${s}s`;
    }
  }, 1000);
}

function stopDurationTimer() {
  if (durationTimer) {
    clearInterval(durationTimer);
    durationTimer = null;
  }
}

// ── Helpers ──────────────────────────────────────────────────────────

function updateResultsCount(shown, total) {
  const el = $("#results-count");
  if (shown === total) {
    el.textContent = `${total} item${total !== 1 ? "s" : ""}`;
  } else {
    el.textContent = `${shown} of ${total} items`;
  }
}

function esc(str) {
  const d = document.createElement("div");
  d.textContent = str || "";
  return d.innerHTML;
}

function truncUrl(url) {
  try {
    const u = new URL(url);
    return u.hostname + (u.pathname !== "/" ? u.pathname : "");
  } catch { return url; }
}

function showToast(msg, level = "info") {
  const toast = $("#toast");
  toast.textContent = msg;
  toast.className = "toast show toast-" + level;
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => (toast.className = "toast"), 2500);
}
