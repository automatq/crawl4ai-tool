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
    $("#homestars-fields").hidden = currentMode !== "homestars";
    $("#url-fields").hidden = currentMode !== "url";
    $("#import-fields").hidden = currentMode !== "import";
    // Load HomeStars city selector on first switch
    if (currentMode === "homestars" && !hsProvincesLoaded) loadHsProvinces();
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

// ── City selector ────────────────────────────────────────────────────

let allCities = [];
let filteredCities = [];
let selectedCityIds = new Set();

async function loadProvinces() {
  try {
    const resp = await fetch("/api/provinces");
    const data = await resp.json();
    const sel = document.getElementById("filter-province");
    data.provinces.forEach((p) => {
      const opt = document.createElement("option");
      opt.value = p;
      opt.textContent = p;
      sel.appendChild(opt);
    });
  } catch (e) {
    console.error("Failed to load provinces:", e);
  }
}

async function loadCities() {
  const province = document.getElementById("filter-province").value;
  const minPop = document.getElementById("filter-population").value;
  const params = new URLSearchParams();
  if (province) params.set("province", province);
  if (minPop && minPop !== "0") params.set("min_population", minPop);

  try {
    const resp = await fetch("/api/cities?" + params);
    const data = await resp.json();
    filteredCities = data.cities;
    renderCityList();
  } catch (e) {
    console.error("Failed to load cities:", e);
  }
}

function renderCityList() {
  const container = document.getElementById("city-list");
  if (!filteredCities.length) {
    container.innerHTML =
      '<div class="city-list-empty">No cities match filters</div>';
    updateCityCount();
    return;
  }
  container.innerHTML = filteredCities
    .map(
      (c) => `
    <label class="city-item">
      <input type="checkbox" value="${c.id}" ${selectedCityIds.has(c.id) ? "checked" : ""}
             onchange="toggleCity(${c.id}, this.checked)">
      <span class="city-name">${esc(c.city)}</span>
      <span class="city-province">${esc(c.province_id)}</span>
      <span class="city-pop">${(c.population || 0).toLocaleString()}</span>
    </label>`
    )
    .join("");
  updateCityCount();
}

function toggleCity(id, checked) {
  if (checked) selectedCityIds.add(id);
  else selectedCityIds.delete(id);
  updateCityCount();
}

function updateCityCount() {
  document.getElementById("city-count").textContent =
    `${selectedCityIds.size} cities selected`;
}

document.getElementById("select-all-cities").addEventListener("click", () => {
  filteredCities.forEach((c) => selectedCityIds.add(c.id));
  renderCityList();
});

document.getElementById("deselect-all-cities").addEventListener("click", () => {
  selectedCityIds.clear();
  renderCityList();
});

document
  .getElementById("filter-province")
  .addEventListener("change", loadCities);
document
  .getElementById("filter-population")
  .addEventListener("change", loadCities);

// Load on page init
loadProvinces();
loadCities();

// ── HomeStars city selector (separate state from Maps) ──────────────

let hsAllCities = [];
let hsFilteredCities = [];
let hsSelectedCityIds = new Set();
let hsProvincesLoaded = false;

async function loadHsProvinces() {
  if (hsProvincesLoaded) return;
  try {
    const resp = await fetch("/api/provinces");
    const data = await resp.json();
    const sel = document.getElementById("hs-filter-province");
    data.provinces.forEach((p) => {
      const opt = document.createElement("option");
      opt.value = p;
      opt.textContent = p;
      sel.appendChild(opt);
    });
    hsProvincesLoaded = true;
    loadHsCities();
  } catch (e) {
    console.error("Failed to load HS provinces:", e);
  }
}

async function loadHsCities() {
  const province = document.getElementById("hs-filter-province").value;
  const minPop = document.getElementById("hs-filter-population").value;
  const params = new URLSearchParams();
  if (province) params.set("province", province);
  if (minPop && minPop !== "0") params.set("min_population", minPop);

  try {
    const resp = await fetch("/api/cities?" + params);
    const data = await resp.json();
    hsFilteredCities = data.cities;
    renderHsCityList();
  } catch (e) {
    console.error("Failed to load HS cities:", e);
  }
}

function renderHsCityList() {
  const container = document.getElementById("hs-city-list");
  if (!hsFilteredCities.length) {
    container.innerHTML = '<div class="city-list-empty">No cities match filters</div>';
    updateHsCityCount();
    return;
  }
  container.innerHTML = hsFilteredCities.map(c => `
    <label class="city-item">
      <input type="checkbox" value="${c.id}" ${hsSelectedCityIds.has(c.id) ? "checked" : ""}
             onchange="toggleHsCity(${c.id}, this.checked)">
      <span class="city-name">${esc(c.city)}</span>
      <span class="city-province">${esc(c.province_id)}</span>
      <span class="city-pop">${(c.population || 0).toLocaleString()}</span>
    </label>`
  ).join("");
  updateHsCityCount();
}

function toggleHsCity(id, checked) {
  if (checked) hsSelectedCityIds.add(id);
  else hsSelectedCityIds.delete(id);
  updateHsCityCount();
}

function updateHsCityCount() {
  document.getElementById("hs-city-count").textContent =
    `${hsSelectedCityIds.size} cities selected`;
}

document.getElementById("hs-select-all").addEventListener("click", () => {
  hsFilteredCities.forEach(c => hsSelectedCityIds.add(c.id));
  renderHsCityList();
});

document.getElementById("hs-deselect-all").addEventListener("click", () => {
  hsSelectedCityIds.clear();
  renderHsCityList();
});

document.getElementById("hs-filter-province").addEventListener("change", loadHsCities);
document.getElementById("hs-filter-population").addEventListener("change", loadHsCities);

// ── Outreach toggle ──────────────────────────────────────────────────

$("#opt-outreach").addEventListener("change", () => {
  $("#outreach-fields").hidden = !$("#opt-outreach").checked;
});

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
    outreach_enabled: $("#opt-outreach").checked,
    sender_email: $("#sender-email").value.trim(),
    sender_phone: $("#sender-phone").value.trim(),
    sender_company: $("#sender-company").value.trim(),
    message_template: $("#message-template").value.trim(),
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
    const maxResults = parseInt($("#maps-max").value) || 500;
    const enrich = $("#maps-enrich").checked;
    const gridSpacing = parseFloat($("#maps-grid-spacing")?.value) || 1.0;
    if (!keyword) {
      showToast("Enter a business type to search for", "warn");
      return;
    }

    // Check for custom polygon first (advanced fallback)
    const rawPolygon = $("#maps-polygon")?.value?.trim();
    if (rawPolygon) {
      let polygonData;
      try {
        polygonData = JSON.parse(rawPolygon);
      } catch {
        showToast("Invalid GeoJSON — check your polygon JSON", "error");
        return;
      }
      endpoint = "/api/maps";
      payload = {
        keyword,
        max_results: maxResults,
        enrich_websites: enrich,
        area_search: true,
        polygon: polygonData,
        grid_spacing_km: gridSpacing,
        ...advancedOpts,
      };
    } else if (selectedCityIds.size > 0) {
      // Multi-city mode
      endpoint = "/api/maps";
      payload = {
        keyword,
        city_ids: [...selectedCityIds],
        max_results: maxResults,
        enrich_websites: enrich,
        grid_spacing_km: gridSpacing,
        ...advancedOpts,
      };
    } else {
      showToast("Select at least one city or paste a custom polygon", "warn");
      return;
    }
  } else if (currentMode === "homestars") {
    const keyword = $("#hs-keyword").value.trim();
    const maxResults = parseInt($("#hs-max").value) || 100;
    const enrich = $("#hs-enrich").checked;
    if (!keyword) {
      showToast("Enter a service category to search for", "warn");
      return;
    }
    if (hsSelectedCityIds.size === 0) {
      showToast("Select at least one city", "warn");
      return;
    }
    endpoint = "/api/homestars";
    payload = {
      keyword,
      city_ids: [...hsSelectedCityIds],
      max_results: maxResults,
      enrich_websites: enrich,
      ...advancedOpts,
    };
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
    else if (data.status === "outreach") setStatus("outreach");

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
      loadRunHistory();
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

function renderResults(data, targetBody = "#results-body") {
  const body = $(targetBody);
  body.innerHTML = "";
  data.forEach((lead) => {
    const tr = document.createElement("tr");
    if (lead.error) {
      tr.className = "error-row";
      tr.innerHTML = `<td colspan="13">Error: ${esc(lead.error)} — ${esc(lead.url)}</td>`;
    } else {
      const emails = (lead.emails || []).join("; ");
      const phones = (lead.phones || []).join("; ");
      const socials = Object.entries(lead.socials || {})
        .map(([p, u]) => `<a href="${esc(u)}" target="_blank" rel="noopener">${esc(p)}</a>`)
        .join(" ");
      const urlDisplay = lead.url ? `<a href="${esc(lead.url)}" target="_blank" rel="noopener">${esc(truncUrl(lead.url))}</a>` : "";
      // Show Maps or HomeStars link
      const extUrl = lead.maps_url || lead.homestars_url || "";
      const extLabel = lead.maps_url ? "Maps" : lead.homestars_url ? "HS" : "";
      const extDisplay = extUrl ? `<a href="${esc(extUrl)}" target="_blank" rel="noopener">${extLabel}</a>` : "";
      // Show whichever rating/reviews are available (Google or HomeStars)
      const reviews = lead.google_reviews != null ? lead.google_reviews : lead.homestars_reviews;
      const rating = lead.google_rating != null ? lead.google_rating
        : lead.homestars_rating != null ? lead.homestars_rating + "/10" : null;

      tr.innerHTML = `
        <td class="cell-copy" title="Click to copy">${esc(lead.company || "")}</td>
        <td class="cell-copy" title="Click to copy">${esc(lead.category || "")}</td>
        <td>${urlDisplay}</td>
        <td class="cell-copy" title="Click to copy">${esc(emails)}</td>
        <td class="cell-copy" title="Click to copy">${esc(phones)}</td>
        <td class="cell-copy" title="Click to copy">${esc(lead.address || "")}</td>
        <td class="cell-copy" title="Click to copy">${esc(lead.hours || "")}</td>
        <td class="cell-copy" title="Click to copy">${reviews != null ? esc(String(reviews)) : ""}</td>
        <td class="cell-copy" title="Click to copy">${rating != null ? esc(String(rating)) : ""}</td>
        <td class="cell-copy" title="Click to copy">${esc(lead.price_level || "")}</td>
        <td class="cell-socials">${socials}</td>
        <td class="cell-outreach" title="${esc(lead.outreach_detail || "")}">${outreachBadge(lead.outreach_status)}</td>
        <td>${extDisplay}</td>
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
    searching: "Searching", scraping: "Scraping", outreach: "Outreach",
    done: "Succeeded", error: "Failed", cancelled: "Cancelled",
  };
  const colors = {
    idle: "badge-idle", pending: "badge-pending", running: "badge-running",
    searching: "badge-running", scraping: "badge-running", outreach: "badge-running",
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

function outreachBadge(status) {
  if (!status) return "";
  const colors = {
    submitted: "#16a34a",
    captcha_blocked: "#ca8a04",
    no_form_found: "#9ca3af",
    skipped: "#9ca3af",
    failed: "#dc2626",
    error: "#dc2626",
  };
  const color = colors[status] || "#9ca3af";
  return `<span style="color:${color};font-size:12px;font-weight:500">${esc(status)}</span>`;
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

// ── Run History ───────────────────────────────────────────────────────

function fmtDuration(seconds) {
  if (!seconds) return "—";
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.round(seconds % 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m === 0) return `${s}s`;
  return `${m}m ${s}s`;
}

function fmtDate(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString() + " " + d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function statusBadge(status) {
  const colors = { completed: "#22c55e", done: "#22c55e", running: "#3b82f6", error: "#ef4444", cancelled: "#9ca3af", failed: "#ef4444" };
  const color = colors[status] || "#9ca3af";
  return `<span style="color:${color};font-weight:600">${esc(status)}</span>`;
}

function statusIcon(status) {
  if (status === "done" || status === "completed")
    return '<span class="status-icon status-success">&#10003;</span>';
  if (status === "error" || status === "failed")
    return '<span class="status-icon status-error">&#10007;</span>';
  if (status === "running")
    return '<span class="status-icon status-running">&#9679;</span>';
  return '<span class="status-icon status-neutral">&mdash;</span>';
}

async function loadRunHistory() {
  try {
    const resp = await fetch("/api/runs");
    const data = await resp.json();
    const runs = data.runs || [];
    const body = $("#history-body");
    const empty = $("#history-empty");
    const wrap = $("#history-table-wrap");

    if (runs.length === 0) {
      empty.hidden = false;
      wrap.hidden = true;
      return;
    }

    empty.hidden = true;
    wrap.hidden = false;
    body.innerHTML = runs.map(r => {
      let cities = r.cities || "—";
      try {
        const arr = JSON.parse(cities);
        if (Array.isArray(arr)) cities = `${arr.length} cities`;
      } catch {}
      return `<tr data-run-id="${esc(r.run_id)}" class="history-row-clickable">
        <td>${statusIcon(r.status)} ${statusBadge(r.status)}</td>
        <td>${esc(r.mode || "—")}</td>
        <td>${esc(r.keyword || "—")}</td>
        <td>${esc(cities)}</td>
        <td>${r.lead_count ?? "—"}</td>
        <td>${fmtDate(r.created_at)}</td>
        <td>${fmtDuration(r.duration_seconds)}</td>
      </tr>`;
    }).join("");

    body.querySelectorAll("tr[data-run-id]").forEach(tr => {
      tr.addEventListener("click", () => showRunDetail(tr.dataset.runId));
    });
  } catch (err) {
    console.error("Failed to load run history:", err);
  }
}

// ── Run Detail View ──────────────────────────────────────────────────

let detailLeads = [];
let detailRunId = null;

async function showRunDetail(runId) {
  detailRunId = runId;
  $("#history-list-view").hidden = true;
  $("#history-detail-view").hidden = false;

  // Loading state
  $("#detail-status").textContent = "Loading...";
  $("#detail-mode").textContent = "";
  $("#detail-keyword").textContent = "";
  $("#detail-lead-count").textContent = "";
  $("#detail-started").textContent = "";
  $("#detail-duration").textContent = "";
  $("#detail-toolbar").hidden = true;
  $("#detail-table-wrap").hidden = true;
  $("#detail-expired").hidden = true;

  try {
    const resp = await fetch(`/api/runs/${runId}`);
    const data = await resp.json();
    if (data.error) {
      showToast(data.error, "error");
      return;
    }

    // Populate header
    $("#detail-status").innerHTML = statusIcon(data.status) + " " + statusBadge(data.status);
    $("#detail-mode").textContent = data.mode || "—";
    $("#detail-keyword").textContent = data.keyword || "—";
    $("#detail-lead-count").textContent = data.lead_count ?? "—";
    $("#detail-started").textContent = fmtDate(data.created_at);
    $("#detail-duration").textContent = fmtDuration(data.duration_seconds);

    if (data.leads && data.leads.length > 0) {
      detailLeads = data.leads;
      renderResults(detailLeads, "#detail-results-body");
      $("#detail-toolbar").hidden = false;
      $("#detail-table-wrap").hidden = false;
      $("#detail-results-count").textContent = `${detailLeads.length} results`;
    } else if (data.backup_expired) {
      detailLeads = [];
      $("#detail-expired").hidden = false;
    } else {
      detailLeads = [];
      $("#detail-expired").hidden = false;
    }
  } catch (err) {
    showToast("Failed to load run: " + err.message, "error");
  }
}

// Back button
$("#history-back-btn").addEventListener("click", () => {
  $("#history-detail-view").hidden = true;
  $("#history-list-view").hidden = false;
  detailRunId = null;
  detailLeads = [];
});

// Detail export buttons
$("#detail-export-csv-btn").addEventListener("click", () => {
  if (detailRunId) window.location.href = `/api/export/run/${detailRunId}?format=csv`;
});

$("#detail-export-json-btn").addEventListener("click", () => {
  if (detailRunId) window.location.href = `/api/export/run/${detailRunId}?format=json`;
});

// Copy emails from detail view
$("#detail-copy-emails-btn").addEventListener("click", () => {
  const all = new Set();
  detailLeads.forEach(l => (l.emails || []).forEach(e => all.add(e)));
  if (!all.size) return showToast("No emails to copy", "warn");
  navigator.clipboard.writeText([...all].sort().join("\n"));
  showToast(`Copied ${all.size} email${all.size !== 1 ? "s" : ""}`);
});

// Filter in detail view
$("#detail-filter-input").addEventListener("input", () => {
  const q = $("#detail-filter-input").value.toLowerCase();
  if (!q) {
    renderResults(detailLeads, "#detail-results-body");
    $("#detail-results-count").textContent = `${detailLeads.length} results`;
    return;
  }
  const filtered = detailLeads.filter(lead => {
    const text = [
      lead.company, lead.url, lead.category || "",
      (lead.emails || []).join(" "), (lead.phones || []).join(" "),
      lead.address || "",
    ].join(" ").toLowerCase();
    return text.includes(q);
  });
  renderResults(filtered, "#detail-results-body");
  $("#detail-results-count").textContent = `${filtered.length} of ${detailLeads.length} results`;
});

// Load history on page init
loadRunHistory();
