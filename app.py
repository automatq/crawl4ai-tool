#!/usr/bin/env python3
"""
Crawl4AI Lead Scraper — Desktop App
"""

import asyncio
import csv
import json
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from scrape import search_leads, scrape_all, normalize_url
import scoring


# ── App ───────────────────────────────────────────────────────────────

class LeadScraperApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Lead Scraper")
        self.root.geometry("1100x700")
        self.root.minsize(900, 500)

        self.leads: list[dict] = []
        self._cancel = False

        self._build_ui()

    # ── Layout ────────────────────────────────────────────────────────

    def _build_ui(self):
        # Top frame: inputs
        top = ttk.LabelFrame(self.root, text="Search", padding=12)
        top.pack(fill="x", padx=12, pady=(12, 6))

        # Row 1: keyword + cities
        ttk.Label(top, text="Keyword:").grid(row=0, column=0, sticky="w")
        self.keyword_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.keyword_var, width=25).grid(
            row=0, column=1, padx=(4, 16), sticky="ew"
        )

        ttk.Label(top, text="Cities (comma-separated):").grid(row=0, column=2, sticky="w")
        self.cities_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.cities_var, width=40).grid(
            row=0, column=3, padx=(4, 16), sticky="ew"
        )

        ttk.Label(top, text="Max leads:").grid(row=0, column=4, sticky="w")
        self.num_var = tk.StringVar(value="50")
        ttk.Entry(top, textvariable=self.num_var, width=7).grid(
            row=0, column=5, padx=(4, 16), sticky="w"
        )

        top.columnconfigure(1, weight=1)
        top.columnconfigure(3, weight=2)

        # Row 2: direct URL + buttons
        ttk.Label(top, text="Or direct URL:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.url_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.url_var, width=60).grid(
            row=1, column=1, columnspan=3, padx=(4, 16), sticky="ew", pady=(8, 0)
        )

        btn_frame = ttk.Frame(top)
        btn_frame.grid(row=1, column=4, columnspan=2, pady=(8, 0))

        self.search_btn = ttk.Button(btn_frame, text="Scrape", command=self._on_scrape)
        self.search_btn.pack(side="left", padx=4)

        self.stop_btn = ttk.Button(btn_frame, text="Stop", command=self._on_stop, state="disabled")
        self.stop_btn.pack(side="left", padx=4)

        self.export_btn = ttk.Button(btn_frame, text="Export CSV", command=self._on_export, state="disabled")
        self.export_btn.pack(side="left", padx=4)

        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief="sunken", anchor="w", padding=4)
        status_bar.pack(fill="x", side="bottom", padx=12, pady=(0, 8))

        # Progress bar (determinate for real progress)
        self.progress = ttk.Progressbar(self.root, mode="determinate", maximum=100)
        self.progress.pack(fill="x", padx=12, pady=(0, 4), side="bottom")

        # Results table
        table_frame = ttk.Frame(self.root)
        table_frame.pack(fill="both", expand=True, padx=12, pady=6)

        columns = ("tier", "score", "company", "url", "emails", "phones", "address", "socials")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", selectmode="browse")

        self.tree.heading("tier", text="Tier")
        self.tree.heading("score", text="Score")
        self.tree.heading("company", text="Company")
        self.tree.heading("url", text="URL")
        self.tree.heading("emails", text="Emails")
        self.tree.heading("phones", text="Phones")
        self.tree.heading("address", text="Address")
        self.tree.heading("socials", text="Socials")

        self.tree.column("tier", width=60, minwidth=50)
        self.tree.column("score", width=50, minwidth=40)
        self.tree.column("company", width=160, minwidth=100)
        self.tree.column("url", width=180, minwidth=120)
        self.tree.column("emails", width=180, minwidth=100)
        self.tree.column("phones", width=130, minwidth=80)
        self.tree.column("address", width=160, minwidth=80)
        self.tree.column("socials", width=160, minwidth=80)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        # Double-click to copy cell
        self.tree.bind("<Double-1>", self._on_cell_click)

    # ── Progress callback (called from worker thread) ─────────────────

    def _update_status(self, current: int, total: int, msg: str):
        pct = int((current / max(total, 1)) * 100)
        self.root.after(0, lambda: self.progress.configure(value=pct))
        self.root.after(0, lambda: self.status_var.set(msg))

    # ── Actions ───────────────────────────────────────────────────────

    def _on_scrape(self):
        keyword = self.keyword_var.get().strip()
        cities_raw = self.cities_var.get().strip()
        direct_url = self.url_var.get().strip()

        if not keyword and not direct_url:
            messagebox.showwarning("Input needed", "Enter a keyword + cities, or a direct URL.")
            return

        if keyword and not cities_raw:
            messagebox.showwarning("Input needed", "Enter at least one city for keyword search.")
            return

        try:
            num = int(self.num_var.get() or 50)
        except ValueError:
            messagebox.showwarning("Input needed", "Max leads must be a number.")
            return

        self._cancel = False
        self.search_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.export_btn.configure(state="disabled")
        self.progress.configure(value=0, mode="indeterminate")
        self.progress.start(10)

        # Clear table
        for item in self.tree.get_children():
            self.tree.delete(item)

        if keyword:
            cities = [c.strip() for c in cities_raw.split(",") if c.strip()]
            self.status_var.set(f"Searching for '{keyword}' across {len(cities)} city/cities...")
            thread = threading.Thread(
                target=self._run_keyword_search, args=(keyword, cities, num), daemon=True
            )
        else:
            url = normalize_url(direct_url)
            self.status_var.set(f"Scraping {url}...")
            thread = threading.Thread(
                target=self._run_url_scrape, args=([url],), daemon=True
            )

        thread.start()

    def _on_stop(self):
        self._cancel = True
        self.status_var.set("Stopping...")

    def _run_keyword_search(self, keyword: str, cities: list[str], num: int):
        try:
            # Search phase
            def search_progress(found, target, msg):
                if self._cancel:
                    raise InterruptedError("Cancelled")
                self.root.after(0, lambda: self.status_var.set(
                    f"Finding sites... {found}/{target} — {msg}"
                ))

            urls = asyncio.run(search_leads(keyword, cities, num, on_progress=search_progress))

            if self._cancel:
                self.root.after(0, self._finish, [], "Stopped.")
                return

            if not urls:
                self.root.after(0, self._finish, [], "No business sites found.")
                return

            # Switch progress bar to determinate for scraping phase
            self.root.after(0, lambda: self.progress.stop())
            self.root.after(0, lambda: self.progress.configure(mode="determinate", value=0))

            def scrape_progress(i, total, msg):
                if self._cancel:
                    raise InterruptedError("Cancelled")
                self._update_status(i, total, f"Scraping {i}/{total} sites...")

            leads = asyncio.run(scrape_all(urls, on_progress=scrape_progress))
            self.root.after(0, self._finish, leads, f"Done — {len(leads)} leads scraped.")

        except InterruptedError:
            self.root.after(0, self._finish, [], "Stopped by user.")
        except Exception as e:
            self.root.after(0, self._finish, [], f"Error: {e}")

    def _run_url_scrape(self, urls: list[str]):
        try:
            def scrape_progress(i, total, msg):
                if self._cancel:
                    raise InterruptedError("Cancelled")
                self._update_status(i, total, msg)

            leads = asyncio.run(scrape_all(urls, on_progress=scrape_progress))
            self.root.after(0, self._finish, leads, f"Done — {len(leads)} leads found.")
        except InterruptedError:
            self.root.after(0, self._finish, [], "Stopped by user.")
        except Exception as e:
            self.root.after(0, self._finish, [], f"Error: {e}")

    def _finish(self, leads: list[dict], message: str):
        self.progress.stop()
        self.progress.configure(mode="determinate", value=100 if leads else 0)
        self.search_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.status_var.set(message)
        leads = scoring.sort_by_score(scoring.annotate(leads))
        self.leads = leads

        for lead in leads:
            if "error" in lead:
                self.tree.insert("", "end", values=(
                    "dead", "",
                    f"ERROR: {lead.get('error', '')}",
                    lead.get("url", ""),
                    "", "", "", "",
                ))
            else:
                socials_str = ", ".join(
                    f"{p}: {u}" for p, u in lead.get("socials", {}).items()
                )
                self.tree.insert("", "end", values=(
                    lead.get("tier", ""),
                    lead.get("score", ""),
                    lead.get("company", ""),
                    lead.get("url", ""),
                    "; ".join(lead.get("emails", [])),
                    "; ".join(lead.get("phones", [])),
                    lead.get("address", "") or "",
                    socials_str,
                ))

        if leads:
            self.export_btn.configure(state="normal")

    def _on_export(self):
        if not self.leads:
            return

        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile="leads.csv",
        )
        if not path:
            return

        count = 0
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "tier", "score", "company", "url", "emails",
                "phones", "address", "socials",
            ])
            for lead in self.leads:
                if "error" in lead:
                    continue
                w.writerow([
                    lead.get("tier", ""),
                    lead.get("score", ""),
                    lead.get("company", ""),
                    lead.get("url", ""),
                    "; ".join(lead.get("emails", [])),
                    "; ".join(lead.get("phones", [])),
                    lead.get("address", "") or "",
                    json.dumps(lead.get("socials", {})),
                ])
                count += 1

        self.status_var.set(f"Exported {count} leads to {path}")

    def _on_cell_click(self, event):
        """Copy clicked cell value to clipboard."""
        item = self.tree.identify_row(event.y)
        col = self.tree.identify_column(event.x)
        if not item or not col:
            return
        col_idx = int(col.replace("#", "")) - 1
        values = self.tree.item(item, "values")
        if col_idx < len(values) and values[col_idx]:
            self.root.clipboard_clear()
            self.root.clipboard_append(values[col_idx])
            self.status_var.set(f"Copied: {values[col_idx][:80]}")


# ── Main ──────────────────────────────────────────────────────────────

def main():
    root = tk.Tk()
    LeadScraperApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
