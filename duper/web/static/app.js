// DUPer Web UI Application

class DuperApp {
    constructor() {
        this.apiKey = localStorage.getItem('duper_api_key') || '';
        this.baseUrl = window.location.origin;
        this.currentScanId = null;
        this.scanPollInterval = null;
        this.currentMediaData = null;
        this.currentMediaMode = 'orphaned';
        this.currentSavesData = null;
        this.currentSavesMode = 'orphaned';
        this.currentDuplicatesData = null;
        this.duplicatesViewMode = 'gallery';
        this.coverArtCache = new Map();

        // Library management
        this.libraries = [];
        this.currentLibraryId = localStorage.getItem('duper_current_library') || '';
        this.games = [];
        this.gamesFilter = '';
        this.gamesViewMode = 'cards';
        this.queuePollInterval = null;

        // Gamification system
        this._initGamification();

        // Gamification state for transfers — persisted to localStorage
        const saved = JSON.parse(localStorage.getItem('duper_transfer_state') || '{}');
        this._transferXP = saved.xp || 0;
        this._transferLevel = saved.level || 1;
        this._transferCombo = saved.combo || 0;
        this._transferLastFile = saved.lastFile || null;
        this._transferLastFileCount = saved.lastFileCount || 0;
        this._transferLog = saved.log || [];
        this._transferCompletedSystems = new Set(saved.completedSystems || []);
        this._transferLastSpeed = saved.lastSpeed || 0;

        // Acquisition state
        this._acqLastStatus = null;
        this._acqLastFile = localStorage.getItem('duper_acq_last_file') || '';

        // Auto-manage state
        this._autoManageEnabled = localStorage.getItem('duper_auto_manage') === '1';
        this._autoMediaStarted = false;
        this._autoAcqStarted = false;
        this._autoManageLog = [];

        // View state persistence
        this._currentView = localStorage.getItem('duper_current_view') || 'dashboard';
        this._pollTimer = null;
        this._pollInterval = 500; // 500ms refresh

        this.init();
    }

    // Smooth DOM update — only touches elements whose text actually changed
    _setText(id, value) {
        const el = document.getElementById(id);
        if (el && el.textContent !== String(value)) {
            el.textContent = value;
        }
    }

    _setWidth(id, pct) {
        const el = document.getElementById(id);
        if (el) {
            const target = `${pct}%`;
            if (el.style.width !== target) el.style.width = target;
        }
    }

    async init() {
        this.bindEvents();
        const connected = await this.checkConnection();
        if (connected) {
            await this.loadLibraries();
            await this.restoreActiveScan();
        }
        // Restore persisted view
        this.switchView(this._currentView);
        // Restore gamification UI from localStorage immediately
        this._renderTransferLog();
        // Restore auto-manage toggle
        const autoToggle = document.getElementById('auto-manage-toggle');
        if (autoToggle) autoToggle.checked = this._autoManageEnabled;
        // Start global poll loop
        this._startPoll();
        // Mouse tracking for card tilt effects
        this._initMouseTracking();
    }

    _initMouseTracking() {
        document.addEventListener('mousemove', (e) => {
            const cards = document.querySelectorAll('.game-card:hover, .glass-card:hover, .acq-collection-card:hover');
            cards.forEach(card => {
                const rect = card.getBoundingClientRect();
                const x = (e.clientX - rect.left) / rect.width - 0.5;
                const y = (e.clientY - rect.top) / rect.height - 0.5;
                const tiltX = y * -8;
                const tiltY = x * 8;
                card.style.transform = `perspective(800px) rotateX(${tiltX}deg) rotateY(${tiltY}deg) translateY(-4px)`;

                // Move glow pseudo-element via CSS custom properties
                card.style.setProperty('--mouse-x', `${e.clientX - rect.left}px`);
                card.style.setProperty('--mouse-y', `${e.clientY - rect.top}px`);
            });
        });
        document.addEventListener('mouseleave', () => {
            document.querySelectorAll('.game-card, .glass-card, .acq-collection-card').forEach(card => {
                card.style.transform = '';
            });
        }, true);
        // Reset tilt when mouse leaves a card
        document.addEventListener('mouseout', (e) => {
            if (e.target.closest && e.target.closest('.game-card, .glass-card, .acq-collection-card')) {
                const card = e.target.closest('.game-card, .glass-card, .acq-collection-card');
                if (!card.matches(':hover')) {
                    card.style.transform = '';
                }
            }
        });
    }

    _startPoll() {
        if (this._pollTimer) clearInterval(this._pollTimer);
        this._pollTimer = setInterval(() => this._pollActiveView(), this._pollInterval);
    }

    _saveTransferState() {
        localStorage.setItem('duper_transfer_state', JSON.stringify({
            xp: this._transferXP,
            level: this._transferLevel,
            combo: this._transferCombo,
            lastFile: this._transferLastFile,
            lastFileCount: this._transferLastFileCount,
            log: this._transferLog.slice(0, 20),
            completedSystems: [...this._transferCompletedSystems],
            lastSpeed: this._transferLastSpeed,
        }));
    }

    async _pollActiveView() {
        try {
            // Always poll transfers — they run in the background regardless of view
            await this._pollTransfers();

            // Update nav badges
            this._updateNavBadges();

            switch (this._currentView) {
                case 'dashboard':
                    await this._pollDashboard();
                    break;
                case 'retronas':
                    await this._pollRetroNAS();
                    break;
                case 'scan':
                    if (this.currentScanId) await this._pollScan();
                    break;
                case 'queue':
                    await this._pollQueue();
                    break;
                case 'devices':
                    await this._pollDevices();
                    break;
                case 'acquisition':
                    await this._pollAcquisition();
                    break;
            }
        } catch (e) {
            // Silently ignore poll errors
        }
    }

    async _pollTransfers() {
        const t = await this.api('/api/retronas/transfer').catch(() => null);
        if (t) { this._lastTransferState = t; this._updateTransferUI(t); }
        const m = await this.api('/api/retronas/media-transfer').catch(() => null);
        if (m) { this._lastMediaState = m; this._updateMediaTransferUI(m); }
    }

    async _pollDashboard() {
        const stats = await this.api('/api/stats').catch(() => null);
        if (!stats) return;
        this._setText('total-files', stats.total_files?.toLocaleString() || '0');
        this._setText('total-size', this.formatSize((stats.total_size_mb || 0) * 1024 * 1024));
        this._setText('duplicates-count', stats.total_duplicates?.toLocaleString() || '0');
        this._setText('moved-count', stats.total_moved?.toLocaleString() || '0');
        this._setText('space-saved', this.formatSize((stats.space_saved_mb || 0) * 1024 * 1024));

        // Poll services every 5th tick (2.5s)
        this._dashPollCount = (this._dashPollCount || 0) + 1;
        if (this._dashPollCount % 5 === 1) {
            this._pollServices();
        }

        // Poll gaming activity every 30th tick (15s) — gamelists don't change fast
        if (this._dashPollCount % 30 === 1) {
            this.loadGamingActivity();
        }

        // Poll RA activity every 120th tick (60s) — external API, be gentle
        if (this._dashPollCount % 120 === 1) {
            this.loadRAActivity();
        }
    }

    async _pollServices() {
        // RetroAchievements
        const ra = await this.api('/api/ra/stats').catch(() => null);
        const raConfig = await this.api('/api/ra/config').catch(() => null);
        const raDot = document.getElementById('svc-ra-dot');
        if (ra && raConfig) {
            this._setText('svc-ra-status', raConfig.enabled ? 'Connected' : 'Disabled');
            this._setText('svc-ra-user', raConfig.username || '-');
            this._setText('svc-ra-verified', (ra.ra_supported + ra.ra_not_supported).toLocaleString());
            this._setText('svc-ra-supported', ra.ra_supported?.toLocaleString() || '0');
            this._setText('svc-ra-unverified', ra.ra_unverified?.toLocaleString() || '0');
            if (raDot) {
                raDot.className = 'service-dot ' + (raConfig.enabled && raConfig.has_api_key ? 'online' : 'offline');
            }
        } else if (raDot) {
            raDot.className = 'service-dot offline';
            this._setText('svc-ra-status', 'Unreachable');
        }

        // ScreenScraper
        const ss = await this.api('/api/ss/config').catch(() => null);
        const ssDot = document.getElementById('svc-ss-dot');
        if (ss && ss.enabled) {
            // Only test connection every 30s to avoid wasting requests
            this._ssPollCount = (this._ssPollCount || 0) + 1;
            if (this._ssPollCount % 12 === 1 || !this._ssLastTest) {
                this._ssLastTest = await this.api('/api/ss/test').catch(() => null);
            }
            const test = this._ssLastTest;
            if (test && test.success) {
                this._setText('svc-ss-status', `Tier ${test.level} — ${test.effective_rate_limit || '?'}s delay`);
                this._setText('svc-ss-user', test.username || '-');
                this._setText('svc-ss-level', test.level || '-');
                this._setText('svc-ss-threads', test.max_threads || test.threads || '-');
                this._setText('svc-ss-requests', `${test.requests_today}/${test.requests_max}`);
                const pct = test.requests_max > 0 ? (test.requests_today / test.requests_max * 100) : 0;
                this._setWidth('svc-ss-quota-fill', pct.toFixed(1));
                this._setText('svc-ss-quota-label', `${test.requests_today?.toLocaleString()} / ${test.requests_max?.toLocaleString()} requests`);
                if (ssDot) ssDot.className = 'service-dot online';
            } else {
                this._setText('svc-ss-status', test?.error || 'Auth failed');
                if (ssDot) ssDot.className = 'service-dot offline';
            }
        } else {
            this._setText('svc-ss-status', ss ? 'Disabled' : 'Not configured');
            if (ssDot) ssDot.className = 'service-dot ' + (ss ? 'offline' : 'offline');
        }

        // RetroNAS
        const nas = await this.api('/api/retronas/live').catch(() => null);
        const nasDot = document.getElementById('svc-nas-dot');
        if (nas && nas.total_files > 0) {
            this._setText('svc-nas-status', `Online — updated ${nas.last_updated}`);
            this._setText('svc-nas-ip', '10.99.11.8');
            this._setText('svc-nas-files', nas.total_files?.toLocaleString() || '0');
            this._setText('svc-nas-size', this.formatSize(nas.total_bytes || 0));
            this._setText('svc-nas-systems', nas.systems?.length?.toString() || '0');
            if (nasDot) nasDot.className = 'service-dot online';
        } else {
            this._setText('svc-nas-status', 'Offline');
            if (nasDot) nasDot.className = 'service-dot offline';
        }
    }

    async _pollRetroNAS() {
        // Transfer + media polling now handled by _pollTransfers (runs on every view)
        const t = this._lastTransferState || null;
        const m = this._lastMediaState || null;

        // Update ops status icons (transfers + media only, acquisition has its own page)
        this._updateOpsStatusIcons(t, m);

        // Auto-manage chaining logic
        this._handleAutoManage(t, m);

        // Poll live VM filesystem data (every 5th tick = 2.5s)
        this._retronasPollCount = (this._retronasPollCount || 0) + 1;
        if (this._retronasPollCount % 5 === 1) {
            const live = await this.api('/api/retronas/live').catch(() => null);
            if (live && live.systems && live.systems.length > 0) {
                this._setText('retronas-total-games', live.total_files?.toLocaleString() || '-');
                this._setText('retronas-total-size', live.total_bytes ? this.formatSize(live.total_bytes) : '-');
                this._setText('retronas-systems-count', live.systems.length.toLocaleString());

                // Update per-system cards without full rebuild (only if changed)
                const liveKey = live.systems.map(s => `${s.system}:${s.file_count}`).join(',');
                if (this._lastLiveSystemsKey !== liveKey) {
                    this._lastLiveSystemsKey = liveKey;
                    this._renderLiveSystems(live.systems);
                }
            }

            // RA stats less frequently (every 20th tick = 10s)
            if (this._retronasPollCount % 20 === 1) {
                const ra = await this.api('/api/ra/stats').catch(() => null);
                if (ra) this._setText('retronas-ra-verified', ra.ra_supported?.toLocaleString() || '0');
            }
        }
    }

    _updateTransferUI(t) {
        // Unified ops card updates
        const opsCard = document.getElementById('ops-rom-transfer');
        const startBtn = document.getElementById('ops-rom-start-btn');
        const cancelBtn = document.getElementById('ops-rom-cancel-btn');
        const body = document.getElementById('ops-rom-transfer-body');

        if (t.active || t.current_system === 'COMPLETE') {
            // Expand card body when active
            body?.classList.remove('collapsed');
            opsCard?.classList.add('active');
            startBtn?.classList.add('hidden');
            cancelBtn?.classList.remove('hidden');

            const pct = t.total_bytes > 0 ? (t.transferred_bytes / t.total_bytes * 100) : 0;
            this._setWidth('ops-rom-fill', pct.toFixed(1));
            this._setWidth('ops-rom-mini-fill', pct.toFixed(1));
            this._setText('ops-rom-pct', `${pct.toFixed(1)}%`);
            this._setText('ops-rom-file', t.current_file ? t.current_file.split('/').pop() : '-');
            this._setText('ops-rom-files', `${t.transferred_files} / ${t.total_files}`);
            const skipped = t.skipped_files || 0;
            this._setText('ops-rom-skipped', skipped > 0 ? `${skipped.toLocaleString()} already on NAS` : '-');
            this._setText('ops-rom-size', `${this.formatSize(t.transferred_bytes)} / ${this.formatSize(t.total_bytes)}`);
            this._setText('ops-rom-speed', t.speed_bps > 0 ? `${this.formatSize(t.speed_bps)}/s` : '- /s');

            if (t.eta_seconds > 0 && t.eta_seconds < 999999) {
                const h = Math.floor(t.eta_seconds / 3600);
                const m = Math.floor((t.eta_seconds % 3600) / 60);
                this._setText('ops-rom-eta', h > 0 ? `${h}h ${m}m` : `${m}m`);
            } else {
                this._setText('ops-rom-eta', t.current_system === 'COMPLETE' ? 'Done!' : 'calculating...');
            }

            // Update system badges only when they change
            const doneKey = (t.systems_done || []).join(',');
            if (this._lastDoneKey !== doneKey) {
                this._lastDoneKey = doneKey;
                const doneEl = document.getElementById('ops-rom-systems-done');
                const remEl = document.getElementById('ops-rom-systems-remaining');
                if (doneEl) doneEl.innerHTML = (t.systems_done || []).map(s => `<span>${s}</span>`).join('');
                if (remEl) remEl.innerHTML = (t.systems_remaining || []).map(s => `<span>${s}</span>`).join('');
            }

            if (!t.active && t.current_system === 'COMPLETE') {
                cancelBtn?.classList.add('hidden');
                startBtn?.classList.remove('hidden');
                opsCard?.classList.remove('active');
                opsCard?.classList.add('completed');
            }

            // --- Gamification updates ---
            this._updateTransferGamification(t, pct);

        } else {
            opsCard?.classList.remove('active');
            startBtn?.classList.remove('hidden');
            cancelBtn?.classList.add('hidden');
            // Reset combo when no transfer (keep XP/level/log)
            this._transferCombo = 0;
            this._transferLastFile = null;
            this._transferLastFileCount = 0;
            this._saveTransferState();
        }
    }

    // Gamified transfer visualization updates
    _updateTransferGamification(t, pct) {
        // --- Progress ring ---
        const ringFill = document.getElementById('transfer-ring-fill');
        const ringText = document.getElementById('transfer-ring-text');
        if (ringFill) {
            const circumference = 2 * Math.PI * 24; // r=24
            const offset = circumference - (pct / 100) * circumference;
            ringFill.setAttribute('stroke-dasharray', circumference.toFixed(1));
            ringFill.setAttribute('stroke-dashoffset', Math.max(0, offset).toFixed(1));
        }
        if (ringText) ringText.textContent = `${pct.toFixed(0)}%`;

        // --- XP counter (1 XP per MB transferred) ---
        const xpMB = Math.floor(t.transferred_bytes / (1024 * 1024));
        this._transferXP = xpMB;

        // Level thresholds in MB: 1GB, 5GB, 10GB, 50GB, 100GB, 500GB, 1TB
        const levels = [0, 1024, 5120, 10240, 51200, 102400, 512000, 1048576];
        let newLevel = 1;
        for (let i = levels.length - 1; i >= 0; i--) {
            if (xpMB >= levels[i]) { newLevel = i + 1; break; }
        }

        const xpEl = document.getElementById('transfer-xp');
        this._setText('transfer-xp-value', xpMB.toLocaleString() + ' XP');
        this._setText('transfer-xp-level', `LVL ${newLevel}`);

        if (newLevel > this._transferLevel && this._transferLevel > 0) {
            // Level up animation
            xpEl?.classList.add('level-up');
            setTimeout(() => xpEl?.classList.remove('level-up'), 1500);
        }
        this._transferLevel = newLevel;

        // --- Speed indicator ---
        const speedBps = t.speed_bps || 0;
        const speedBar = document.getElementById('transfer-speed-bar');
        const speedVal = document.getElementById('transfer-speed-value');

        if (speedBar) {
            speedBar.className = 'speed-bar-fill';
            if (speedBps < 1024 * 1024) {
                speedBar.classList.add('slow');
            } else if (speedBps < 10 * 1024 * 1024) {
                speedBar.classList.add('medium');
            } else if (speedBps < 50 * 1024 * 1024) {
                speedBar.classList.add('fast');
            } else {
                speedBar.classList.add('blazing');
            }
        }
        if (speedVal) {
            speedVal.textContent = speedBps > 0 ? `${this.formatSize(speedBps)}/s` : '- /s';
        }

        // --- Combo counter ---
        // Detect new file transfer completion
        if (t.current_file && t.current_file !== this._transferLastFile) {
            if (this._transferLastFile !== null && t.transferred_files > this._transferLastFileCount) {
                // A file completed successfully
                this._transferCombo++;
                this._addToTransferLog(this._transferLastFile, t.current_system, true);
            }
            this._transferLastFile = t.current_file;
            this._transferLastFileCount = t.transferred_files;

            // Spawn file fly animation
            this._spawnFileFly(t.current_file);
        }

        // Handle transfer errors resetting combo
        if (t.errors && t.errors > (this._transferLastErrors || 0)) {
            if (this._transferCombo > 0) {
                this._addToTransferLog(t.current_file || 'unknown', t.current_system, false);
            }
            this._transferCombo = 0;
            this._transferLastErrors = t.errors;
        }

        const comboEl = document.getElementById('transfer-combo');
        const comboCount = document.getElementById('transfer-combo-count');
        if (comboCount) comboCount.textContent = this._transferCombo;
        if (comboEl) {
            comboEl.className = 'transfer-combo';
            if (this._transferCombo >= 50) {
                comboEl.classList.add('fire');
            } else if (this._transferCombo >= 20) {
                comboEl.classList.add('hot');
            } else if (this._transferCombo > 0) {
                comboEl.classList.add('active');
            }
        }

        // --- System completion badges ---
        const doneSystems = t.systems_done || [];
        doneSystems.forEach(sys => {
            if (!this._transferCompletedSystems.has(sys)) {
                this._transferCompletedSystems.add(sys);
                this._flashSystemBadge(sys);
            }
        });

        // --- Transfer log rendering ---
        this._renderTransferLog();

        // Persist gamification state
        this._saveTransferState();
    }

    // Spawn a file-fly SVG icon across the transfer lane
    _spawnFileFly(filename) {
        const lane = document.getElementById('transfer-lane');
        if (!lane) return;

        const ext = (filename || '').split('.').pop().toLowerCase();

        // Pick SVG icon based on file extension
        let iconSvg;
        const discExts = ['chd', 'iso', 'bin', 'cue', 'img', 'mdf', 'nrg', 'cdi'];
        const cartExts = ['nes', 'sfc', 'smc', 'gba', 'gbc', 'gb', 'n64', 'z64', 'v64', 'nds', 'md', 'gen', 'sms', 'gg', 'pce', 'a26', 'a78', 'col', 'int', 'vec'];

        if (discExts.includes(ext)) {
            // Disc icon
            iconSvg = `<svg viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
                <circle cx="10" cy="10" r="9" stroke="${getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()}" stroke-width="1.5" fill="none"/>
                <circle cx="10" cy="10" r="3" stroke="${getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()}" stroke-width="1" fill="none"/>
                <circle cx="10" cy="10" r="1" fill="${getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()}"/>
            </svg>`;
        } else if (cartExts.includes(ext)) {
            // Cartridge icon
            iconSvg = `<svg viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
                <rect x="3" y="2" width="14" height="16" rx="2" stroke="${getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()}" stroke-width="1.5" fill="none"/>
                <rect x="6" y="5" width="8" height="5" rx="1" fill="${getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()}" opacity="0.3"/>
                <rect x="7" y="14" width="2" height="4" fill="${getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()}" opacity="0.5"/>
                <rect x="11" y="14" width="2" height="4" fill="${getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()}" opacity="0.5"/>
            </svg>`;
        } else {
            // Generic file icon
            iconSvg = `<svg viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M4 2h8l4 4v12a1 1 0 01-1 1H5a1 1 0 01-1-1V3a1 1 0 011-1z" stroke="${getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()}" stroke-width="1.5" fill="none"/>
                <path d="M12 2v4h4" stroke="${getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()}" stroke-width="1.5" fill="none"/>
            </svg>`;
        }

        const iconEl = document.createElement('div');
        iconEl.className = 'transfer-file-icon';
        iconEl.innerHTML = iconSvg;
        lane.appendChild(iconEl);

        // Remove after animation completes
        setTimeout(() => iconEl.remove(), 2000);
    }

    // Add an entry to the transfer log (keep last 10)
    _addToTransferLog(filename, system, success) {
        this._transferLog.unshift({
            file: (filename || '').split('/').pop(),
            system: system || '-',
            ok: success,
            time: Date.now()
        });
        if (this._transferLog.length > 10) this._transferLog.pop();
    }

    // Render the transfer log list
    _renderTransferLog() {
        const logEl = document.getElementById('transfer-log');
        if (!logEl || this._transferLog.length === 0) return;

        // Only re-render if changed
        const logKey = this._transferLog.map(e => e.file + e.ok).join('|');
        if (this._lastLogKey === logKey) return;
        this._lastLogKey = logKey;

        logEl.innerHTML = `<div class="transfer-log-header">Recent Transfers</div>` +
            this._transferLog.map(entry => `
                <div class="transfer-log-entry">
                    <span class="log-status ${entry.ok ? 'ok' : 'err'}">${entry.ok ? '\u2713' : '\u2717'}</span>
                    <span class="log-system">${entry.system}</span>
                    <span class="log-file" title="${entry.file}">${entry.file}</span>
                </div>
            `).join('');
    }

    // Flash a completion badge when a system finishes
    _flashSystemBadge(systemName) {
        const container = document.getElementById('transfer-badges');
        if (!container) return;

        const badge = document.createElement('span');
        badge.className = 'transfer-completion-badge';
        badge.textContent = `${systemName} Complete`;
        container.appendChild(badge);

        // Also fire a toast
        this.showToast('System Complete', `${systemName} transfer finished`, 'success');
    }

    async _pollScan() {
        if (!this.currentScanId) return;
        const p = await this.api(`/api/scan/${this.currentScanId}/status`).catch(() => null);
        if (p) {
            this._setText('scan-progress-text', p.current_file || '');
            if (p.total_files > 0) {
                const pct = (p.processed_files / p.total_files * 100).toFixed(1);
                this._setWidth('scan-progress-fill', pct);
                this._setText('scan-progress-count', `${p.processed_files} / ${p.total_files}`);
            }
        }
    }

    async _pollQueue() {
        // Handled by existing queue polling
    }

    async restoreActiveScan() {
        const savedScanId = localStorage.getItem('duper_active_scan');
        const savedScanDir = localStorage.getItem('duper_scan_directory');

        if (!savedScanId) return;

        try {
            // Check if scan is still active on the server
            const progress = await this.api(`/api/scan/${savedScanId}/status`);

            if (progress.status === 'scanning' || progress.status === 'hashing') {
                // Scan is still active - restore UI state
                this.currentScanId = savedScanId;

                // Switch to scan view
                this.switchView('scan');

                // Restore directory input
                if (savedScanDir) {
                    document.getElementById('scan-directory').value = savedScanDir;
                }

                // Show progress panel
                const progressPanel = document.getElementById('scan-progress');
                const progressFill = document.getElementById('progress-fill');
                const btn = document.getElementById('start-scan');

                progressPanel.classList.remove('hidden');
                progressPanel.classList.add('scanning');
                progressFill.classList.add('active');
                btn.disabled = true;
                btn.classList.add('scanning');
                btn.textContent = 'Scanning...';

                // Update progress immediately
                this.updateProgressUI(progress);

                // Resume polling
                this.pollScanProgress();

                this.showToast('Scan Restored', 'Resumed monitoring active scan', 'info');
            } else if (progress.status === 'completed') {
                // Scan completed while we were away - show results
                const result = await this.api(`/api/scan/${savedScanId}/result`);
                this.clearScanState();

                // Switch to scan view and show results
                this.switchView('scan');
                if (savedScanDir) {
                    document.getElementById('scan-directory').value = savedScanDir;
                }

                const progressPanel = document.getElementById('scan-progress');
                progressPanel.classList.remove('hidden');
                this.showScanResult(result);
            } else {
                // Scan errored or unknown state - clear saved state
                this.clearScanState();
            }
        } catch (e) {
            // Scan doesn't exist anymore - clear saved state
            console.log('Could not restore scan:', e.message);
            this.clearScanState();
        }
    }

    saveScanState(scanId, directory) {
        localStorage.setItem('duper_active_scan', scanId);
        localStorage.setItem('duper_scan_directory', directory);
    }

    clearScanState() {
        localStorage.removeItem('duper_active_scan');
        localStorage.removeItem('duper_scan_directory');
    }

    bindEvents() {
        // Navigation
        document.querySelectorAll('.nav-btn').forEach(btn => {
            btn.addEventListener('click', () => this.switchView(btn.dataset.view));
        });

        // API Key Modal
        document.getElementById('api-key-submit').addEventListener('click', () => this.submitApiKey());
        document.getElementById('api-key-input').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.submitApiKey();
        });

        // Scan
        document.getElementById('start-scan').addEventListener('click', () => this.startScan());

        // Duplicates
        document.getElementById('load-duplicates').addEventListener('click', () => this.loadDuplicates());

        // Duplicates view toggle
        document.querySelectorAll('.view-toggle .view-btn').forEach(btn => {
            btn.addEventListener('click', () => this.switchDuplicatesViewMode(btn.dataset.viewMode));
        });

        // Duplicates action panel
        document.getElementById('process-selected').addEventListener('click', () => this.processSelectedDuplicates());
        document.getElementById('select-all-duplicates').addEventListener('click', () => this.selectAllDuplicates());
        document.getElementById('deselect-all-duplicates').addEventListener('click', () => this.deselectAllDuplicates());
        document.querySelectorAll('input[name="duplicate-action"]').forEach(radio => {
            radio.addEventListener('change', () => this.updateActionUI());
        });

        // Moved Files
        document.getElementById('load-moved').addEventListener('click', () => this.loadMovedFiles());
        document.getElementById('restore-all').addEventListener('click', () => this.restoreAll());

        // Config
        document.getElementById('save-config').addEventListener('click', () => this.saveConfig());
        document.getElementById('test-ra-connection').addEventListener('click', () => this.testRAConnection());
        document.getElementById('test-ss-connection').addEventListener('click', () => this.testSSConnection());

        // Media
        document.querySelectorAll('.media-tab').forEach(tab => {
            tab.addEventListener('click', () => this.switchMediaTab(tab.dataset.mediaTab));
        });
        document.getElementById('find-orphaned-media').addEventListener('click', () => this.findOrphanedMedia());
        document.getElementById('find-moved-rom-media').addEventListener('click', () => this.findMovedRomMedia());
        document.getElementById('cleanup-media').addEventListener('click', () => this.cleanupMedia());

        // Saves
        document.querySelectorAll('.saves-tab').forEach(tab => {
            tab.addEventListener('click', () => this.switchSavesTab(tab.dataset.savesTab));
        });
        document.getElementById('find-orphaned-saves').addEventListener('click', () => this.findOrphanedSaves());
        document.getElementById('find-moved-rom-saves').addEventListener('click', () => this.findMovedRomSaves());
        document.getElementById('preserve-saves').addEventListener('click', () => this.preserveSaves());

        // System Detection
        document.getElementById('detect-systems').addEventListener('click', () => this.detectSystems());
        document.getElementById('select-all-systems').addEventListener('click', () => this.selectAllSystems());
        document.getElementById('deselect-all-systems').addEventListener('click', () => this.deselectAllSystems());

        // Games view toggle
        document.querySelectorAll('[data-games-view]').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('[data-games-view]').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this.gamesViewMode = btn.dataset.gamesView;
                if (this.games.length > 0) this.renderGames(this.games);
            });
        });
    }

    _saveNavState(key, value) {
        const state = JSON.parse(localStorage.getItem('duper_nav_state') || '{}');
        state[key] = value;
        localStorage.setItem('duper_nav_state', JSON.stringify(state));
    }

    _getNavState(key, fallback) {
        const state = JSON.parse(localStorage.getItem('duper_nav_state') || '{}');
        return state[key] !== undefined ? state[key] : fallback;
    }

    switchView(view) {
        // Save scroll position of current view before switching
        if (this._currentView) {
            this._saveNavState('scroll_' + this._currentView, window.scrollY);
        }
        this._currentView = view;
        localStorage.setItem('duper_current_view', view);

        document.querySelectorAll('.nav-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.view === view);
        });
        document.querySelectorAll('.view').forEach(v => {
            v.classList.toggle('active', v.id === `${view}-view`);
        });

        // Load data for the view
        switch (view) {
            case 'dashboard':
                this.loadDashboard();
                break;
            case 'games':
                this.loadGames();
                break;
            case 'scan':
                this.loadScanSuggestions();
                break;
            case 'queue':
                this.loadQueue();
                this.startQueuePolling();
                break;
            case 'moved':
                this.loadMovedFiles();
                break;
            case 'retronas':
                this.loadRetroNAS();
                break;
            case 'acquisition':
                this.loadAcquisitionPage();
                break;
            case 'devices':
                this._pollDevices();
                break;
            case 'config':
                this.loadConfig();
                break;
        }

        // Restore scroll position after a brief delay for rendering
        setTimeout(() => {
            const savedScroll = this._getNavState('scroll_' + view, 0);
            if (savedScroll > 0) window.scrollTo(0, savedScroll);
        }, 100);

        // Stop queue polling when leaving queue view
        if (view !== 'queue') {
            this.stopQueuePolling();
        }
    }

    async api(endpoint, options = {}) {
        const url = `${this.baseUrl}${endpoint}`;
        const headers = {
            'Content-Type': 'application/json',
            ...options.headers
        };

        if (this.apiKey) {
            headers['X-API-Key'] = this.apiKey;
        }

        const response = await fetch(url, {
            ...options,
            headers
        });

        if (response.status === 401) {
            this.showApiKeyModal();
            throw new Error('Authentication required');
        }

        if (!response.ok) {
            const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
            throw new Error(error.detail || `HTTP ${response.status}`);
        }

        return response.json();
    }

    showApiKeyModal() {
        document.getElementById('api-key-modal').classList.remove('hidden');
    }

    hideApiKeyModal() {
        document.getElementById('api-key-modal').classList.add('hidden');
    }

    async submitApiKey() {
        const input = document.getElementById('api-key-input');
        this.apiKey = input.value.trim();
        localStorage.setItem('duper_api_key', this.apiKey);
        this.hideApiKeyModal();
        await this.checkConnection();
    }

    async checkConnection() {
        const status = document.getElementById('connection-status');
        try {
            const data = await this.api('/api/health');
            status.textContent = `Connected - v${data.version} "${data.codename}"`;
            status.className = 'status connected';

            // Update footer - Falcon Technix standard pattern
            const footerVersion = document.getElementById('footer-version');
            if (footerVersion) footerVersion.textContent = `v${data.version}`;

            const footerCodename = document.getElementById('footer-codename');
            if (footerCodename) footerCodename.textContent = data.codename;

            const footerStatus = document.getElementById('footer-status');
            if (footerStatus) {
                footerStatus.textContent = 'Online';
                footerStatus.className = 'footer-status online';
            }

            // Store version info for later use
            this.appVersion = data.version;
            this.appCodename = data.codename;

            return true;
        } catch (e) {
            if (e.message !== 'Authentication required') {
                status.textContent = 'Disconnected';
                status.className = 'status disconnected';
                const footerStatus = document.getElementById('footer-status');
                if (footerStatus) {
                    footerStatus.textContent = 'Offline';
                    footerStatus.className = 'footer-status offline';
                }
            }
            return false;
        }
    }

    async loadDashboard() {
        try {
            const stats = await this.api('/api/stats?include_system=true');

            document.getElementById('stat-files').textContent = stats.total_files.toLocaleString();
            document.getElementById('stat-duplicates').textContent = stats.total_duplicates.toLocaleString();
            document.getElementById('stat-exact-duplicates').textContent = (stats.exact_duplicates || 0).toLocaleString();
            document.getElementById('stat-moved').textContent = stats.total_moved.toLocaleString();
            document.getElementById('stat-size').textContent = this.formatSize(stats.total_size_mb * 1024 * 1024);

            // Update space analysis visualization
            this.updateSpaceAnalysis(stats);

            // Update size breakdown
            this.renderSizeBreakdown(stats.size_breakdown || []);

            if (stats.system_info) {
                document.getElementById('system-info').textContent = JSON.stringify(stats.system_info, null, 2);
            }

            // Load RA stats
            this.loadRAStats();

            // Load last scan summary for Quick Actions
            this.updateLastScanSummary();

            // Load gaming activity and collection metrics
            this.loadGamingActivity();
            this.loadRAActivity();
        } catch (e) {
            console.error('Failed to load dashboard:', e);
        }
    }

    async updateLastScanSummary() {
        const summaryEl = document.getElementById('last-scan-summary');
        const timeEl = document.getElementById('last-scan-time');
        const dirEl = document.getElementById('last-scan-dir');
        const dupesEl = document.getElementById('last-scan-dupes');
        const crossEl = document.getElementById('last-scan-cross');
        const filesEl = document.getElementById('last-scan-files');

        if (!summaryEl) return;

        try {
            const suggestions = await this.api('/api/scan/suggestions');

            if (suggestions.history && suggestions.history.length > 0) {
                const lastScan = suggestions.history[0];
                this.lastScannedDirectory = lastScan.path;

                // Update UI elements
                if (timeEl) {
                    timeEl.textContent = this.formatDate(lastScan.last_scanned) || 'Recently';
                }
                if (dirEl) {
                    dirEl.textContent = lastScan.path.split('/').pop() || lastScan.path;
                    dirEl.title = lastScan.path;
                }
                if (filesEl) {
                    filesEl.textContent = lastScan.file_count?.toLocaleString() || '0';
                }
                if (dupesEl) {
                    dupesEl.textContent = lastScan.duplicate_count?.toLocaleString() || '0';
                    // Add highlight if duplicates found
                    const statEl = dupesEl.closest('.scan-stat');
                    if (statEl) {
                        statEl.classList.toggle('has-dupes', lastScan.duplicate_count > 0);
                    }
                }
                if (crossEl) {
                    crossEl.textContent = lastScan.cross_platform_count?.toLocaleString() || '0';
                }

                summaryEl.classList.remove('no-scans');
                summaryEl.classList.add('has-scan');
            } else {
                summaryEl.classList.add('no-scans');
                summaryEl.classList.remove('has-scan');
                if (timeEl) timeEl.textContent = 'No scans yet';
                if (dirEl) dirEl.textContent = '-';
                if (dupesEl) dupesEl.textContent = '0';
                if (crossEl) crossEl.textContent = '0';
                if (filesEl) filesEl.textContent = '0';
            }
        } catch (e) {
            console.error('Failed to load last scan summary:', e);
            summaryEl.classList.add('no-scans');
        }
    }

    async goToDuplicates() {
        // Switch to duplicates view
        this.switchView('duplicates');

        // Try to get the last scanned directory if we don't have it
        if (!this.lastScannedDirectory) {
            try {
                const suggestions = await this.api('/api/scan/suggestions');
                if (suggestions.history && suggestions.history.length > 0) {
                    this.lastScannedDirectory = suggestions.history[0].path;
                }
            } catch (e) {
                console.error('Failed to get scan suggestions:', e);
            }
        }

        // Pre-fill the directory if we have one
        if (this.lastScannedDirectory) {
            document.getElementById('duplicates-directory').value = this.lastScannedDirectory;
            // Auto-load duplicates
            this.loadDuplicates();
        } else {
            this.showToast('No Scans Yet', 'Scan a directory first to find duplicates', 'info');
        }
    }

    rescanLast() {
        if (!this.lastScannedDirectory) {
            alert('No previous scan directory found. Please start a new scan.');
            this.switchView('scan');
            return;
        }

        // Switch to scan view and pre-fill
        this.switchView('scan');
        document.getElementById('scan-directory').value = this.lastScannedDirectory;

        // Auto-start the scan after a brief delay for UI update
        setTimeout(() => {
            this.startScan();
        }, 100);
    }

    renderSizeBreakdown(breakdown) {
        const container = document.getElementById('size-breakdown');
        if (!container) return;

        if (!breakdown || breakdown.length === 0) {
            container.innerHTML = '<div class="empty">No data yet. Scan a directory to see breakdown.</div>';
            return;
        }

        // Get max size for percentage calculation
        const maxSize = Math.max(...breakdown.map(b => b.total_size_mb));

        container.innerHTML = breakdown.slice(0, 10).map((item, index) => {
            const percent = maxSize > 0 ? (item.total_size_mb / maxSize) * 100 : 0;
            const sizeFormatted = this.formatSize(item.total_size_mb * 1024 * 1024);

            return `
                <div class="breakdown-item" style="animation-delay: ${index * 0.05}s">
                    <span class="breakdown-ext">.${item.extension}</span>
                    <div class="breakdown-bar-container">
                        <div class="breakdown-bar" style="width: ${percent}%"></div>
                    </div>
                    <div class="breakdown-stats">
                        <span class="size">${sizeFormatted}</span>
                        <span class="count">${item.file_count.toLocaleString()} files</span>
                    </div>
                </div>
            `;
        }).join('');
    }

    async loadRAStats() {
        try {
            const raStats = await this.api('/api/ra/stats');

            document.getElementById('ra-supported-count').textContent = raStats.ra_supported.toLocaleString();
            document.getElementById('ra-not-supported-count').textContent = raStats.ra_not_supported.toLocaleString();
            document.getElementById('ra-unverified-count').textContent = raStats.ra_unverified.toLocaleString();

            // Update status badge
            const statusBadge = document.getElementById('ra-enabled-status');
            if (raStats.ra_enabled) {
                statusBadge.textContent = 'RA Enabled';
                statusBadge.classList.remove('disabled');
                statusBadge.classList.add('enabled');
            } else {
                statusBadge.textContent = 'RA Disabled';
                statusBadge.classList.remove('enabled');
                statusBadge.classList.add('disabled');
            }

            // Update progress bar
            const total = raStats.ra_supported + raStats.ra_not_supported + raStats.ra_unverified;
            if (total > 0) {
                const supportedPercent = (raStats.ra_supported / total) * 100;
                const notSupportedPercent = (raStats.ra_not_supported / total) * 100;

                document.getElementById('ra-bar-supported').style.width = `${supportedPercent}%`;
                document.getElementById('ra-bar-not-supported').style.width = `${notSupportedPercent}%`;
            }
        } catch (e) {
            console.error('Failed to load RA stats:', e);
        }
    }

    // --- Gaming Activity Dashboard ---

    async loadGamingActivity() {
        try {
            const data = await this.api('/api/dashboard/gaming');

            // Update last played
            if (data.last_played) {
                const lp = data.last_played;
                this._setText('lp-game-title', lp.name);
                this._setText('lp-system', lp.system);
                this._setText('lp-playcount', lp.playcount);
                this._setText('lp-developer', lp.developer || '--');
                this._setText('lp-genre', lp.genre || '');

                // Publisher
                const pubWrap = document.getElementById('lp-publisher-wrap');
                const pubEl = document.getElementById('lp-publisher');
                if (pubWrap && pubEl) {
                    if (lp.publisher) {
                        pubEl.textContent = lp.publisher;
                        pubWrap.style.display = '';
                    } else {
                        pubWrap.style.display = 'none';
                    }
                }

                // Rating display
                if (lp.rating && lp.rating > 0) {
                    const stars = Math.round(lp.rating * 5);
                    let starStr = '';
                    for (let i = 0; i < 5; i++) {
                        starStr += i < stars ? '\u2605' : '\u2606';
                    }
                    this._setText('lp-rating-val', starStr);
                } else {
                    this._setText('lp-rating-val', '--');
                }

                // Format playtime
                const mins = lp.playtime_minutes || 0;
                if (mins >= 60) {
                    const h = Math.floor(mins / 60);
                    const m = mins % 60;
                    this._setText('lp-playtime', `${h}h ${m}m`);
                } else if (mins > 0) {
                    this._setText('lp-playtime', `${mins}m`);
                } else {
                    this._setText('lp-playtime', '--');
                }

                // NOW PLAYING vs LAST PLAYED - 15 minute threshold
                const card = document.getElementById('last-played-card');
                const statusLabel = document.getElementById('lp-status-label');
                const liveDot = document.getElementById('lp-live-dot');
                let isNowPlaying = false;
                if (lp.lastplayed) {
                    try {
                        const lpDate = new Date(lp.lastplayed);
                        const diffMs = Date.now() - lpDate.getTime();
                        isNowPlaying = diffMs < 15 * 60 * 1000; // 15 minutes
                    } catch {}
                }

                if (statusLabel) {
                    statusLabel.textContent = isNowPlaying ? 'Now Playing' : 'Last Played';
                }
                if (card) {
                    card.classList.add('has-data');
                    card.classList.toggle('now-playing', isNowPlaying);
                }
                if (liveDot) {
                    liveDot.style.display = isNowPlaying ? '' : 'none';
                }

                // Format relative time
                if (lp.lastplayed) {
                    this._setText('lp-time', this.formatDate(lp.lastplayed));
                }

                // Load cover art for the now-playing card
                this._loadNowPlayingCover(lp.filepath, lp.path || lp.name, lp.system);

                // Load RA game progress for the currently played game
                this._loadNowPlayingRA(lp.system, lp.path || lp.name);

                // Live feed: auto-start capture when playing, show feed
                this._updateLiveFeed(isNowPlaying);
            }

            // Update recently played list with thumbnails
            const listEl = document.getElementById('lp-recently-list');
            if (listEl && data.recently_played && data.recently_played.length > 1) {
                // Skip first since it's the "last played" already shown above
                const recent = data.recently_played.slice(1, 6);
                listEl.innerHTML = '<div class="lp-recently-title">Recent Sessions</div>' +
                    recent.map(g => {
                        const mins = g.playtime_minutes || 0;
                        let pt = '';
                        if (mins >= 60) { pt = Math.floor(mins / 60) + 'h ' + (mins % 60) + 'm'; }
                        else if (mins > 0) { pt = mins + 'm'; }
                        const thumbUrl = this._gameImageUrl(g.system, g.path || g.name);
                        return `<div class="lp-recently-item">
                            <img class="lp-recently-thumb" src="${thumbUrl}" alt="" loading="lazy" onerror="this.style.display='none'">
                            <span class="lp-recently-name">${this._escHtml(g.name)}</span>
                            <span class="lp-recently-sys">${this._escHtml(g.system)}</span>
                            <span class="lp-recently-plays">${g.playcount}x</span>
                            ${pt ? '<span class="lp-recently-time">' + pt + '</span>' : ''}
                        </div>`;
                    }).join('');
            }

            // Update collection metrics
            if (data.collection) {
                const c = data.collection;
                this._setText('coll-total-systems', c.total_systems.toLocaleString());
                this._setText('coll-total-games', c.total_games.toLocaleString());
                this._setText('coll-total-played', c.total_played.toLocaleString());
                this._setText('coll-completion', c.completion_pct + '%');

                // Format total playtime
                const totalMins = c.total_playtime_minutes || 0;
                if (totalMins >= 60) {
                    const h = Math.floor(totalMins / 60);
                    this._setText('coll-playtime', h + 'h');
                } else {
                    this._setText('coll-playtime', totalMins + 'm');
                }
            }

            // Update hero stats and gamification with gaming data
            this._lastGamingData = data;
            this._updateHeroStats(data, this._lastRAData, null);
            this._updateGamerProfileUI(data, this._lastRAData);

            // Update per-system grid
            const sysGrid = document.getElementById('collection-systems-grid');
            if (sysGrid && data.systems && data.systems.length > 0) {
                sysGrid.innerHTML = data.systems.map(s => {
                    const pct = s.completion_pct || 0;
                    const barColor = pct >= 50 ? 'var(--success)' : pct >= 20 ? 'var(--warning)' : 'var(--text-muted)';
                    const ptMins = s.playtime_minutes || 0;
                    let ptStr = '';
                    if (ptMins >= 60) { ptStr = Math.floor(ptMins / 60) + 'h'; }
                    else if (ptMins > 0) { ptStr = ptMins + 'm'; }

                    // Mini progress ring
                    const circumference = 2 * Math.PI * 14;
                    const ringOffset = circumference - (pct / 100) * circumference;
                    const ringColor = pct >= 50 ? 'var(--success)' : pct >= 20 ? 'var(--warning)' : 'var(--text-muted)';

                    return `<div class="coll-sys-card">
                        <div class="coll-sys-header">
                            <div style="display:flex;align-items:center;gap:8px;">
                                <svg class="coll-sys-ring" viewBox="0 0 36 36" width="32" height="32">
                                    <circle cx="18" cy="18" r="14" fill="none" stroke="var(--border)" stroke-width="3"/>
                                    <circle cx="18" cy="18" r="14" fill="none" stroke="${ringColor}" stroke-width="3" stroke-linecap="round"
                                        stroke-dasharray="${circumference.toFixed(1)}" stroke-dashoffset="${ringOffset.toFixed(1)}"
                                        transform="rotate(-90 18 18)" style="transition:stroke-dashoffset 0.8s ease;filter:drop-shadow(0 0 2px ${ringColor})"/>
                                    <text x="18" y="20" text-anchor="middle" fill="var(--text-primary)" font-size="8" font-weight="800" font-family="var(--font-mono)">${pct}%</text>
                                </svg>
                                <span class="coll-sys-name">${this._escHtml(s.system)}</span>
                            </div>
                            <span class="coll-sys-count">${s.total_games}</span>
                        </div>
                        <div class="coll-sys-bar-wrap">
                            <div class="coll-sys-bar" style="width:${pct}%;background:${barColor}"></div>
                        </div>
                        <div class="coll-sys-footer">
                            <span>${s.played}/${s.total_games} played</span>
                            ${ptStr ? '<span>' + ptStr + '</span>' : ''}
                        </div>
                    </div>`;
                }).join('');
            }
        } catch (e) {
            console.error('Failed to load gaming activity:', e);
        }
    }

    async loadRAActivity() {
        try {
            const data = await this.api('/api/dashboard/ra-activity');

            if (!data.enabled) {
                const card = document.getElementById('ra-activity-card');
                if (card) card.classList.add('ra-disabled');
                return;
            }

            this._setText('ra-act-user', data.username || '--');
            this._setText('ra-act-recent', data.total_recent.toString());

            if (data.summary) {
                this._setText('ra-act-points', (data.summary.total_points || 0).toLocaleString());
                this._setText('ra-act-truepoints', (data.summary.total_true_points || 0).toLocaleString());
                const rankEl = document.getElementById('ra-act-rank');
                if (rankEl && data.summary.rank) {
                    rankEl.textContent = '#' + data.summary.rank.toLocaleString();
                    rankEl.title = 'Global Rank';
                }
            }

            // Render recent achievements
            const achEl = document.getElementById('ra-act-achievements');
            if (achEl) {
                if (data.recent_achievements && data.recent_achievements.length > 0) {
                    const items = data.recent_achievements.slice(0, 6);
                    achEl.innerHTML = items.map(a => {
                        const dateStr = a.date ? this._formatRADate(a.date) : '';
                        const typeClass = a.type === 'missable' ? ' missable' : '';
                        return `<div class="ra-ach-item${typeClass}">
                            ${a.badge_url ? '<img class="ra-ach-badge" src="' + this._escHtml(a.badge_url) + '" alt="" loading="lazy">' : '<div class="ra-ach-badge-placeholder"></div>'}
                            <div class="ra-ach-info">
                                <div class="ra-ach-title">${this._escHtml(a.title)}</div>
                                <div class="ra-ach-game">${this._escHtml(a.game_title)} <span class="ra-ach-console">${this._escHtml(a.console_name)}</span></div>
                            </div>
                            <div class="ra-ach-points">+${a.points}</div>
                            ${dateStr ? '<div class="ra-ach-date">' + dateStr + '</div>' : ''}
                        </div>`;
                    }).join('');
                } else {
                    achEl.innerHTML = '<div class="ra-act-empty">No achievements in the last 24 hours</div>';
                }
            }

            const card = document.getElementById('ra-activity-card');
            if (card) {
                card.classList.remove('ra-disabled');
                if (data.total_recent > 0) card.classList.add('has-activity');
            }

            // Update gamification with RA data
            this._lastRAData = data;
            this._updateHeroStats(this._lastGamingData, data, null);
            this._updateGamerProfileUI(this._lastGamingData, data);
            this._checkNewAchievements(data);
        } catch (e) {
            console.error('Failed to load RA activity:', e);
        }
    }

    _formatRADate(dateStr) {
        try {
            const d = new Date(dateStr);
            const now = new Date();
            const diffMs = now - d;
            const diffMins = Math.floor(diffMs / 60000);
            if (diffMins < 1) return 'Just now';
            if (diffMins < 60) return diffMins + 'm ago';
            const diffHours = Math.floor(diffMins / 60);
            if (diffHours < 24) return diffHours + 'h ago';
            return Math.floor(diffHours / 24) + 'd ago';
        } catch { return ''; }
    }

    _escHtml(str) {
        if (!str) return '';
        return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    updateSpaceAnalysis(stats) {
        const totalMb = stats.total_size_mb || 0;
        const wastedMb = stats.wasted_space_mb || 0;
        const savedMb = stats.space_saved_mb || 0;
        const duplicateGroups = stats.duplicate_groups || 0;

        // Convert to bytes for formatting
        const totalBytes = totalMb * 1024 * 1024;
        const wastedBytes = wastedMb * 1024 * 1024;
        const savedBytes = savedMb * 1024 * 1024;

        // Update text values
        document.getElementById('space-total-value').textContent = this.formatSize(totalBytes);
        document.getElementById('space-wasted-value').textContent = this.formatSize(wastedBytes);
        document.getElementById('space-saved-value').textContent = this.formatSize(savedBytes);
        document.getElementById('space-recoverable-value').textContent = this.formatSize(wastedBytes);
        document.getElementById('space-already-saved-value').textContent = this.formatSize(savedBytes);
        document.getElementById('space-groups-value').textContent = duplicateGroups.toLocaleString();

        // Calculate percentages relative to total
        const wastedPercent = totalMb > 0 ? (wastedMb / totalMb) * 100 : 0;
        const savedPercent = totalMb > 0 ? (savedMb / totalMb) * 100 : 0;

        // Update bar widths with animation
        document.getElementById('space-total-bar').style.width = '100%';
        document.getElementById('space-wasted-bar').style.width = `${Math.min(wastedPercent, 100)}%`;
        document.getElementById('space-saved-bar').style.width = `${Math.min(savedPercent, 100)}%`;

        // Update tooltip/title with percentage info
        const wastedBar = document.getElementById('space-wasted-bar').parentElement;
        const savedBar = document.getElementById('space-saved-bar').parentElement;
        wastedBar.title = `${wastedPercent.toFixed(1)}% of total scanned space`;
        savedBar.title = `${savedPercent.toFixed(1)}% of total scanned space`;
    }

    async loadScanSuggestions() {
        const historyList = document.getElementById('history-list');
        const removableList = document.getElementById('removable-list');
        const commonList = document.getElementById('common-list');

        try {
            const data = await this.api('/api/scan/suggestions');

            // Render removable devices
            if (data.removable_devices && data.removable_devices.length > 0) {
                removableList.innerHTML = data.removable_devices
                    .filter(d => d.is_mounted)
                    .map(d => `
                        <div class="suggestion-item device" data-path="${d.path}" onclick="app.selectScanPath('${d.path}', this)">
                            <span class="icon">💾</span>
                            <div class="info">
                                <div class="name">${d.label || d.name}</div>
                                <div class="path">${d.path}</div>
                            </div>
                            <span class="size">${d.size_human}</span>
                        </div>
                    `).join('') || '<div class="empty">No mounted removable media</div>';
            } else {
                removableList.innerHTML = '<div class="empty">No removable media detected</div>';
            }

            // Render scan history
            if (data.history && data.history.length > 0) {
                historyList.innerHTML = data.history.map(h => `
                    <div class="suggestion-item history" data-path="${h.path}" onclick="app.selectScanPath('${h.path}', this)">
                        <span class="icon">📁</span>
                        <div class="info">
                            <div class="name">${h.path.split('/').pop() || h.path}</div>
                            <div class="path">${h.path}</div>
                            <div class="stats">
                                ${h.file_count} files
                                ${h.duplicate_count > 0 ? `<span class="duplicates">(${h.duplicate_count} duplicates)</span>` : ''}
                            </div>
                        </div>
                    </div>
                `).join('');
            } else {
                historyList.innerHTML = '<div class="empty">No scan history yet</div>';
            }

            // Render common paths
            if (data.common_paths && data.common_paths.length > 0) {
                commonList.innerHTML = data.common_paths.map(p => `
                    <div class="suggestion-item common" data-path="${p}" onclick="app.selectScanPath('${p}', this)">
                        <span class="icon">📂</span>
                        <div class="info">
                            <div class="name">${p.split('/').pop() || p}</div>
                            <div class="path">${p}</div>
                        </div>
                    </div>
                `).join('');
            } else {
                commonList.innerHTML = '<div class="empty">No common paths found</div>';
            }

        } catch (e) {
            console.error('Failed to load scan suggestions:', e);
            historyList.innerHTML = '<div class="empty">Failed to load</div>';
            removableList.innerHTML = '<div class="empty">Failed to load</div>';
            commonList.innerHTML = '<div class="empty">Failed to load</div>';
        }
    }

    selectScanPath(path, element) {
        // Update the input field
        document.getElementById('scan-directory').value = path;

        // Remove selected state from all suggestion items
        document.querySelectorAll('.suggestion-item').forEach(item => {
            item.classList.remove('selected', 'scanning');
        });

        // Find and highlight the clicked element
        if (element) {
            element.classList.add('selected');
        } else {
            // Find by path if element not passed directly
            document.querySelectorAll('.suggestion-item').forEach(item => {
                if (item.dataset.path === path) {
                    item.classList.add('selected');
                }
            });
        }

        // Store selected path for later use
        this.selectedScanPath = path;
    }

    async startScan() {
        const directory = document.getElementById('scan-directory').value.trim();
        if (!directory) {
            alert('Please enter a directory path');
            return;
        }

        const fullScan = document.getElementById('full-scan').checked;
        const btn = document.getElementById('start-scan');
        const progressPanel = document.getElementById('scan-progress');
        const resultPanel = document.getElementById('scan-result');
        const progressFill = document.getElementById('progress-fill');

        // Check if we have systems detected and should use selected ones
        const selectedPaths = this.getSelectedSystemPaths();
        const systemPanel = document.getElementById('system-selection-panel');
        const hasSystemSelection = !systemPanel.classList.contains('hidden') && selectedPaths.length > 0;

        // Validate selection
        if (hasSystemSelection && selectedPaths.length === 0) {
            alert('Please select at least one system to scan, or click "Detect Systems" again');
            return;
        }

        // Update button state
        btn.disabled = true;
        btn.classList.add('scanning');
        btn.textContent = hasSystemSelection ? `Scanning ${selectedPaths.length} system(s)...` : 'Scanning...';

        // Show progress panel with animation
        progressPanel.classList.remove('hidden');
        progressPanel.classList.add('scanning');
        resultPanel.classList.add('hidden');

        // Mark selected suggestion item as scanning
        document.querySelectorAll('.suggestion-item.selected').forEach(item => {
            item.classList.remove('selected');
            item.classList.add('scanning');
        });

        // Activate progress bar animation
        progressFill.classList.add('active');

        try {
            // Build request body
            const requestBody = {
                directory,
                full_scan: fullScan
            };

            // Add selected system directories if available
            if (hasSystemSelection) {
                requestBody.directories = selectedPaths;
            }

            const response = await this.api('/api/scan', {
                method: 'POST',
                body: JSON.stringify(requestBody)
            });

            this.currentScanId = response.scan_id;

            // Save scan state for page reload persistence
            this.saveScanState(response.scan_id, directory);

            this.pollScanProgress();
        } catch (e) {
            alert(`Scan failed: ${e.message}`);
            this.resetScanUI();
        }
    }

    resetScanUI() {
        const btn = document.getElementById('start-scan');
        const progressPanel = document.getElementById('scan-progress');
        const progressFill = document.getElementById('progress-fill');

        btn.disabled = false;
        btn.classList.remove('scanning');
        btn.textContent = 'Start Scan';

        progressPanel.classList.add('hidden');
        progressPanel.classList.remove('scanning');
        progressFill.classList.remove('active');

        // Reset suggestion items
        document.querySelectorAll('.suggestion-item.scanning').forEach(item => {
            item.classList.remove('scanning');
        });
    }

    pollScanProgress() {
        if (this.scanPollInterval) {
            clearInterval(this.scanPollInterval);
        }

        this.scanPollInterval = setInterval(async () => {
            try {
                const progress = await this.api(`/api/scan/${this.currentScanId}/status`);
                this.updateProgressUI(progress);

                if (progress.status === 'completed' || progress.status === 'error') {
                    clearInterval(this.scanPollInterval);
                    this.scanPollInterval = null;

                    // Clear saved scan state
                    this.clearScanState();

                    // Reset UI state
                    this.resetScanUI();

                    // Keep progress panel visible for results
                    const progressPanel = document.getElementById('scan-progress');
                    progressPanel.classList.remove('hidden');

                    if (progress.status === 'completed') {
                        const result = await this.api(`/api/scan/${this.currentScanId}/result`);
                        this.showScanResult(result);
                    } else {
                        this.showScanResult({ status: 'error', error: 'Scan failed' });
                    }
                }
            } catch (e) {
                console.error('Failed to poll progress:', e);
            }
        }, 500);
    }

    updateProgressUI(progress) {
        // Update progress bar
        const progressFill = document.getElementById('progress-fill');
        progressFill.style.width = `${progress.percent_complete}%`;

        // Update counters with animation trigger
        const processedEl = document.getElementById('progress-processed');
        const totalEl = document.getElementById('progress-total');
        const newProcessed = progress.processed_files.toString();

        if (processedEl.textContent !== newProcessed) {
            processedEl.textContent = newProcessed;
            // Trigger animation by removing and re-adding class
            processedEl.style.animation = 'none';
            processedEl.offsetHeight; // Trigger reflow
            processedEl.style.animation = null;
        }

        totalEl.textContent = progress.total_files;

        // Update status and percent
        document.getElementById('progress-status').textContent = progress.status;
        document.getElementById('progress-percent').textContent = `${progress.percent_complete.toFixed(1)}%`;

        // Update current file with truncation
        const currentFile = progress.current_file || 'Processing...';
        const truncated = currentFile.length > 80
            ? '...' + currentFile.slice(-77)
            : currentFile;
        document.getElementById('progress-current').textContent = truncated;

        // Update verbose progress fields
        document.getElementById('progress-step').textContent = progress.current_step || 'Processing...';
        document.getElementById('progress-storage').textContent = progress.storage_type || '-';
        document.getElementById('progress-threads').textContent = progress.thread_count || '-';
        document.getElementById('progress-hashed').textContent = progress.files_hashed?.toLocaleString() || '0';
        document.getElementById('progress-last-hash').textContent = progress.last_hash ? progress.last_hash.substring(0, 12) + '...' : '-';
        document.getElementById('progress-serial').textContent = progress.last_serial || '-';
        document.getElementById('progress-ra-checked').textContent = progress.files_ra_checked?.toLocaleString() || '0';
        document.getElementById('progress-ra-matched').textContent = progress.files_ra_matched?.toLocaleString() || '0';
        document.getElementById('progress-ra-result').textContent = progress.last_ra_result || '-';
    }

    showScanResult(result) {
        const panel = document.getElementById('scan-result');
        panel.classList.remove('hidden');

        if (result.status === 'completed') {
            const r = result.result;
            // Store the scanned directory for duplicates view
            this.lastScannedDirectory = r.directory;

            // Build media stats row if media was found
            const mediaStatsHtml = (r.media_files_found || r.roms_with_media) ? `
                <div class="scan-result-media-stats">
                    <div class="result-stat media">
                        <span class="result-value">${(r.media_files_found || 0).toLocaleString()}</span>
                        <span class="result-label">Media Files</span>
                    </div>
                    <div class="result-stat media">
                        <span class="result-value">${(r.roms_with_media || 0).toLocaleString()}</span>
                        <span class="result-label">ROMs with Media</span>
                    </div>
                </div>
            ` : '';

            panel.innerHTML = `
                <div class="scan-complete-animation">
                    <div class="success-check">
                        <svg viewBox="0 0 52 52" width="60" height="60">
                            <circle class="check-circle" cx="26" cy="26" r="25" fill="none" stroke="var(--success)" stroke-width="2"/>
                            <path class="check-mark" fill="none" stroke="var(--success)" stroke-width="3" d="M14.1 27.2l7.1 7.2 16.7-16.8"/>
                        </svg>
                    </div>
                    <h3 style="color: var(--success); margin: 15px 0;">Scan Complete!</h3>
                </div>
                <div class="scan-result-stats">
                    <div class="result-stat">
                        <span class="result-value">${r.files_processed.toLocaleString()}</span>
                        <span class="result-label">Files Processed</span>
                    </div>
                    <div class="result-stat">
                        <span class="result-value">${r.duration_seconds}s</span>
                        <span class="result-label">Duration</span>
                    </div>
                    <div class="result-stat ${r.errors > 0 ? 'has-errors' : ''}">
                        <span class="result-value">${r.errors}</span>
                        <span class="result-label">Errors</span>
                    </div>
                </div>
                ${mediaStatsHtml}
                <p style="margin-top: 15px; color: var(--text-secondary); font-size: 0.9rem;">
                    ${r.is_update ? 'Incremental update' : 'Full scan'} of <code>${r.directory}</code>
                </p>
                <div class="scan-result-actions">
                    <button class="btn action-btn primary" onclick="app.goToDuplicates()">
                        <span class="btn-icon">🔍</span>
                        <span>View Duplicates</span>
                    </button>
                </div>
            `;
        } else {
            panel.innerHTML = `
                <div class="scan-error-animation">
                    <span style="font-size: 3rem;">⚠️</span>
                    <h3 style="color: var(--error); margin: 15px 0;">Scan Failed</h3>
                    <p>${result.error}</p>
                </div>
            `;
        }
    }

    async loadDuplicates() {
        const directory = document.getElementById('duplicates-directory').value.trim();
        if (!directory) {
            alert('Please enter a directory path');
            return;
        }

        try {
            const data = await this.api(`/api/duplicates?directory=${encodeURIComponent(directory)}`);
            this.currentDuplicatesData = data;
            this.renderDuplicates(data);
        } catch (e) {
            alert(`Failed to load duplicates: ${e.message}`);
        }
    }

    switchDuplicatesViewMode(mode) {
        this.duplicatesViewMode = mode;

        // Update toggle buttons
        document.querySelectorAll('.view-toggle .view-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.viewMode === mode);
        });

        // Re-render if we have data
        if (this.currentDuplicatesData) {
            this.renderDuplicates(this.currentDuplicatesData);
        }
    }

    renderDuplicates(data) {
        const summary = document.getElementById('duplicates-summary');
        summary.innerHTML = `
            <div class="summary-stats">
                <div class="summary-stat">
                    <span class="summary-value">${data.total_groups}</span>
                    <span class="summary-label">Duplicate Groups</span>
                </div>
                <div class="summary-stat">
                    <span class="summary-value">${data.total_duplicate_files}</span>
                    <span class="summary-label">Total Files</span>
                </div>
                <div class="summary-stat">
                    <span class="summary-value">${data.files_to_remove}</span>
                    <span class="summary-label">To Remove</span>
                </div>
                <div class="summary-stat wasted">
                    <span class="summary-value">${this.formatSize(data.wasted_space_mb * 1024 * 1024)}</span>
                    <span class="summary-label">Wasted Space</span>
                </div>
            </div>
        `;

        const list = document.getElementById('duplicates-list');
        const gallery = document.getElementById('duplicates-gallery');

        if (this.duplicatesViewMode === 'gallery') {
            list.classList.add('hidden');
            gallery.classList.remove('hidden');
            this.renderDuplicatesGallery(data, gallery);
        } else {
            gallery.classList.add('hidden');
            list.classList.remove('hidden');
            this.renderDuplicatesList(data, list);
        }
    }

    renderDuplicatesList(data, container) {
        container.innerHTML = data.groups.map(group => `
            <div class="duplicate-group">
                <div class="duplicate-group-header">
                    <span>${group.file_count} files</span>
                    <code>${group.md5}</code>
                </div>
                <div class="duplicate-files">
                    ${group.files.map(file => `
                        <div class="duplicate-file ${file.filepath === group.recommended_keep ? 'keep' : 'remove'}">
                            <div class="duplicate-file-info">
                                <span class="duplicate-file-name">${file.filename}</span>
                                <span class="duplicate-file-path">${file.filepath}</span>
                            </div>
                            <span class="duplicate-file-score">Score: ${file.score.toFixed(2)}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `).join('');
    }

    async renderDuplicatesGallery(data, container) {
        if (data.groups.length === 0) {
            container.innerHTML = '<p class="no-results">No duplicate files found!</p>';
            document.getElementById('duplicates-action-panel').classList.add('hidden');
            return;
        }

        // Show action panel
        document.getElementById('duplicates-action-panel').classList.remove('hidden');
        this.selectedGroups = new Set();

        // Render game cards for each group
        container.innerHTML = data.groups.map((group, index) => {
            const keepFile = group.files.find(f => f.filepath === group.recommended_keep);
            const removeFiles = group.files.filter(f => f.filepath !== group.recommended_keep);
            const gameName = this.extractGameName(keepFile?.filename || group.files[0]?.filename || 'Unknown');
            const wastedSize = removeFiles.reduce((sum, f) => sum + (f.size_mb || 0), 0);

            // Check RA status for the group
            const hasRASupported = group.files.some(f => f.ra_supported);
            const keepFileRA = keepFile?.ra_supported;
            const raGameTitle = keepFile?.ra_game_title || group.files.find(f => f.ra_game_title)?.ra_game_title || '';
            const raCheckedDate = keepFile?.ra_checked_date || group.files.find(f => f.ra_checked_date)?.ra_checked_date || null;

            return `
                <div class="game-card ${hasRASupported ? 'has-ra' : ''}" data-group-index="${index}" data-md5="${group.md5}">
                    <label class="card-select" onclick="event.stopPropagation()">
                        <input type="checkbox" class="group-checkbox" data-md5="${group.md5}" />
                        <span class="checkmark"></span>
                    </label>
                    ${hasRASupported ? `
                        <div class="ra-badge-corner ${keepFileRA ? 'keep-has-ra' : 'other-has-ra'}" title="${keepFileRA ? 'Keeping RA-supported ROM' : 'RA-supported ROM exists but not recommended'}">
                            <svg viewBox="0 0 24 24" fill="currentColor" width="16" height="16">
                                <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>
                            </svg>
                            <span>RA</span>
                        </div>
                    ` : ''}
                    <div class="game-cover" data-filepath="${keepFile?.filepath || ''}">
                        <img src="${this._gameImageUrl(keepFile?.filepath?.split('/').slice(-2,-1)[0] || '', keepFile?.filename || gameName)}"
                             alt="${this._escHtml(gameName)}" loading="lazy"
                             onerror="this.parentElement.innerHTML='<div class=cover-placeholder><svg viewBox=&quot;0 0 24 24&quot; fill=&quot;currentColor&quot; width=&quot;48&quot; height=&quot;48&quot;><path d=&quot;M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z&quot;/></svg><span>No Art</span></div>'">
                    </div>
                    <div class="game-info">
                        <h3 class="game-title" title="${gameName}">${gameName}</h3>
                        ${raGameTitle ? `<div class="ra-game-title" title="RetroAchievements: ${raGameTitle}">${raGameTitle}</div>` : ''}
                        <div class="game-meta">
                            <span class="duplicate-count">${group.file_count} copies</span>
                            <span class="game-size">${this.formatSize(keepFile?.size_mb * 1024 * 1024 || 0)}</span>
                        </div>
                        <div class="wasted-space-badge">
                            <span class="wasted-label">Wasted:</span>
                            <span class="wasted-value">${this.formatSize(wastedSize * 1024 * 1024)}</span>
                        </div>
                        <div class="game-files">
                            <div class="file-item keep ${keepFile?.ra_supported ? 'ra-verified' : ''}">
                                <span class="file-status">KEEP</span>
                                ${keepFile?.ra_supported ? `<span class="ra-icon" title="RA Verified${keepFile.ra_checked_date ? ': ' + this.formatDate(keepFile.ra_checked_date) : ''}">✓</span>` : ''}
                                <span class="file-name" title="${keepFile?.filepath || ''}">${keepFile?.filename || 'N/A'}</span>
                                <span class="file-score">${keepFile?.score?.toFixed(1) || '0'}</span>
                            </div>
                            ${removeFiles.map(f => `
                                <div class="file-item remove ${f.ra_supported ? 'ra-verified' : ''}">
                                    <span class="file-status">REMOVE</span>
                                    ${f.ra_supported ? `<span class="ra-icon" title="RA Verified${f.ra_checked_date ? ': ' + this.formatDate(f.ra_checked_date) : ''}">✓</span>` : ''}
                                    <span class="file-name" title="${f.filepath}">${f.filename}</span>
                                    <span class="file-score">${f.score?.toFixed(1) || '0'}</span>
                                </div>
                            `).join('')}
                        </div>
                        ${raCheckedDate ? `<div class="ra-check-date">RA checked: ${this.formatDate(raCheckedDate)}</div>` : ''}
                    </div>
                    <div class="card-actions">
                        <button class="card-btn process-single" data-md5="${group.md5}" title="Process this group">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polyline points="20 6 9 17 4 12"></polyline>
                            </svg>
                        </button>
                    </div>
                </div>
            `;
        }).join('');

        // Bind checkbox events
        container.querySelectorAll('.group-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', () => this.updateSelectionCount());
        });

        // Bind single process buttons
        container.querySelectorAll('.process-single').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.processSingleGroup(btn.dataset.md5);
            });
        });

        // Bind card click for selection
        container.querySelectorAll('.game-card').forEach(card => {
            card.addEventListener('click', (e) => {
                if (e.target.closest('.card-actions') || e.target.closest('.card-select')) return;
                const checkbox = card.querySelector('.group-checkbox');
                checkbox.checked = !checkbox.checked;
                card.classList.toggle('selected', checkbox.checked);
                this.updateSelectionCount();
            });
        });

        // Load cover art for each card
        this.loadCoverArtForGallery(container);
    }

    extractGameName(filename) {
        if (!filename) return 'Unknown';
        // Remove file extension
        let name = filename.replace(/\.[^/.]+$/, '');
        // Remove common ROM tags like (USA), [!], etc.
        name = name.replace(/\s*\([^)]*\)\s*/g, ' ');
        name = name.replace(/\s*\[[^\]]*\]\s*/g, ' ');
        // Clean up whitespace
        name = name.replace(/\s+/g, ' ').trim();
        return name || filename;
    }

    async loadCoverArtForGallery(container) {
        const coverElements = container.querySelectorAll('.game-cover[data-filepath]');

        // Load cover art in parallel with a small batch size
        const batchSize = 5;
        const elements = Array.from(coverElements);

        for (let i = 0; i < elements.length; i += batchSize) {
            const batch = elements.slice(i, i + batchSize);
            await Promise.all(batch.map(coverEl => this.loadSingleCoverArt(coverEl)));
        }
    }

    async loadSingleCoverArt(coverEl) {
        const filepath = coverEl.dataset.filepath;
        if (!filepath) {
            this.applyNoCover(coverEl);
            return;
        }

        // Check cache first
        if (this.coverArtCache.has(filepath)) {
            const cached = this.coverArtCache.get(filepath);
            this.applyCoverArt(coverEl, cached);
            return;
        }

        try {
            // The filepath already starts with /, so just append it
            const coverData = await this.api(`/api/media/cover-data${filepath}`);

            if (coverData.has_cover && coverData.data) {
                const imgSrc = `data:${coverData.mime_type};base64,${coverData.data}`;
                this.coverArtCache.set(filepath, { src: imgSrc, category: coverData.category });
                this.applyCoverArt(coverEl, { src: imgSrc, category: coverData.category });
            } else {
                this.coverArtCache.set(filepath, null);
                this.applyNoCover(coverEl);
            }
        } catch (e) {
            console.error('Failed to load cover art for:', filepath, e);
            this.coverArtCache.set(filepath, null);
            this.applyNoCover(coverEl);
        }
    }

    applyCoverArt(coverEl, coverData) {
        if (!coverData || !coverData.src) {
            this.applyNoCover(coverEl);
            return;
        }

        coverEl.innerHTML = `
            <img src="${coverData.src}" alt="Cover Art" loading="lazy" />
            ${coverData.category ? `<span class="cover-category">${coverData.category}</span>` : ''}
        `;
        coverEl.classList.add('has-cover');
        coverEl.classList.remove('loading');
    }

    applyNoCover(coverEl) {
        // Extract game name from the filepath for display
        const filepath = coverEl.dataset.filepath || '';
        const filename = filepath.split('/').pop() || '';
        const gameName = this.extractGameName(filename);
        const initials = gameName.split(' ').slice(0, 2).map(w => w[0] || '').join('').toUpperCase();

        coverEl.innerHTML = `
            <div class="cover-placeholder no-art">
                <div class="cover-initials">${initials || '?'}</div>
                <span class="cover-game-name">${gameName || 'Unknown'}</span>
            </div>
        `;
        coverEl.classList.add('no-cover');
        coverEl.classList.remove('loading');
    }

    applyLoadingCover(coverEl) {
        coverEl.innerHTML = `
            <div class="cover-placeholder loading">
                <div class="cover-spinner"></div>
                <span>Loading...</span>
            </div>
        `;
        coverEl.classList.add('loading');
    }

    // Legacy method kept for backwards compatibility
    applyNoCoverLegacy(coverEl) {
        coverEl.innerHTML = `
            <div class="cover-placeholder no-art">
                <svg viewBox="0 0 24 24" fill="currentColor" width="48" height="48">
                    <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>
                    <polyline points="3.27 6.96 12 12.01 20.73 6.96"/>
                    <line x1="12" y1="22.08" x2="12" y2="12"/>
                </svg>
                <span>No Cover</span>
            </div>
        `;
        coverEl.classList.add('no-cover');
    }

    async processDuplicates() {
        // Legacy method - now redirects to action panel selection
        this.selectAllDuplicates();
        alert('Select duplicates to process and choose an action (Archive or Delete) in the action panel above.');
    }

    updateSelectionCount() {
        const checkboxes = document.querySelectorAll('.group-checkbox:checked');
        const count = checkboxes.length;

        document.querySelector('.selected-count').textContent = `${count} selected`;
        document.getElementById('selected-count-btn').textContent = count;

        const processBtn = document.getElementById('process-selected');
        processBtn.disabled = count === 0;

        // Update card selected state
        document.querySelectorAll('.game-card').forEach(card => {
            const checkbox = card.querySelector('.group-checkbox');
            card.classList.toggle('selected', checkbox?.checked);
        });
    }

    selectAllDuplicates() {
        document.querySelectorAll('.group-checkbox').forEach(checkbox => {
            checkbox.checked = true;
        });
        this.updateSelectionCount();
    }

    deselectAllDuplicates() {
        document.querySelectorAll('.group-checkbox').forEach(checkbox => {
            checkbox.checked = false;
        });
        this.updateSelectionCount();
    }

    updateActionUI() {
        const action = document.querySelector('input[name="duplicate-action"]:checked')?.value;
        const archiveSection = document.getElementById('archive-location-section');

        if (action === 'delete') {
            archiveSection.style.display = 'none';
        } else {
            archiveSection.style.display = 'block';
        }
    }

    getSelectedGroupHashes() {
        const checkboxes = document.querySelectorAll('.group-checkbox:checked');
        return Array.from(checkboxes).map(cb => cb.dataset.md5);
    }

    async processSelectedDuplicates() {
        const directory = document.getElementById('duplicates-directory').value.trim();
        if (!directory) {
            alert('Please enter a directory path');
            return;
        }

        const selectedHashes = this.getSelectedGroupHashes();
        if (selectedHashes.length === 0) {
            alert('Please select at least one duplicate group to process');
            return;
        }

        const action = document.querySelector('input[name="duplicate-action"]:checked')?.value || 'archive';
        const archiveLocation = document.getElementById('archive-location').value.trim() || null;
        const dryRun = document.getElementById('duplicates-dry-run').checked;

        const actionText = action === 'delete' ? 'permanently DELETE' : 'archive';
        const confirmMsg = dryRun
            ? `DRY RUN: Preview ${actionText} action on ${selectedHashes.length} duplicate groups?`
            : `This will ${actionText} duplicates from ${selectedHashes.length} groups. This cannot be undone. Continue?`;

        if (!confirm(confirmMsg)) {
            return;
        }

        // Get all selected card elements for animation
        const cards = selectedHashes.map(md5 =>
            document.querySelector(`.game-card[data-md5="${md5}"]`)
        ).filter(Boolean);

        // Add processing state to all cards
        if (!dryRun) {
            cards.forEach(card => card.classList.add('processing'));
        }

        try {
            const result = await this.api('/api/duplicates/process', {
                method: 'POST',
                body: JSON.stringify({
                    directory,
                    action,
                    archive_location: archiveLocation,
                    dry_run: dryRun,
                    group_hashes: selectedHashes,
                })
            });

            if (!dryRun) {
                // Animate cards with staggered effect
                const animClass = action === 'delete' ? 'removing-delete' : 'removing-archive';

                for (let i = 0; i < cards.length; i++) {
                    const card = cards[i];
                    // Stagger the animations
                    setTimeout(() => {
                        card.classList.remove('processing');
                        card.classList.add(animClass);
                    }, i * 100);
                }

                // Wait for all animations to complete
                await new Promise(resolve =>
                    setTimeout(resolve, 800 + (cards.length * 100))
                );

                // Remove all processed cards
                cards.forEach(card => card.remove());

                // Show success toast
                this.showToast(
                    action === 'delete' ? 'Deleted' : 'Archived',
                    `${result.processed_count} files from ${selectedHashes.length} groups`,
                    action === 'delete' ? 'danger' : 'success'
                );

                // Update stats
                this.loadDashboard();

                // Check if there are any cards left
                const remainingCards = document.querySelectorAll('.game-card').length;
                if (remainingCards === 0) {
                    document.getElementById('duplicates-gallery').innerHTML =
                        '<p class="no-results all-processed">All duplicates processed!</p>';
                    document.getElementById('duplicates-action-panel').classList.add('hidden');
                } else {
                    // Update selection count
                    this.updateSelectionCount();
                }
            } else {
                const summary = '[DRY RUN] Would process';
                let message = `${summary} ${result.processed_count} files.\n`;
                message += `- Would Archive: ${result.archived_count}\n`;
                message += `- Would Delete: ${result.deleted_count}\n`;
                message += `- Space to free: ${this.formatSize(result.space_freed_mb * 1024 * 1024)}\n`;

                if (result.errors.length > 0) {
                    message += `\nErrors: ${result.errors.length}`;
                }

                alert(message);
            }
        } catch (e) {
            // Remove processing state on error
            cards.forEach(card => {
                card.classList.remove('processing');
                card.classList.add('error');
                setTimeout(() => card.classList.remove('error'), 1000);
            });
            alert(`Failed to process duplicates: ${e.message}`);
        }
    }

    async processSingleGroup(md5) {
        const directory = document.getElementById('duplicates-directory').value.trim();
        if (!directory) {
            alert('Please enter a directory path');
            return;
        }

        const action = document.querySelector('input[name="duplicate-action"]:checked')?.value || 'archive';
        const archiveLocation = document.getElementById('archive-location').value.trim() || null;
        const dryRun = document.getElementById('duplicates-dry-run').checked;

        const actionText = action === 'delete' ? 'DELETE' : 'archive';
        if (!confirm(`${dryRun ? '[DRY RUN] ' : ''}${actionText} duplicates from this group?`)) {
            return;
        }

        // Find the card element for animation
        const card = document.querySelector(`.game-card[data-md5="${md5}"]`);

        try {
            const result = await this.api('/api/duplicates/process', {
                method: 'POST',
                body: JSON.stringify({
                    directory,
                    action,
                    archive_location: archiveLocation,
                    dry_run: dryRun,
                    group_hashes: [md5],
                })
            });

            if (!dryRun && card) {
                // Animate the card removal
                const animClass = action === 'delete' ? 'removing-delete' : 'removing-archive';
                card.classList.add('processing');

                // Brief processing state
                await new Promise(resolve => setTimeout(resolve, 200));

                card.classList.remove('processing');
                card.classList.add(animClass);

                // Wait for animation to complete before removing
                await new Promise(resolve => setTimeout(resolve, 800));

                // Remove the card from DOM
                card.remove();

                // Show success toast
                this.showToast(
                    action === 'delete' ? 'Deleted' : 'Archived',
                    `${result.processed_count} files processed`,
                    action === 'delete' ? 'danger' : 'success'
                );

                // Update stats
                this.loadDashboard();

                // Check if there are any cards left
                const remainingCards = document.querySelectorAll('.game-card').length;
                if (remainingCards === 0) {
                    document.getElementById('duplicates-gallery').innerHTML =
                        '<p class="no-results all-processed">All duplicates processed!</p>';
                    document.getElementById('duplicates-action-panel').classList.add('hidden');
                }
            } else if (dryRun) {
                alert(`[DRY RUN] Would ${action} ${result.processed_count} files`);
            }
        } catch (e) {
            if (card) {
                card.classList.remove('processing');
                card.classList.add('error');
                setTimeout(() => card.classList.remove('error'), 1000);
            }
            alert(`Failed: ${e.message}`);
        }
    }

    showToast(title, message, type = 'info') {
        // Create toast container if it doesn't exist
        let container = document.getElementById('toast-container');
        if (!container) {
            container = document.createElement('div');
            container.id = 'toast-container';
            document.body.appendChild(container);
        }

        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.innerHTML = `
            <div class="toast-icon">${type === 'danger' ? '🗑️' : type === 'success' ? '📦' : 'ℹ️'}</div>
            <div class="toast-content">
                <div class="toast-title">${title}</div>
                <div class="toast-message">${message}</div>
            </div>
        `;

        container.appendChild(toast);

        // Trigger animation
        requestAnimationFrame(() => {
            toast.classList.add('show');
        });

        // Remove after delay
        setTimeout(() => {
            toast.classList.remove('show');
            toast.classList.add('hide');
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }

    async loadMovedFiles() {
        try {
            const data = await this.api('/api/duplicates/moved');
            this.renderMovedFiles(data);
        } catch (e) {
            console.error('Failed to load moved files:', e);
        }
    }

    renderMovedFiles(data) {
        const list = document.getElementById('moved-list');

        if (data.files.length === 0) {
            list.innerHTML = '<p>No moved files</p>';
            return;
        }

        list.innerHTML = data.files.map(file => `
            <div class="moved-file">
                <div class="moved-file-info">
                    <div class="moved-file-name">${file.filename}</div>
                    <div class="moved-file-paths">
                        <span><strong>From:</strong> ${file.original_filepath}</span>
                        <span><strong>To:</strong> ${file.moved_to_path}</span>
                        <span><strong>Moved:</strong> ${file.moved_time}</span>
                    </div>
                </div>
                <button class="btn" onclick="app.restoreFile(${file.move_id})">Restore</button>
            </div>
        `).join('');
    }

    async restoreFile(moveId) {
        try {
            const result = await this.api('/api/duplicates/restore', {
                method: 'POST',
                body: JSON.stringify({ move_id: moveId })
            });

            if (result.restored_count > 0) {
                this.loadMovedFiles();
            } else {
                alert(`Failed to restore: ${result.errors.join(', ')}`);
            }
        } catch (e) {
            alert(`Failed to restore: ${e.message}`);
        }
    }

    async restoreAll() {
        if (!confirm('This will restore all moved files to their original locations. Continue?')) {
            return;
        }

        try {
            const result = await this.api('/api/duplicates/restore-all', {
                method: 'POST'
            });

            alert(`Restored ${result.restored_count} files. ${result.errors.length} errors.`);
            this.loadMovedFiles();
        } catch (e) {
            alert(`Failed to restore: ${e.message}`);
        }
    }

    async loadConfig() {
        try {
            const config = await this.api('/api/config');

            // Server settings
            document.getElementById('cfg-server-port').value = config.server.port;
            document.getElementById('cfg-server-host').value = config.server.host;
            document.getElementById('cfg-web-ui-enabled').checked = config.server.web_ui_enabled;
            document.getElementById('cfg-auth-enabled').checked = config.server.auth_enabled;
            document.getElementById('cfg-api-key').value = config.server.has_api_key ? '********' : '';

            // Scanner settings
            document.getElementById('cfg-ignore-fodder').checked = config.scanner.ignore_fodder;
            document.getElementById('cfg-ignore-video').checked = config.scanner.ignore_video;
            document.getElementById('cfg-ignore-music').checked = config.scanner.ignore_music;
            document.getElementById('cfg-ignore-pictures').checked = config.scanner.ignore_pictures;
            document.getElementById('cfg-retroarch-mode').checked = config.scanner.retroarch_mode;

            // Paths
            document.getElementById('cfg-working-dir').value = config.paths.working_dir;
            document.getElementById('cfg-duplicates-dir').value = config.paths.duplicates_dir;

            // Load RA config
            const raConfig = await this.api('/api/ra/config');
            document.getElementById('cfg-ra-enabled').checked = raConfig.enabled;
            document.getElementById('cfg-ra-username').value = raConfig.username || '';
            document.getElementById('cfg-ra-api-key').value = raConfig.has_api_key ? '********' : '';
            document.getElementById('cfg-ra-score-bonus').value = raConfig.ra_score_bonus;
            document.getElementById('cfg-ra-verify-on-scan').checked = raConfig.verify_on_scan;

            // Load ScreenScraper config
            const ssConfig = await this.api('/api/ss/config');
            document.getElementById('cfg-ss-enabled').checked = ssConfig.enabled;
            document.getElementById('cfg-ss-username').value = ssConfig.username || '';
            document.getElementById('cfg-ss-password').value = ssConfig.has_password ? '********' : '';
            document.getElementById('cfg-ss-dev-id').value = ssConfig.has_dev_credentials ? '********' : '';
            document.getElementById('cfg-ss-dev-password').value = ssConfig.has_dev_credentials ? '********' : '';
            document.getElementById('cfg-ss-region').value = ssConfig.preferred_region || 'us';
            document.getElementById('cfg-ss-language').value = ssConfig.preferred_language || 'en';
            document.getElementById('cfg-ss-box-art').checked = ssConfig.download_box_art;
            document.getElementById('cfg-ss-screenshot').checked = ssConfig.download_screenshot;
            document.getElementById('cfg-ss-wheel').checked = ssConfig.download_wheel;
            document.getElementById('cfg-ss-video').checked = ssConfig.download_video;
            document.getElementById('cfg-ss-media-path').value = ssConfig.media_path || '';
        } catch (e) {
            console.error('Failed to load config:', e);
        }
    }

    async saveConfig() {
        // Build config object with server settings
        const serverApiKey = document.getElementById('cfg-api-key').value;
        const config = {
            // Server settings
            server_port: parseInt(document.getElementById('cfg-server-port').value) || 8420,
            server_host: document.getElementById('cfg-server-host').value,
            web_ui_enabled: document.getElementById('cfg-web-ui-enabled').checked,
            auth_enabled: document.getElementById('cfg-auth-enabled').checked,
            // Scanner settings
            ignore_fodder: document.getElementById('cfg-ignore-fodder').checked,
            ignore_video: document.getElementById('cfg-ignore-video').checked,
            ignore_music: document.getElementById('cfg-ignore-music').checked,
            ignore_pictures: document.getElementById('cfg-ignore-pictures').checked,
            retroarch_mode: document.getElementById('cfg-retroarch-mode').checked,
            // Paths
            working_dir: document.getElementById('cfg-working-dir').value,
            duplicates_dir: document.getElementById('cfg-duplicates-dir').value
        };

        // Only include API key if changed
        if (serverApiKey && serverApiKey !== '********') {
            config.api_key = serverApiKey;
        }

        try {
            await this.api('/api/config', {
                method: 'PUT',
                body: JSON.stringify(config)
            });

            // Save RA config separately
            const raApiKey = document.getElementById('cfg-ra-api-key').value;
            const raConfig = {
                enabled: document.getElementById('cfg-ra-enabled').checked,
                username: document.getElementById('cfg-ra-username').value,
                ra_score_bonus: parseInt(document.getElementById('cfg-ra-score-bonus').value) || 1000,
                verify_on_scan: document.getElementById('cfg-ra-verify-on-scan').checked
            };

            // Only include API key if it was changed (not placeholder)
            if (raApiKey && raApiKey !== '********') {
                raConfig.api_key = raApiKey;
            }

            await this.api('/api/ra/config', {
                method: 'PUT',
                body: JSON.stringify(raConfig)
            });

            // Save ScreenScraper config
            const ssPassword = document.getElementById('cfg-ss-password').value;
            const ssDevId = document.getElementById('cfg-ss-dev-id').value;
            const ssDevPassword = document.getElementById('cfg-ss-dev-password').value;
            const ssConfig = {
                enabled: document.getElementById('cfg-ss-enabled').checked,
                username: document.getElementById('cfg-ss-username').value,
                preferred_region: document.getElementById('cfg-ss-region').value,
                preferred_language: document.getElementById('cfg-ss-language').value,
                download_box_art: document.getElementById('cfg-ss-box-art').checked,
                download_screenshot: document.getElementById('cfg-ss-screenshot').checked,
                download_wheel: document.getElementById('cfg-ss-wheel').checked,
                download_video: document.getElementById('cfg-ss-video').checked,
                media_path: document.getElementById('cfg-ss-media-path').value
            };

            // Only include credentials if changed (not placeholder)
            if (ssPassword && ssPassword !== '********') {
                ssConfig.password = ssPassword;
            }
            if (ssDevId && ssDevId !== '********') {
                ssConfig.dev_id = ssDevId;
            }
            if (ssDevPassword && ssDevPassword !== '********') {
                ssConfig.dev_password = ssDevPassword;
            }

            await this.api('/api/ss/config', {
                method: 'PUT',
                body: JSON.stringify(ssConfig)
            });

            alert('Configuration saved. Server settings require a restart to take effect.');
        } catch (e) {
            alert(`Failed to save config: ${e.message}`);
        }
    }

    async testRAConnection() {
        const resultEl = document.getElementById('ra-test-result');
        resultEl.textContent = 'Testing...';
        resultEl.className = 'ra-test-result testing';

        // First save current RA config
        const raApiKey = document.getElementById('cfg-ra-api-key').value;
        const raConfig = {
            enabled: document.getElementById('cfg-ra-enabled').checked,
            username: document.getElementById('cfg-ra-username').value,
            ra_score_bonus: parseInt(document.getElementById('cfg-ra-score-bonus').value) || 1000,
            verify_on_scan: document.getElementById('cfg-ra-verify-on-scan').checked
        };

        if (raApiKey && raApiKey !== '********') {
            raConfig.api_key = raApiKey;
        }

        try {
            // Update config first
            await this.api('/api/ra/config', {
                method: 'PUT',
                body: JSON.stringify(raConfig)
            });

            // Test with a known hash (Super Mario Bros NES)
            const testHash = '3337ec46b36c0a5c3adbc71c9ac3c7e2';
            const result = await this.api(`/api/ra/verify/${testHash}`);

            if (result.ra_supported) {
                resultEl.textContent = `Success! Found: ${result.game_title}`;
                resultEl.className = 'ra-test-result success';
            } else {
                resultEl.textContent = 'Connected but test hash not found';
                resultEl.className = 'ra-test-result warning';
            }
        } catch (e) {
            resultEl.textContent = `Failed: ${e.message}`;
            resultEl.className = 'ra-test-result error';
        }
    }

    async testSSConnection() {
        const resultEl = document.getElementById('ss-test-result');
        resultEl.textContent = 'Testing...';
        resultEl.className = 'ss-test-result testing';

        // First save current SS config
        const ssPassword = document.getElementById('cfg-ss-password').value;
        const ssDevId = document.getElementById('cfg-ss-dev-id').value;
        const ssDevPassword = document.getElementById('cfg-ss-dev-password').value;
        const ssConfig = {
            enabled: document.getElementById('cfg-ss-enabled').checked,
            username: document.getElementById('cfg-ss-username').value,
            preferred_region: document.getElementById('cfg-ss-region').value,
            preferred_language: document.getElementById('cfg-ss-language').value
        };

        if (ssPassword && ssPassword !== '********') {
            ssConfig.password = ssPassword;
        }
        if (ssDevId && ssDevId !== '********') {
            ssConfig.dev_id = ssDevId;
        }
        if (ssDevPassword && ssDevPassword !== '********') {
            ssConfig.dev_password = ssDevPassword;
        }

        try {
            // Update config first
            await this.api('/api/ss/config', {
                method: 'PUT',
                body: JSON.stringify(ssConfig)
            });

            // Test connection
            const result = await this.api('/api/ss/test');

            if (result.success) {
                resultEl.textContent = `Connected! User: ${result.username}, Level: ${result.level}, Requests: ${result.requests_today}/${result.requests_max}`;
                resultEl.className = 'ss-test-result success';
            } else {
                resultEl.textContent = result.error || 'Connection failed';
                resultEl.className = 'ss-test-result error';
            }
        } catch (e) {
            resultEl.textContent = `Failed: ${e.message}`;
            resultEl.className = 'ss-test-result error';
        }
    }

    // === Media Functions ===

    switchMediaTab(tab) {
        document.querySelectorAll('.media-tab').forEach(t => {
            t.classList.toggle('active', t.dataset.mediaTab === tab);
        });
        document.getElementById('media-orphaned-tab').classList.toggle('active', tab === 'orphaned');
        document.getElementById('media-moved-roms-tab').classList.toggle('active', tab === 'moved-roms');

        // Clear results when switching tabs
        document.getElementById('media-summary').classList.add('hidden');
        document.getElementById('media-list').innerHTML = '';
        document.getElementById('media-cleanup-controls').classList.add('hidden');
        this.currentMediaData = null;
        this.currentMediaMode = tab;
    }

    async findOrphanedMedia() {
        const directory = document.getElementById('media-directory').value.trim();
        if (!directory) {
            alert('Please enter a ROM directory path');
            return;
        }

        try {
            const data = await this.api(`/api/media/orphaned?directory=${encodeURIComponent(directory)}`);
            this.currentMediaData = data;
            this.currentMediaMode = 'orphaned';
            this.renderMediaResults(data);
        } catch (e) {
            alert(`Failed to find orphaned media: ${e.message}`);
        }
    }

    async findMovedRomMedia() {
        try {
            const data = await this.api('/api/media/moved-roms');
            this.currentMediaData = data;
            this.currentMediaMode = 'moved-roms';
            this.renderMediaResults(data);
        } catch (e) {
            alert(`Failed to find media for moved ROMs: ${e.message}`);
        }
    }

    renderMediaResults(data) {
        const summary = document.getElementById('media-summary');
        const list = document.getElementById('media-list');
        const controls = document.getElementById('media-cleanup-controls');

        if (data.total_files === 0) {
            summary.classList.add('hidden');
            list.innerHTML = '<p class="no-results">No orphaned media files found!</p>';
            controls.classList.add('hidden');
            return;
        }

        summary.classList.remove('hidden');
        summary.innerHTML = `
            <p><strong>Total Files:</strong> ${data.total_files}</p>
            <p><strong>Total Size:</strong> ${this.formatSize(data.total_size_bytes)}</p>
            <p><strong>ROM Names:</strong> ${data.orphaned.length}</p>
        `;

        list.innerHTML = data.orphaned.map(orphan => `
            <div class="media-group">
                <div class="media-group-header">
                    <span class="media-rom-name">${orphan.rom_name}</span>
                    <span class="media-file-count">${orphan.file_count} files (${this.formatSize(orphan.total_size_bytes)})</span>
                </div>
                ${orphan.rom_path ? `<div class="media-rom-path">Original ROM: ${orphan.rom_path}</div>` : ''}
                <div class="media-files">
                    ${orphan.media_files.map(file => `
                        <div class="media-file">
                            <span class="media-category">${file.category}</span>
                            <span class="media-type">${file.media_type}</span>
                            <span class="media-path" title="${file.path}">${file.path.split('/').pop()}</span>
                            <span class="media-size">${this.formatSize(file.size_bytes)}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `).join('');

        controls.classList.remove('hidden');
    }

    async cleanupMedia() {
        if (!this.currentMediaData || this.currentMediaData.total_files === 0) {
            alert('No media files to clean up');
            return;
        }

        const dryRun = document.getElementById('media-dry-run').checked;
        const moveTo = document.getElementById('media-move-to').value.trim() || null;

        const action = dryRun ? 'preview' : (moveTo ? 'move' : 'delete');
        if (!dryRun && !confirm(`This will ${moveTo ? 'move' : 'DELETE'} ${this.currentMediaData.total_files} media files. Continue?`)) {
            return;
        }

        try {
            const body = {
                dry_run: dryRun,
                move_to: moveTo
            };

            if (this.currentMediaMode === 'orphaned') {
                body.directory = document.getElementById('media-directory').value.trim();
            } else {
                body.cleanup_moved_roms = true;
            }

            const result = await this.api('/api/media/cleanup', {
                method: 'POST',
                body: JSON.stringify(body)
            });

            if (dryRun) {
                alert(`Dry run complete. Would ${moveTo ? 'move' : 'remove'} ${result.removed_count} files (${this.formatSize(result.removed_size_bytes)})`);
            } else {
                alert(`${moveTo ? 'Moved' : 'Removed'} ${result.removed_count} files (${this.formatSize(result.removed_size_bytes)}). ${result.errors.length} errors.`);
                // Refresh the media list
                if (this.currentMediaMode === 'orphaned') {
                    this.findOrphanedMedia();
                } else {
                    this.findMovedRomMedia();
                }
            }
        } catch (e) {
            alert(`Failed to cleanup media: ${e.message}`);
        }
    }

    // === Saves Functions ===

    switchSavesTab(tab) {
        document.querySelectorAll('.saves-tab').forEach(t => {
            t.classList.toggle('active', t.dataset.savesTab === tab);
        });
        document.getElementById('saves-orphaned-tab').classList.toggle('active', tab === 'orphaned');
        document.getElementById('saves-moved-roms-tab').classList.toggle('active', tab === 'moved-roms');

        // Clear results when switching tabs
        document.getElementById('saves-summary').classList.add('hidden');
        document.getElementById('saves-list').innerHTML = '';
        document.getElementById('saves-preserve-controls').classList.add('hidden');
        this.currentSavesData = null;
        this.currentSavesMode = tab;
    }

    async findOrphanedSaves() {
        const directory = document.getElementById('saves-directory').value.trim();
        if (!directory) {
            alert('Please enter a ROM directory path');
            return;
        }

        try {
            const data = await this.api(`/api/saves/orphaned?directory=${encodeURIComponent(directory)}`);
            this.currentSavesData = data;
            this.currentSavesMode = 'orphaned';
            this.renderSavesResults(data);
        } catch (e) {
            alert(`Failed to find orphaned saves: ${e.message}`);
        }
    }

    async findMovedRomSaves() {
        try {
            const data = await this.api('/api/saves/moved-roms');
            this.currentSavesData = data;
            this.currentSavesMode = 'moved-roms';
            this.renderSavesResults(data);
        } catch (e) {
            alert(`Failed to find saves for moved ROMs: ${e.message}`);
        }
    }

    renderSavesResults(data) {
        const summary = document.getElementById('saves-summary');
        const list = document.getElementById('saves-list');
        const controls = document.getElementById('saves-preserve-controls');

        if (data.total_files === 0) {
            summary.classList.add('hidden');
            list.innerHTML = '<p class="no-results">No orphaned saves found!</p>';
            controls.classList.add('hidden');
            return;
        }

        summary.classList.remove('hidden');
        summary.innerHTML = `
            <p><strong>Save Games:</strong> ${data.total_saves}</p>
            <p><strong>Save States:</strong> ${data.total_states}</p>
            <p><strong>Total Size:</strong> ${this.formatSize(data.total_size_bytes)}</p>
            <p><strong>ROM Names:</strong> ${data.orphaned.length}</p>
        `;

        list.innerHTML = data.orphaned.map(orphan => `
            <div class="saves-group">
                <div class="saves-group-header">
                    <span class="saves-rom-name">${orphan.rom_name}</span>
                    <span class="saves-file-count">${orphan.save_count + orphan.state_count} files (${this.formatSize(orphan.total_size_bytes)})</span>
                </div>
                ${orphan.rom_path ? `<div class="saves-rom-path">Original ROM: ${orphan.rom_path}</div>` : ''}
                <div class="saves-files">
                    ${orphan.save_files.map(file => `
                        <div class="save-file save-type">
                            <span class="save-badge save">SAVE</span>
                            <span class="save-ext">${file.extension}</span>
                            <span class="save-path" title="${file.path}">${file.path.split('/').pop()}</span>
                            <span class="save-size">${this.formatSize(file.size_bytes)}</span>
                        </div>
                    `).join('')}
                    ${orphan.state_files.map(file => `
                        <div class="save-file state-type">
                            <span class="save-badge state">STATE</span>
                            <span class="save-ext">${file.extension}</span>
                            <span class="save-path" title="${file.path}">${file.path.split('/').pop()}</span>
                            <span class="save-size">${this.formatSize(file.size_bytes)}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `).join('');

        controls.classList.remove('hidden');
    }

    async preserveSaves() {
        if (!this.currentSavesData || this.currentSavesData.total_files === 0) {
            alert('No saves to preserve');
            return;
        }

        const dryRun = document.getElementById('saves-dry-run').checked;
        const moveTo = document.getElementById('saves-move-to').value.trim();

        if (!moveTo) {
            alert('Please specify a destination directory to preserve saves');
            return;
        }

        if (!dryRun && !confirm(`This will MOVE ${this.currentSavesData.total_files} save files to ${moveTo}. Continue?`)) {
            return;
        }

        try {
            const body = {
                move_to: moveTo,
                dry_run: dryRun
            };

            if (this.currentSavesMode === 'orphaned') {
                body.directory = document.getElementById('saves-directory').value.trim();
            } else {
                body.preserve_moved_roms = true;
            }

            const result = await this.api('/api/saves/preserve', {
                method: 'POST',
                body: JSON.stringify(body)
            });

            if (dryRun) {
                alert(`Dry run complete. Would move ${result.moved_count} files (${this.formatSize(result.moved_size_bytes)})`);
            } else {
                alert(`Preserved ${result.moved_count} save files (${this.formatSize(result.moved_size_bytes)}). ${result.errors.length} errors.`);
                // Refresh the saves list
                if (this.currentSavesMode === 'orphaned') {
                    this.findOrphanedSaves();
                } else {
                    this.findMovedRomSaves();
                }
            }
        } catch (e) {
            alert(`Failed to preserve saves: ${e.message}`);
        }
    }

    // === System Detection Functions ===

    async detectSystems() {
        const directory = document.getElementById('scan-directory').value.trim();
        if (!directory) {
            alert('Please enter a directory path first');
            return;
        }

        const panel = document.getElementById('system-selection-panel');
        const checkboxContainer = document.getElementById('system-checkboxes');

        // Show panel with loading state
        panel.classList.remove('hidden');
        checkboxContainer.innerHTML = '<div class="loading">Detecting systems...</div>';

        try {
            const data = await this.api(`/api/scan/detect-systems?directory=${encodeURIComponent(directory)}`);
            this.detectedSystems = data.systems;
            this.renderSystemCheckboxes(data);
        } catch (e) {
            checkboxContainer.innerHTML = `<div class="empty">Failed to detect systems: ${e.message}</div>`;
        }
    }

    renderSystemCheckboxes(data) {
        const container = document.getElementById('system-checkboxes');

        if (!data.systems || data.systems.length === 0) {
            container.innerHTML = '<div class="empty">No recognized game systems found in this directory.</div>';
            return;
        }

        // System icons based on type
        const systemIcons = {
            'NES': '🎮', 'SNES': '🎮', 'N64': '🎮', 'GameCube': '🎮', 'Wii': '🎮', 'Wii U': '🎮', 'Switch': '🎮',
            'Game Boy': '🕹️', 'Game Boy Color': '🕹️', 'Game Boy Advance': '🕹️', 'Nintendo DS': '📱', 'Nintendo 3DS': '📱',
            'PlayStation': '🎮', 'PlayStation 2': '📀', 'PlayStation 3': '📀', 'PSP': '📱', 'PS Vita': '📱',
            'Sega Master System': '🎮', 'Genesis': '🎮', 'Sega CD': '📀', 'Sega 32X': '🎮', 'Saturn': '📀', 'Dreamcast': '📀', 'Game Gear': '🕹️',
            'Atari 2600': '🕹️', 'Atari 7800': '🕹️', 'Atari Lynx': '🕹️', 'Atari Jaguar': '🎮',
            'TurboGrafx-16': '🎮', 'PC Engine CD': '📀', 'Neo Geo': '🎮', 'Neo Geo Pocket': '🕹️',
            'Arcade': '🕹️', 'MAME': '🕹️', 'FBNeo': '🕹️',
            'DOS': '💾', 'Windows': '🖥️', 'ScummVM': '🖥️',
            'default': '📁'
        };

        container.innerHTML = data.systems.map((system, index) => {
            const icon = systemIcons[system.system] || systemIcons['default'];
            return `
                <label class="system-checkbox" data-index="${index}">
                    <input type="checkbox"
                           class="system-check"
                           data-path="${system.path}"
                           data-files="${system.file_count}"
                           data-size="${system.size_mb}"
                           checked />
                    <span class="system-icon">${icon}</span>
                    <div class="system-info">
                        <div class="system-name">${system.system}</div>
                        <div class="system-folder">/${system.folder}</div>
                        <div class="system-stats">
                            <span class="count">${system.file_count.toLocaleString()} files</span>
                            <span class="size">${this.formatSize(system.size_mb * 1024 * 1024)}</span>
                        </div>
                    </div>
                </label>
            `;
        }).join('');

        // Bind checkbox events
        container.querySelectorAll('.system-check').forEach(checkbox => {
            checkbox.addEventListener('change', (e) => {
                e.target.closest('.system-checkbox').classList.toggle('selected', e.target.checked);
                this.updateSystemSelectionStats();
            });
        });

        // Bind label click to toggle selection styling
        container.querySelectorAll('.system-checkbox').forEach(label => {
            label.classList.add('selected'); // Initially all are selected
        });

        // Update stats
        this.updateSystemSelectionStats();
    }

    updateSystemSelectionStats() {
        const checkboxes = document.querySelectorAll('.system-check');
        let selectedCount = 0;
        let totalFiles = 0;
        let totalSizeMb = 0;

        checkboxes.forEach(checkbox => {
            if (checkbox.checked) {
                selectedCount++;
                totalFiles += parseInt(checkbox.dataset.files) || 0;
                totalSizeMb += parseFloat(checkbox.dataset.size) || 0;
            }
        });

        document.getElementById('selected-systems-count').textContent = selectedCount;
        document.getElementById('selected-files-count').textContent = totalFiles.toLocaleString();
        document.getElementById('selected-size').textContent = this.formatSize(totalSizeMb * 1024 * 1024);
    }

    selectAllSystems() {
        document.querySelectorAll('.system-check').forEach(checkbox => {
            checkbox.checked = true;
            checkbox.closest('.system-checkbox').classList.add('selected');
        });
        this.updateSystemSelectionStats();
    }

    deselectAllSystems() {
        document.querySelectorAll('.system-check').forEach(checkbox => {
            checkbox.checked = false;
            checkbox.closest('.system-checkbox').classList.remove('selected');
        });
        this.updateSystemSelectionStats();
    }

    getSelectedSystemPaths() {
        const selectedPaths = [];
        document.querySelectorAll('.system-check:checked').forEach(checkbox => {
            selectedPaths.push(checkbox.dataset.path);
        });
        return selectedPaths;
    }

    formatSize(bytes) {
        if (bytes === 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
    }

    formatDate(dateStr) {
        if (!dateStr) return '';
        try {
            const date = new Date(dateStr);
            const now = new Date();
            const diffMs = now - date;
            const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

            if (diffDays === 0) {
                return 'Today';
            } else if (diffDays === 1) {
                return 'Yesterday';
            } else if (diffDays < 7) {
                return `${diffDays} days ago`;
            } else if (diffDays < 30) {
                const weeks = Math.floor(diffDays / 7);
                return `${weeks} week${weeks > 1 ? 's' : ''} ago`;
            } else {
                return date.toLocaleDateString();
            }
        } catch {
            return dateStr;
        }
    }

    // ===== Library Management =====

    async loadLibraries() {
        try {
            const response = await this.api('/api/libraries');
            this.libraries = response.libraries || [];

            // If no libraries, get/create default
            if (this.libraries.length === 0) {
                const defaultLib = await this.api('/api/libraries/default');
                this.libraries = [defaultLib];
            }

            this.updateLibrarySelector();

            // Select saved library or first one
            if (!this.currentLibraryId && this.libraries.length > 0) {
                this.currentLibraryId = this.libraries[0].library_id;
                localStorage.setItem('duper_current_library', this.currentLibraryId);
            }
        } catch (e) {
            console.error('Failed to load libraries:', e);
        }
    }

    updateLibrarySelector() {
        const select = document.getElementById('library-select');
        if (!select) return;

        select.innerHTML = this.libraries.map(lib =>
            `<option value="${lib.library_id}" ${lib.library_id === this.currentLibraryId ? 'selected' : ''}>
                ${lib.name} ${lib.device_type === 'remote' ? '(remote)' : ''}
            </option>`
        ).join('');
    }

    switchLibrary(libraryId) {
        this.currentLibraryId = libraryId;
        localStorage.setItem('duper_current_library', libraryId);
        this.showToast('Library Changed', `Switched to ${this.getLibraryName(libraryId)}`, 'info');

        // Reload data for new library
        this.loadDashboard();
        if (document.getElementById('games-view').classList.contains('active')) {
            this.loadGames();
        }
    }

    getLibraryName(libraryId) {
        const lib = this.libraries.find(l => l.library_id === libraryId);
        return lib ? lib.name : 'Unknown';
    }

    showNewLibraryModal() {
        document.getElementById('new-library-modal').classList.remove('hidden');
        document.getElementById('new-library-name').value = '';
        document.getElementById('new-library-path').value = '';
        document.getElementById('new-library-type').value = 'local';
    }

    hideNewLibraryModal() {
        document.getElementById('new-library-modal').classList.add('hidden');
    }

    async createLibrary() {
        const name = document.getElementById('new-library-name').value.trim();
        const rootPath = document.getElementById('new-library-path').value.trim();
        const deviceType = document.getElementById('new-library-type').value;

        if (!name || !rootPath) {
            this.showToast('Error', 'Please fill in all fields', 'error');
            return;
        }

        try {
            const library = await this.api('/api/libraries', {
                method: 'POST',
                body: JSON.stringify({ name, root_path: rootPath, device_type: deviceType })
            });

            this.libraries.push(library);
            this.updateLibrarySelector();
            this.hideNewLibraryModal();
            this.showToast('Library Created', `Created "${library.name}"`, 'success');

            // Switch to new library
            this.switchLibrary(library.library_id);
        } catch (e) {
            this.showToast('Error', e.message || 'Failed to create library', 'error');
        }
    }

    // ===== Games View =====

    async loadGames() {
        const grid = document.getElementById('games-grid');
        grid.innerHTML = '<p class="placeholder-text">Loading games...</p>';

        try {
            // Try loading games from library first
            if (this.currentLibraryId) {
                const response = await this.api(`/api/games?library_id=${this.currentLibraryId}`).catch(() => null);
                if (response && response.games && response.games.length > 0) {
                    this.games = response.games;
                    this.updateSystemFilter(response.systems || {});
                    document.getElementById('games-count').textContent = `${response.total} games`;
                    this.renderGames(this.games);
                    return;
                }
            }

            // Fallback: build game list from scanned files
            const stats = await this.api('/api/retronas/summary').catch(() => null);
            if (!stats || !stats.systems || stats.systems.length === 0) {
                grid.innerHTML = '<p class="placeholder-text">No games found. Scan a ROM directory first.</p>';
                return;
            }

            // Build pseudo-game entries from file data per system
            const allFiles = [];
            for (const sys of stats.systems) {
                // Fetch files for each system directory
                const filesResp = await this.api(`/api/files?directory=${encodeURIComponent(sys.system)}&limit=5000`).catch(() => null);
                if (filesResp && filesResp.files) {
                    for (const f of filesResp.files) {
                        allFiles.push({
                            game_id: f.rom_serial || f.md5,
                            title: f.filename.replace(/\.[^.]+$/, ''),
                            filename: f.filename,
                            system: sys.system,
                            filepath: f.filepath,
                            total_size_mb: f.size_mb,
                            ra_supported: f.ra_supported === true || f.ra_supported === 1,
                            ra_game_id: f.ra_game_id,
                            ra_game_title: f.ra_game_title,
                            md5: f.md5,
                            extension: f.extension,
                            cover_url: f.filename ? this._gameImageUrl(sys.system, f.filename) : null,
                            file_count: 1,
                        });
                    }
                }
            }

            if (allFiles.length === 0) {
                // Final fallback: just get all files from the stats
                const filesResp = await this.api('/api/files?limit=5000').catch(() => null);
                if (filesResp && filesResp.files) {
                    for (const f of filesResp.files) {
                        const parts = f.filepath.split('/');
                        const system = parts.length > 2 ? parts[parts.length - 2] : 'unknown';
                        allFiles.push({
                            game_id: f.rom_serial || f.md5,
                            title: f.filename.replace(/\.[^.]+$/, ''),
                            filename: f.filename,
                            system: system,
                            filepath: f.filepath,
                            total_size_mb: f.size_mb,
                            ra_supported: f.ra_supported === true || f.ra_supported === 1,
                            ra_game_id: f.ra_game_id,
                            ra_game_title: f.ra_game_title,
                            md5: f.md5,
                            extension: f.extension,
                            cover_url: f.filename ? this._gameImageUrl(system, f.filename) : null,
                            file_count: 1,
                        });
                    }
                }
            }

            this.games = allFiles;

            // Build system filter from files
            const systemCounts = {};
            for (const g of allFiles) {
                systemCounts[g.system] = (systemCounts[g.system] || 0) + 1;
            }
            this.updateSystemFilter(systemCounts);
            document.getElementById('games-count').textContent = `${allFiles.length} games`;

            // Restore persisted system filter
            const savedFilter = this._getNavState('games_system_filter', '');
            if (savedFilter) {
                const filterEl = document.getElementById('games-system-filter');
                if (filterEl) filterEl.value = savedFilter;
                this.filterGamesBySystem(savedFilter);
            } else {
                this.renderGames(allFiles);
            }
        } catch (e) {
            grid.innerHTML = '<p class="placeholder-text">Failed to load games</p>';
            console.error('Failed to load games:', e);
        }
    }

    updateSystemFilter(systems) {
        const select = document.getElementById('games-system-filter');
        if (!select) return;

        select.innerHTML = '<option value="">All Systems</option>' +
            Object.entries(systems)
                .sort(([, a], [, b]) => b - a)
                .map(([system, count]) => `<option value="${system}">${system} (${count})</option>`)
                .join('');
    }

    filterGamesBySystem(system) {
        this.gamesFilter = system;
        this._saveNavState('games_system_filter', system);
        const filtered = system
            ? this.games.filter(g => g.system === system)
            : this.games;
        this.renderGames(filtered);
    }

    sortGames(sortBy) {
        let sorted = [...this.games];
        switch (sortBy) {
            case 'name':
                sorted.sort((a, b) => a.title.localeCompare(b.title));
                break;
            case 'system':
                sorted.sort((a, b) => a.system.localeCompare(b.system) || a.title.localeCompare(b.title));
                break;
            case 'ra':
                sorted.sort((a, b) => (b.ra_supported ? 1 : 0) - (a.ra_supported ? 1 : 0) || a.title.localeCompare(b.title));
                break;
        }
        this.renderGames(sorted);
    }

    searchGames(query) {
        const q = query.toLowerCase();
        const filtered = this.games.filter(g =>
            g.title.toLowerCase().includes(q) ||
            g.system.toLowerCase().includes(q)
        );
        this.renderGames(filtered);
    }

    renderGames(games) {
        const grid = document.getElementById('games-grid');

        if (games.length === 0) {
            grid.innerHTML = '<p class="placeholder-text">No games found</p>';
            return;
        }

        // System colors for placeholder backgrounds
        const systemColors = {
            'NES': '#c0392b', 'SNES': '#6c3483', 'N64': '#1e8449', 'GameCube': '#6c3483',
            'Game Boy': '#2c3e50', 'Game Boy Color': '#f39c12', 'Game Boy Advance': '#2980b9',
            'Nintendo DS': '#7f8c8d', 'Nintendo 3DS': '#e74c3c',
            'PlayStation': '#2c3e50', 'PlayStation 2': '#2980b9', 'PSP': '#34495e',
            'Genesis': '#2c3e50', 'Master System': '#e74c3c', 'Game Gear': '#3498db',
            'Saturn': '#7f8c8d', 'Dreamcast': '#f39c12',
            'Arcade': '#e67e22', 'MAME': '#d35400', 'Neo Geo': '#c0392b',
            'default': '#c9952e'
        };

        grid.innerHTML = games.map((game, idx) => {
            const sysColor = systemColors[game.system] || systemColors['default'];
            const initials = game.title.split(' ').slice(0, 2).map(w => w[0] || '').join('').toUpperCase();
            const playcount = game.playcount || 0;
            const animDelay = Math.min(idx * 30, 600);

            return `
                <div class="game-card" data-game-id="${game.game_id}" style="animation-delay:${animDelay}ms">
                    <div class="game-card-cover" data-filepath="${game.filepath || ''}" data-system="${game.system}" data-filename="${game.filename || ''}" data-game-id="${game.game_id}" style="background: linear-gradient(135deg, ${sysColor}22, #0c0a08);">
                        ${game.cover_url
                            ? `<img src="${game.cover_url}" alt="${game.title}" loading="lazy" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                               <span class="no-cover" style="display:none; color: ${sysColor};">${initials || '?'}</span>`
                            : `<span class="no-cover" style="color: ${sysColor};">${initials || '?'}</span>`
                        }
                        ${game.ra_supported ? `<div class="ra-stamp" title="RetroAchievements Supported">
                            <svg viewBox="0 0 24 24" width="28" height="28" fill="none" stroke-width="1.5">
                                <circle cx="12" cy="12" r="10" stroke="rgba(255,199,44,0.6)" fill="rgba(255,199,44,0.15)"/>
                                <path d="M12 6l1.5 3.5L17 10l-2.5 2.5.5 3.5-3-1.5-3 1.5.5-3.5L7 10l3.5-.5z" fill="rgba(255,199,44,0.7)" stroke="rgba(255,199,44,0.9)"/>
                            </svg>
                        </div>` : ''}
                    </div>
                    <div class="game-card-info">
                        <div class="game-card-title" title="${game.title}">${this._escHtml(game.title)}</div>
                        <div class="game-card-system">${this._escHtml(game.system)}</div>
                        <div class="game-card-badges">
                            ${game.ra_supported && game.ra_game_id ? `<div class="ra-cheevos-row" data-ra-id="${game.ra_game_id}" data-ra-loaded="false">
                                <span class="game-badge ra-cheevos-count" title="Loading...">-- cheevos</span>
                            </div>` : ''}
                            ${playcount > 0 ? '<span class="game-badge played">' + playcount + 'x</span>' : ''}
                        </div>
                    </div>
                </div>
            `;
        }).join('');

        // Bind card click for detail overlay
        grid.querySelectorAll('.game-card').forEach(card => {
            card.addEventListener('click', () => {
                const gameId = card.dataset.gameId;
                this.showGameDetails(gameId);
            });
        });

        // Attempt to load box art from media API for cards without covers
        this.loadGameCardArt(grid);

        // Lazy-load RA cheevos data as cards scroll into view
        this._loadRACheevosLazy(grid);
    }

    _loadRACheevosLazy(container) {
        if (!this._raCheevosCache) this._raCheevosCache = new Map();

        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (!entry.isIntersecting) return;
                const row = entry.target;
                const raId = row.dataset.raId;
                if (!raId || row.dataset.raLoaded === 'true') return;
                row.dataset.raLoaded = 'true';
                observer.unobserve(row);
                this._fetchRACheevos(raId, row);
            });
        }, { rootMargin: '200px' });

        container.querySelectorAll('.ra-cheevos-row[data-ra-id]').forEach(el => {
            const raId = el.dataset.raId;
            if (this._raCheevosCache.has(raId)) {
                this._renderRACheevosRow(el, this._raCheevosCache.get(raId));
                el.dataset.raLoaded = 'true';
            } else {
                observer.observe(el);
            }
        });
    }

    async _fetchRACheevos(raId, row) {
        try {
            const data = await this.api('/api/dashboard/ra-game-progress?game_id=' + raId).catch(() => null);
            if (data && !data.error) {
                this._raCheevosCache.set(raId, data);
                this._renderRACheevosRow(row, data);
            } else {
                row.innerHTML = '<span class="game-badge ra-cheevos-count">RA #' + raId + '</span>';
            }
        } catch (e) {
            row.innerHTML = '<span class="game-badge ra-cheevos-count">RA</span>';
        }
    }

    _renderRACheevosRow(row, data) {
        const earned = data.earned || 0;
        const total = data.total || 0;
        const pct = total > 0 ? Math.round(earned / total * 100) : 0;
        const rarest = data.rarest_achievement;
        const latest = data.latest_achievement;

        let html = '<span class="game-badge ra-cheevos-count" title="' + earned + '/' + total + ' achievements">' + earned + '/' + total + ' cheevos</span>';

        if (latest) {
            html += '<span class="game-badge ra-latest" title="Latest: ' + (latest.title || '') + '">' + (latest.title || '').substring(0, 20) + '</span>';
        }
        if (rarest && rarest.title !== (latest && latest.title)) {
            html += '<span class="game-badge ra-rarest" title="Rarest: ' + (rarest.title || '') + ' (' + (rarest.earn_pct || '?') + '% earned)">' + (rarest.title || '').substring(0, 18) + ' (' + (rarest.earn_pct || '?') + '%)</span>';
        }

        row.innerHTML = html;
    }

    async loadGameCardArt(container) {
        const coverEls = container.querySelectorAll('.game-card-cover[data-filepath]');
        const batchSize = 6;
        const elements = Array.from(coverEls).filter(el => {
            const fp = el.dataset.filepath;
            return fp && fp.length > 0 && !el.querySelector('img[src]:not([style*="display:none"])');
        });

        for (let i = 0; i < elements.length; i += batchSize) {
            const batch = elements.slice(i, i + batchSize);
            await Promise.all(batch.map(async (coverEl) => {
                const filepath = coverEl.dataset.filepath;
                if (!filepath) return;

                try {
                    if (this.coverArtCache.has(filepath)) {
                        const cached = this.coverArtCache.get(filepath);
                        if (cached && cached.src) {
                            coverEl.innerHTML = `<img src="${cached.src}" alt="Cover" loading="lazy">`;
                        }
                        return;
                    }

                    const coverData = await this.api(`/api/media/cover-data${filepath}`);
                    if (coverData.has_cover && coverData.data) {
                        const imgSrc = `data:${coverData.mime_type};base64,${coverData.data}`;
                        this.coverArtCache.set(filepath, { src: imgSrc });
                        coverEl.innerHTML = `<img src="${imgSrc}" alt="Cover" loading="lazy">`;
                    } else {
                        this.coverArtCache.set(filepath, null);
                    }
                } catch (e) {
                    // Silently fail - placeholder stays
                    this.coverArtCache.set(filepath, null);
                }
            }));
        }
    }

    showGameDetails(gameId) {
        const game = this.games.find(g => g.game_id === gameId);
        if (!game) {
            this.showToast('Game Details', `Game ID: ${gameId}`, 'info');
            return;
        }

        const modal = document.getElementById('game-detail-modal');
        const content = document.getElementById('game-detail-content');
        if (!modal || !content) return;

        // Extract system and filename from filepath
        const filepath = game.filepath || '';
        const parts = filepath.split('/');
        const filename = parts[parts.length - 1] || game.title;
        const system = game.system || (parts.length > 2 ? parts[parts.length - 2] : '');

        // Build cover image URL
        const coverUrl = this._gameImageUrl(system, filename);

        // Render initial skeleton while loading detail
        content.innerHTML = `
            <div class="gd-hero">
                <div class="gd-cover-wrap">
                    <img class="gd-cover-img" src="${coverUrl}" alt="${this._escHtml(game.title)}" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                    <div class="gd-cover-fallback" style="display:none">
                        <span>${game.title.split(' ').slice(0, 2).map(w => w[0] || '').join('').toUpperCase()}</span>
                    </div>
                </div>
                <div class="gd-hero-info">
                    <h2 class="gd-title">${this._escHtml(game.title)}</h2>
                    <div class="gd-badges">
                        <span class="gd-badge gd-badge-system">${this._escHtml(system)}</span>
                        ${game.ra_supported ? '<span class="gd-badge gd-badge-ra"><svg viewBox="0 0 16 16" width="10" height="10" fill="currentColor"><polygon points="8 1 10.2 5.5 15 6.2 11.5 9.5 12.3 14.3 8 12 3.7 14.3 4.5 9.5 1 6.2 5.8 5.5 8 1"/></svg> RA Supported</span>' : ''}
                    </div>
                    <div class="gd-loading-meta">
                        <div class="gd-loading-spinner"></div>
                        <span>Loading game details...</span>
                    </div>
                </div>
            </div>
        `;

        modal.classList.remove('hidden');
        document.body.style.overflow = 'hidden';

        // Close on Escape key
        this._gameDetailEscHandler = (e) => {
            if (e.key === 'Escape') this.closeGameDetail();
        };
        document.addEventListener('keydown', this._gameDetailEscHandler);

        // Fetch full detail from API
        this._loadGameDetail(system, filename, game, content, coverUrl);
    }

    async _loadGameDetail(system, filename, game, content, coverUrl) {
        let detail = null;
        try {
            detail = await this.api(`/api/game-detail/${encodeURIComponent(system)}/${encodeURIComponent(filename)}`);
        } catch (e) {
            // Fallback: render with local data only
        }

        const gl = detail?.gamelist || {};
        const fi = detail?.file || {};
        const ra = detail?.ra_progress || null;

        const title = gl.name || game.title;
        const description = gl.description || '';
        const developer = gl.developer || game.developer || '';
        const publisher = gl.publisher || game.publisher || '';
        const genre = gl.genre || game.genre || '';
        const players = gl.players || '';
        const releaseDate = gl.release_date || '';
        const rating = gl.rating ? parseFloat(gl.rating) : null;
        const playcount = gl.playcount || game.playcount || 0;
        const playtimeMins = gl.playtime_minutes || game.playtime_minutes || 0;
        const lastplayed = gl.lastplayed_iso || gl.lastplayed || game.lastplayed || '';

        // Format playtime
        let playtimeStr = '--';
        if (playtimeMins >= 60) {
            playtimeStr = Math.floor(playtimeMins / 60) + 'h ' + (playtimeMins % 60) + 'm';
        } else if (playtimeMins > 0) {
            playtimeStr = playtimeMins + 'm';
        }

        // Format rating as stars (0-1 scale to 5 stars)
        let starsHtml = '';
        if (rating !== null && rating > 0) {
            const starCount = Math.round(rating * 5);
            for (let i = 0; i < 5; i++) {
                starsHtml += `<svg class="gd-star ${i < starCount ? 'filled' : ''}" viewBox="0 0 24 24" width="14" height="14"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>`;
            }
        }

        // Format release date
        let releaseFmt = '';
        if (releaseDate) {
            try {
                // ES-DE format: 20010205T000000
                const m = releaseDate.match(/^(\d{4})(\d{2})(\d{2})/);
                if (m) {
                    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
                    releaseFmt = `${months[parseInt(m[2])-1] || m[2]} ${parseInt(m[3])}, ${m[1]}`;
                } else {
                    releaseFmt = releaseDate;
                }
            } catch { releaseFmt = releaseDate; }
        }

        // Region badge from filename
        const regionMatch = filename.match(/\(([A-Za-z, ]+)\)/);
        const region = regionMatch ? regionMatch[1] : '';

        // File info
        const md5 = fi.md5 || game.md5 || '';
        const romSerial = fi.rom_serial || game.rom_serial || '';
        const sizeMb = fi.size_mb || game.total_size_mb || game.size_mb || 0;
        const extension = fi.extension || (filename.includes('.') ? filename.split('.').pop() : '');
        const raGameId = fi.ra_game_id || 0;
        const raSupported = fi.ra_supported || game.ra_supported || false;

        // Screenshots URL
        const screenshotUrl = this._gameImageUrl(system, filename) + '?category=screenshots';

        content.innerHTML = `
            <div class="gd-hero">
                <div class="gd-cover-wrap">
                    <img class="gd-cover-img" src="${coverUrl}" alt="${this._escHtml(title)}" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                    <div class="gd-cover-fallback" style="display:none">
                        <span>${title.split(' ').slice(0, 2).map(w => w[0] || '').join('').toUpperCase()}</span>
                    </div>
                </div>
                <div class="gd-hero-info">
                    <h2 class="gd-title">${this._escHtml(title)}</h2>
                    <div class="gd-badges">
                        <span class="gd-badge gd-badge-system">${this._escHtml(system)}</span>
                        ${region ? `<span class="gd-badge gd-badge-region">${this._escHtml(region)}</span>` : ''}
                        ${raSupported ? '<span class="gd-badge gd-badge-ra"><svg viewBox="0 0 16 16" width="10" height="10" fill="currentColor"><polygon points="8 1 10.2 5.5 15 6.2 11.5 9.5 12.3 14.3 8 12 3.7 14.3 4.5 9.5 1 6.2 5.8 5.5 8 1"/></svg> RA</span>' : ''}
                    </div>
                    ${starsHtml ? `<div class="gd-rating">${starsHtml}</div>` : ''}
                </div>
            </div>

            ${(developer || publisher || genre || releaseFmt || players) ? `
            <div class="gd-section gd-metadata">
                <h4>Game Info</h4>
                <div class="gd-meta-grid">
                    ${developer ? `<div class="gd-meta-item"><span class="gd-meta-label">Developer</span><span class="gd-meta-value">${this._escHtml(developer)}</span></div>` : ''}
                    ${publisher ? `<div class="gd-meta-item"><span class="gd-meta-label">Publisher</span><span class="gd-meta-value">${this._escHtml(publisher)}</span></div>` : ''}
                    ${genre ? `<div class="gd-meta-item"><span class="gd-meta-label">Genre</span><span class="gd-meta-value">${this._escHtml(genre)}</span></div>` : ''}
                    ${releaseFmt ? `<div class="gd-meta-item"><span class="gd-meta-label">Released</span><span class="gd-meta-value">${releaseFmt}</span></div>` : ''}
                    ${players ? `<div class="gd-meta-item"><span class="gd-meta-label">Players</span><span class="gd-meta-value">${this._escHtml(players)}</span></div>` : ''}
                </div>
            </div>` : ''}

            ${description ? `
            <div class="gd-section gd-description">
                <h4>Description</h4>
                <p>${this._escHtml(description)}</p>
            </div>` : ''}

            ${(playcount > 0 || playtimeMins > 0) ? `
            <div class="gd-section gd-play-stats">
                <h4>Play Statistics</h4>
                <div class="gd-stats-grid">
                    <div class="gd-stat-item">
                        <span class="gd-stat-value">${playcount}</span>
                        <span class="gd-stat-label">Play Count</span>
                    </div>
                    <div class="gd-stat-item">
                        <span class="gd-stat-value">${playtimeStr}</span>
                        <span class="gd-stat-label">Total Playtime</span>
                    </div>
                    <div class="gd-stat-item">
                        <span class="gd-stat-value">${lastplayed ? this.formatDate(lastplayed) : '--'}</span>
                        <span class="gd-stat-label">Last Played</span>
                    </div>
                </div>
            </div>` : ''}

            ${ra ? `
            <div class="gd-section gd-ra-section">
                <h4>
                    <svg viewBox="0 0 24 24" width="14" height="14" fill="#ffc72c"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>
                    RetroAchievements
                    <a href="${ra.ra_url}" target="_blank" rel="noopener" class="gd-ra-ext-link" title="View on RetroAchievements.org">
                        <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                    </a>
                </h4>
                <div class="gd-ra-progress">
                    <div class="gd-ra-summary">
                        <div class="gd-ra-stat">
                            <span class="gd-ra-stat-val">${ra.achievements_earned}/${ra.achievements_total}</span>
                            <span class="gd-ra-stat-lbl">Achievements</span>
                        </div>
                        <div class="gd-ra-stat">
                            <span class="gd-ra-stat-val">${ra.points_earned}/${ra.points_possible}</span>
                            <span class="gd-ra-stat-lbl">Points</span>
                        </div>
                        <div class="gd-ra-stat">
                            <span class="gd-ra-stat-val">${ra.completion_pct}%</span>
                            <span class="gd-ra-stat-lbl">Completion</span>
                        </div>
                    </div>
                    <div class="gd-ra-bar-wrap">
                        <div class="gd-ra-bar" style="width:${ra.completion_pct}%"></div>
                    </div>
                    ${ra.earned_achievements && ra.earned_achievements.length > 0 ? `
                    <div class="gd-ra-badges">
                        ${ra.earned_achievements.slice(0, 12).map(a => `
                            <div class="gd-ra-badge-item" title="${this._escHtml(a.title)}: ${this._escHtml(a.description)} (+${a.points} pts)">
                                ${a.badge_url ? `<img src="${a.badge_url}" alt="${this._escHtml(a.title)}" loading="lazy">` : '<div class="gd-ra-badge-placeholder"></div>'}
                            </div>
                        `).join('')}
                        ${ra.earned_achievements.length > 12 ? `<div class="gd-ra-badge-more">+${ra.earned_achievements.length - 12}</div>` : ''}
                    </div>` : ''}
                </div>
            </div>` : (raSupported && raGameId ? `
            <div class="gd-section gd-ra-section">
                <h4>
                    <svg viewBox="0 0 24 24" width="14" height="14" fill="#ffc72c"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>
                    RetroAchievements
                </h4>
                <div class="gd-ra-supported-badge">
                    <span class="gd-badge gd-badge-ra">RA Supported</span>
                    <a href="https://retroachievements.org/game/${raGameId}" target="_blank" rel="noopener" class="gd-ra-ext-link">Game ID: ${raGameId}</a>
                </div>
            </div>` : '')}

            <div class="gd-section gd-screenshots" id="gd-screenshots">
                <img class="gd-screenshot-probe" src="${screenshotUrl}" alt="" loading="lazy"
                    onload="this.parentElement.classList.add('has-screenshot'); this.className='gd-screenshot-img';"
                    onerror="this.parentElement.style.display='none';">
                <h4 style="display:none">Screenshot</h4>
            </div>

            <div class="gd-section gd-file-info">
                <h4>File Information</h4>
                <div class="gd-file-grid">
                    <div class="gd-file-row"><span class="gd-file-label">Filename</span><span class="gd-file-value">${this._escHtml(filename)}</span></div>
                    ${extension ? `<div class="gd-file-row"><span class="gd-file-label">Format</span><span class="gd-file-value">.${this._escHtml(extension.toUpperCase())}</span></div>` : ''}
                    ${sizeMb ? `<div class="gd-file-row"><span class="gd-file-label">Size</span><span class="gd-file-value">${this.formatSize(sizeMb * 1024 * 1024)}</span></div>` : ''}
                    ${md5 ? `<div class="gd-file-row"><span class="gd-file-label">MD5</span><span class="gd-file-value gd-monospace">${md5}</span></div>` : ''}
                    ${romSerial ? `<div class="gd-file-row"><span class="gd-file-label">Serial</span><span class="gd-file-value gd-monospace">${this._escHtml(romSerial)}</span></div>` : ''}
                </div>
            </div>
        `;

        // Handle screenshot section visibility
        const ssSection = content.querySelector('.gd-screenshots.has-screenshot');
        if (ssSection) {
            ssSection.querySelector('h4').style.display = '';
        }
    }

    closeGameDetail() {
        const modal = document.getElementById('game-detail-modal');
        if (modal) {
            modal.classList.add('hidden');
            document.body.style.overflow = '';
        }
        if (this._gameDetailEscHandler) {
            document.removeEventListener('keydown', this._gameDetailEscHandler);
            this._gameDetailEscHandler = null;
        }
    }

    // ===== RetroNAS Dashboard =====

    async loadRetroNAS() {
        try {
            const summary = await this.api('/api/retronas/summary').catch(() => null);

            if (summary) {
                document.getElementById('retronas-total-games').textContent =
                    summary.total_files ? summary.total_files.toLocaleString() : '-';

                document.getElementById('retronas-total-size').textContent =
                    summary.total_size_mb ? this.formatSize(summary.total_size_mb * 1024 * 1024) : '-';

                const systemCount = summary.systems?.length || summary.total_systems || '-';
                document.getElementById('retronas-systems-count').textContent =
                    typeof systemCount === 'number' ? systemCount.toLocaleString() : systemCount;

                document.getElementById('retronas-ra-verified').textContent =
                    summary.ra_supported ? summary.ra_supported.toLocaleString() : '0';

                this.renderRetroNASSystems(summary);
                this.renderRetroNASStorage(summary);
            }

            // Transfer status handled by global poll loop
        } catch (e) {
            console.error('Failed to load RetroNAS data:', e);
        }
    }

    getSystemIcon(system) {
        const icons = {
            nes: { color: '#c0392b', abbr: 'NES', svg: '<path d="M2 8h20v8H2z M6 6h4v2H6z M14 6h4v2h-4z M4 10h2v4H4z M18 10h2v4h-2z"/>' },
            snes: { color: '#6c3483', abbr: 'SNES', svg: '<path d="M3 7h18v10H3z M7 5h2v2H7z M15 5h2v2h-2z M6 10a1.5 1.5 0 103 0 1.5 1.5 0 10-3 0z M15 10a1.5 1.5 0 103 0 1.5 1.5 0 10-3 0z"/>' },
            n64: { color: '#1e8449', abbr: 'N64', svg: '<path d="M12 3L2 9v6l10 6 10-6V9z M12 3v18 M2 9l10 6 10-6"/>' },
            gba: { color: '#2980b9', abbr: 'GBA', svg: '<path d="M3 7h18v10H3z M1 9h3v6H1z M20 9h3v6h-3z M7 10h3v1H7z M14 10.5a1.5 1.5 0 103 0 1.5 1.5 0 10-3 0z"/>' },
            gb: { color: '#7d8c5c', abbr: 'GB', svg: '<path d="M6 2h12v20H6z M8 4h8v7H8z M10 14h1v2h-1z M13 13h1v1h-1z M15 14h1v1h-1z M13 15h1v1h-1z"/>' },
            gbc: { color: '#8e44ad', abbr: 'GBC', svg: '<path d="M6 2h12v20H6z M8 4h8v7H8z M10 14h1v2h-1z M13 13h1v1h-1z M15 14h1v1h-1z M13 15h1v1h-1z"/>' },
            nds: { color: '#2c3e50', abbr: 'NDS', svg: '<path d="M5 1h14v10H5z M5 13h14v10H5z M5 11h14v2H5z M10 7a2 2 0 104 0 2 2 0 10-4 0z"/>' },
            gc: { color: '#5b2c8e', abbr: 'GCN', svg: '<circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="4" fill="none" stroke="currentColor"/>' },
            wii: { color: '#3498db', abbr: 'Wii', svg: '<rect x="8" y="2" width="8" height="20" rx="4"/><circle cx="12" cy="7" r="1.5"/>' },
            wiiu: { color: '#00a9e0', abbr: 'WiiU', svg: '<rect x="3" y="6" width="18" height="12" rx="2"/><rect x="5" y="8" width="10" height="8" rx="1"/>' },
            psx: { color: '#1a5276', abbr: 'PS1', svg: '<path d="M3 6l9-3v18l-9-3z M12 3l9 3v12l-9 3z"/>' },
            ps2: { color: '#2471a3', abbr: 'PS2', svg: '<path d="M3 6l9-3v18l-9-3z M12 3l9 3v12l-9 3z"/><line x1="3" y1="12" x2="21" y2="12"/>' },
            ps3: { color: '#1a1a2e', abbr: 'PS3', svg: '<path d="M3 6l9-3v18l-9-3z M12 3l9 3v12l-9 3z"/><circle cx="12" cy="12" r="2"/>' },
            psp: { color: '#34495e', abbr: 'PSP', svg: '<rect x="1" y="7" width="22" height="10" rx="5"/><circle cx="6" cy="12" r="2"/><rect x="13" y="10" width="6" height="4" rx="1"/>' },
            xbox: { color: '#107c10', abbr: 'XBOX', svg: '<circle cx="12" cy="12" r="10"/><path d="M12 2C8 6 6 10 6 12s2 6 6 10c4-4 6-8 6-10s-2-6-6-10z"/>' },
            xbox360: { color: '#52b043', abbr: '360', svg: '<circle cx="12" cy="12" r="10"/><path d="M7 7c3 3 3 7 0 10 M17 7c-3 3-3 7 0 10"/>' },
            genesis: { color: '#1c1c1c', abbr: 'GEN', svg: '<path d="M2 8h20v8H2z M4 6h6v2H4z M14 6h6v2h-6z"/>' },
            megadrive: { color: '#1c1c1c', abbr: 'MD', svg: '<path d="M2 8h20v8H2z M4 6h6v2H4z M14 6h6v2h-6z"/>' },
            saturn: { color: '#7f8c8d', abbr: 'SAT', svg: '<circle cx="12" cy="12" r="9"/><ellipse cx="12" cy="12" rx="9" ry="3" transform="rotate(-20 12 12)"/>' },
            dreamcast: { color: '#f39c12', abbr: 'DC', svg: '<circle cx="12" cy="12" r="9"/><path d="M12 3a9 9 0 010 18" fill="none" stroke="currentColor" stroke-width="2"/>' },
            neogeo: { color: '#d4a017', abbr: 'NEO', svg: '<rect x="3" y="3" width="18" height="18" rx="2"/><rect x="6" y="6" width="5" height="12"/><circle cx="16" cy="12" r="3"/>' },
            neogeocd: { color: '#c9952e', abbr: 'NCD', svg: '<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="3"/>' },
            mame: { color: '#e74c3c', abbr: 'ARC', svg: '<rect x="6" y="2" width="12" height="20" rx="2"/><rect x="8" y="4" width="8" height="6" rx="1"/><circle cx="10" cy="15" r="1.5"/><circle cx="14" cy="13" r="1.5"/>' },
            arcade: { color: '#e74c3c', abbr: 'ARC', svg: '<rect x="6" y="2" width="12" height="20" rx="2"/><rect x="8" y="4" width="8" height="6" rx="1"/><circle cx="10" cy="15" r="1.5"/><circle cx="14" cy="13" r="1.5"/>' },
            dos: { color: '#2c3e50', abbr: 'DOS', svg: '<rect x="3" y="5" width="18" height="14" rx="1"/><path d="M5 9h5 M5 12h8 M5 15h3"/>' },
            msx: { color: '#c0392b', abbr: 'MSX', svg: '<rect x="2" y="10" width="20" height="8" rx="1"/><rect x="4" y="12" width="3" height="2"/><rect x="8" y="12" width="3" height="2"/>' },
            ngp: { color: '#27ae60', abbr: 'NGP', svg: '<rect x="5" y="2" width="14" height="20" rx="3"/><rect x="7" y="4" width="10" height="8" rx="1"/>' },
            model2: { color: '#8e44ad', abbr: 'MD2', svg: '<rect x="3" y="6" width="18" height="12" rx="2"/><path d="M8 12h8 M12 8v8"/>' },
        };
        const key = system.toLowerCase().replace(/[^a-z0-9]/g, '');
        return icons[key] || { color: '#546478', abbr: system.substring(0, 3).toUpperCase(), svg: '<rect x="3" y="5" width="18" height="14" rx="2"/><path d="M7 9h4v2H7z M13 9h4v2h-4z M10 13h4v2h-4z"/>' };
    }

    renderRetroNASSystems(summary) {
        const container = document.getElementById('retronas-systems-grid');
        if (!container) return;

        const systems = summary?.systems || [];

        if (systems.length > 0) {
            container.innerHTML = systems.map(sys => {
                const icon = this.getSystemIcon(sys.system);
                const raPercent = sys.ra_total > 0
                    ? Math.round((sys.ra_supported / sys.ra_total) * 100) : 0;
                const topGames = (sys.top_games || []).slice(0, 5);

                return `
                    <div class="retronas-system-card" style="--sys-color: ${icon.color}">
                        <div class="retronas-system-header">
                            <div class="retronas-system-icon" style="color: ${icon.color}">
                                <img src="img/systems/${sys.system}.png" class="system-logo-img" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';" alt="${sys.system}">
                                <div class="fallback-icon" style="display:none;"><svg viewBox="0 0 24 24" width="40" height="40" fill="currentColor" stroke="currentColor" stroke-width="0.5">${icon.svg}</svg></div>
                            </div>
                            <div class="retronas-system-info">
                                <div class="retronas-system-name">${sys.system}</div>
                                <div class="retronas-system-meta">
                                    <span class="retronas-system-count">${sys.file_count.toLocaleString()} games</span>
                                    <span class="retronas-system-size">${this.formatSize(sys.total_size_mb * 1024 * 1024)}</span>
                                </div>
                                ${raPercent > 0 ? `
                                    <div class="retronas-system-ra-bar">
                                        <div class="retronas-system-ra-fill" style="width: ${raPercent}%"></div>
                                        <span class="retronas-system-ra-label">RA ${raPercent}%</span>
                                    </div>
                                ` : ''}
                            </div>
                        </div>
                        ${topGames.length > 0 ? `
                            <div class="retronas-system-top-games">
                                <div class="retronas-top-label">Top Games</div>
                                <ol class="retronas-top-list">
                                    ${topGames.map(g => `<li title="${g}">${g}</li>`).join('')}
                                </ol>
                            </div>
                        ` : ''}
                    </div>
                `;
            }).join('');
        } else {
            container.innerHTML = '<p class="placeholder-text">No system data available. Scan a ROM directory first.</p>';
        }
    }

    _renderLiveSystems(systems) {
        const container = document.getElementById('retronas-systems-grid');
        if (!container) return;

        container.innerHTML = systems.map(sys => {
            const icon = this.getSystemIcon(sys.system);
            return `
                <div class="retronas-system-card" style="--sys-color: ${icon.color}">
                    <div class="retronas-system-header">
                        <div class="retronas-system-icon" style="color: ${icon.color}">
                            <img src="img/systems/${sys.system}.png" class="system-logo-img" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';" alt="${sys.system}">
                            <div class="fallback-icon" style="display:none;"><svg viewBox="0 0 24 24" width="40" height="40" fill="currentColor" stroke="currentColor" stroke-width="0.5">${icon.svg}</svg></div>
                        </div>
                        <div class="retronas-system-info">
                            <div class="retronas-system-name">${sys.system}</div>
                            <div class="retronas-system-meta">
                                <span class="retronas-system-count">${sys.file_count.toLocaleString()} files</span>
                                <span class="retronas-system-size">${this.formatSize(sys.total_size_mb * 1024 * 1024)}</span>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }).join('');
    }

    renderRetroNASStorage(stats) {
        if (!stats) return;

        const totalMb = stats.total_size_mb || 0;
        const totalBytes = totalMb * 1024 * 1024;

        // Use system info for disk space if available, otherwise estimate
        const diskTotal = stats.system_info?.disk_total_gb
            ? stats.system_info.disk_total_gb * 1024 * 1024 * 1024
            : totalBytes * 4; // fallback: estimate 25% usage
        const diskFree = stats.system_info?.disk_free_gb
            ? stats.system_info.disk_free_gb * 1024 * 1024 * 1024
            : diskTotal - totalBytes;

        const usedBytes = diskTotal - diskFree;
        const usedPercent = diskTotal > 0 ? (usedBytes / diskTotal) * 100 : 0;

        document.getElementById('retronas-storage-used').textContent = `${this.formatSize(usedBytes)} used`;
        document.getElementById('retronas-storage-total').textContent = `${this.formatSize(diskTotal)} total`;
        document.getElementById('retronas-storage-free').textContent = `${this.formatSize(diskFree)} free`;
        document.getElementById('retronas-storage-percent').textContent = `${usedPercent.toFixed(1)}%`;

        const fill = document.getElementById('retronas-storage-fill');
        fill.style.width = `${Math.min(usedPercent, 100)}%`;
        fill.classList.remove('warning', 'critical');
        if (usedPercent > 90) {
            fill.classList.add('critical');
        } else if (usedPercent > 75) {
            fill.classList.add('warning');
        }
    }

    // ===== Transfer Progress =====

    async startTransfer() {
        try {
            const result = await this.api('/api/retronas/transfer/start', { method: 'POST' });
            if (result.error) {
                alert(result.error);
                return;
            }
            // Global poll loop handles UI updates automatically
        } catch (e) {
            alert('Failed to start transfer: ' + e.message);
        }
    }

    async cancelTransfer() {
        if (confirm('Cancel the transfer?')) {
            await this.api('/api/retronas/transfer/cancel', { method: 'POST' });
        }
    }

    async startMediaTransfer() {
        try {
            const result = await this.api('/api/retronas/media-transfer/start', { method: 'POST' });
            if (result.error) alert(result.error);
        } catch (e) {
            alert('Failed to start media transfer: ' + e.message);
        }
    }

    async cancelMediaTransfer() {
        if (confirm('Cancel media transfer?')) {
            await this.api('/api/retronas/media-transfer/cancel', { method: 'POST' });
        }
    }

    _updateMediaTransferUI(m) {
        const opsCard = document.getElementById('ops-media-transfer');
        const startBtn = document.getElementById('ops-media-start-btn');
        const cancelBtn = document.getElementById('ops-media-cancel-btn');
        const body = document.getElementById('ops-media-transfer-body');

        if (m.active || m.current_system === 'COMPLETE') {
            body?.classList.remove('collapsed');
            opsCard?.classList.add('active');
            startBtn?.classList.add('hidden');
            cancelBtn?.classList.remove('hidden');

            const pct = m.total_bytes > 0 ? (m.transferred_bytes / m.total_bytes * 100) : 0;
            this._setWidth('ops-media-fill', pct.toFixed(1));
            this._setWidth('ops-media-mini-fill', pct.toFixed(1));
            this._setText('ops-media-pct', `${pct.toFixed(1)}%`);
            this._setText('ops-media-file', m.current_file ? m.current_file.split('/').pop() : '-');
            this._setText('ops-media-files', `${m.transferred_files} / ${m.total_files}`);
            const mSkipped = m.skipped_files || 0;
            this._setText('ops-media-skipped', mSkipped > 0 ? `${mSkipped.toLocaleString()} already on NAS` : '-');
            this._setText('ops-media-size', `${this.formatSize(m.transferred_bytes)} / ${this.formatSize(m.total_bytes)}`);
            this._setText('ops-media-speed', m.speed_bps > 0 ? `${this.formatSize(m.speed_bps)}/s` : '- /s');
            if (m.eta_seconds > 0 && m.eta_seconds < 999999) {
                const h = Math.floor(m.eta_seconds / 3600);
                const mn = Math.floor((m.eta_seconds % 3600) / 60);
                this._setText('ops-media-eta', h > 0 ? `${h}h ${mn}m` : `${mn}m`);
            } else {
                this._setText('ops-media-eta', m.current_system === 'COMPLETE' ? 'Done!' : '-');
            }

            const doneKey = (m.systems_done || []).join(',');
            if (this._lastMediaDoneKey !== doneKey) {
                this._lastMediaDoneKey = doneKey;
                const doneEl = document.getElementById('ops-media-systems-done');
                const remEl = document.getElementById('ops-media-systems-remaining');
                if (doneEl) doneEl.innerHTML = (m.systems_done || []).map(s => `<span>${s}</span>`).join('');
                if (remEl) remEl.innerHTML = (m.systems_remaining || []).map(s => `<span>${s}</span>`).join('');
            }

            if (!m.active && m.current_system === 'COMPLETE') {
                cancelBtn?.classList.add('hidden');
                startBtn?.classList.remove('hidden');
                opsCard?.classList.remove('active');
                opsCard?.classList.add('completed');
            }
        } else {
            opsCard?.classList.remove('active');
            startBtn?.classList.remove('hidden');
            cancelBtn?.classList.add('hidden');
        }
    }

    async pollTransfer() {
        // Legacy — now handled by _pollRetroNAS via global poll loop
        const t = await this.api('/api/retronas/transfer').catch(() => null);
        if (t) this._updateTransferUI(t);
    }

    // ===== Scan Queue =====

    async loadQueue() {
        try {
            const response = await this.api('/api/queue?include_completed=true');
            this.renderQueue(response);
        } catch (e) {
            console.error('Failed to load queue:', e);
        }
    }

    renderQueue(queue) {
        // Running
        const runningEl = document.getElementById('queue-running');
        if (queue.running) {
            runningEl.innerHTML = this.renderQueueItem(queue.running, true);
        } else {
            runningEl.innerHTML = '<p class="placeholder-text">No scan running</p>';
        }

        // Pending
        const pendingEl = document.getElementById('queue-pending');
        if (queue.pending && queue.pending.length > 0) {
            pendingEl.innerHTML = queue.pending.map((item, i) =>
                this.renderQueueItem(item, false, i + 1)
            ).join('');
        } else {
            pendingEl.innerHTML = '<p class="placeholder-text">Queue is empty</p>';
        }

        // Completed
        const completedEl = document.getElementById('queue-completed');
        if (queue.completed && queue.completed.length > 0) {
            completedEl.innerHTML = queue.completed.slice(0, 10).map(item =>
                this.renderQueueItem(item, false, null, true)
            ).join('');
        } else {
            completedEl.innerHTML = '<p class="placeholder-text">No completed scans</p>';
        }
    }

    renderQueueItem(item, isRunning, position = null, isCompleted = false) {
        const statusClass = isRunning ? 'running' : (item.status === 'failed' ? 'failed' : (isCompleted ? 'completed' : ''));
        const dirName = item.directory.split('/').pop() || item.directory;

        return `
            <div class="queue-item ${statusClass}">
                ${position ? `<div class="queue-item-position">${position}</div>` : ''}
                <div class="queue-item-info">
                    <div class="queue-item-directory" title="${item.directory}">${dirName}</div>
                    <div class="queue-item-status">
                        ${isRunning ? `Processing: ${item.current_file || 'Starting...'}` :
                          isCompleted ? `${item.files_processed} files processed${item.errors > 0 ? `, ${item.errors} errors` : ''}` :
                          'Pending'}
                    </div>
                </div>
                ${isRunning ? `
                    <div class="queue-item-progress">
                        <div class="queue-item-progress-bar">
                            <div class="queue-item-progress-fill" style="width: ${item.percent_complete}%"></div>
                        </div>
                        <small>${item.percent_complete.toFixed(1)}%</small>
                    </div>
                ` : ''}
                <div class="queue-item-actions">
                    ${!isRunning && !isCompleted ? `<button class="btn btn-small" onclick="app.removeFromQueue('${item.queue_id}')">Remove</button>` : ''}
                </div>
            </div>
        `;
    }

    showAddToQueueModal() {
        document.getElementById('add-queue-modal').classList.remove('hidden');
        document.getElementById('queue-directory').value = '';
        document.getElementById('queue-priority').value = '0';
        document.getElementById('queue-full-scan').checked = false;
    }

    hideAddToQueueModal() {
        document.getElementById('add-queue-modal').classList.add('hidden');
    }

    async addToQueue() {
        const directory = document.getElementById('queue-directory').value.trim();
        const priority = parseInt(document.getElementById('queue-priority').value);
        const fullScan = document.getElementById('queue-full-scan').checked;

        if (!directory) {
            this.showToast('Error', 'Please enter a directory', 'error');
            return;
        }

        if (!this.currentLibraryId) {
            this.showToast('Error', 'No library selected', 'error');
            return;
        }

        try {
            await this.api('/api/queue', {
                method: 'POST',
                body: JSON.stringify({
                    library_id: this.currentLibraryId,
                    directory,
                    priority,
                    full_scan: fullScan
                })
            });

            this.hideAddToQueueModal();
            this.showToast('Added to Queue', `Added ${directory} to scan queue`, 'success');
            this.loadQueue();
        } catch (e) {
            this.showToast('Error', e.message || 'Failed to add to queue', 'error');
        }
    }

    async removeFromQueue(queueId) {
        try {
            await this.api(`/api/queue/${queueId}`, { method: 'DELETE' });
            this.showToast('Removed', 'Item removed from queue', 'success');
            this.loadQueue();
        } catch (e) {
            this.showToast('Error', e.message || 'Failed to remove', 'error');
        }
    }

    async clearCompletedQueue() {
        try {
            const result = await this.api('/api/queue/clear-completed', { method: 'POST' });
            this.showToast('Cleared', `Cleared ${result.count} completed items`, 'success');
            this.loadQueue();
        } catch (e) {
            this.showToast('Error', e.message || 'Failed to clear', 'error');
        }
    }


    startQueuePolling() {
        if (this.queuePollInterval) return;
        this.queuePollInterval = setInterval(() => this.loadQueue(), 2000);
    }

    stopQueuePolling() {
        if (this.queuePollInterval) {
            clearInterval(this.queuePollInterval);
            this.queuePollInterval = null;
        }
    }

    // ===== Unified Operations Panel =====

    toggleOpsCard(cardId) {
        const body = document.getElementById(cardId + '-body');
        if (body) body.classList.toggle('collapsed');
    }

    _updateOpsStatusIcons(t, m) {
        // ROM transfer icon
        const romIcon = document.getElementById('ops-rom-status-icon');
        if (romIcon) {
            if (t?.active) {
                romIcon.innerHTML = '<div class="ops-spinner"></div>';
            } else if (t?.current_system === 'COMPLETE') {
                romIcon.innerHTML = '<svg viewBox="0 0 20 20" width="18" height="18"><circle cx="10" cy="10" r="8" fill="var(--success)" opacity="0.2"/><path d="M6 10l3 3 5-5" fill="none" stroke="var(--success)" stroke-width="2"/></svg>';
            } else {
                romIcon.innerHTML = '<svg class="ops-icon-idle" viewBox="0 0 20 20" width="18" height="18"><circle cx="10" cy="10" r="8" fill="none" stroke="var(--text-muted)" stroke-width="1.5"/></svg>';
            }
        }

        // Media transfer icon
        const mediaIcon = document.getElementById('ops-media-status-icon');
        if (mediaIcon) {
            if (m?.active) {
                mediaIcon.innerHTML = '<div class="ops-spinner ops-media-spinner"></div>';
            } else if (m?.current_system === 'COMPLETE') {
                mediaIcon.innerHTML = '<svg viewBox="0 0 20 20" width="18" height="18"><circle cx="10" cy="10" r="8" fill="var(--success)" opacity="0.2"/><path d="M6 10l3 3 5-5" fill="none" stroke="var(--success)" stroke-width="2"/></svg>';
            } else {
                mediaIcon.innerHTML = '<svg class="ops-icon-idle" viewBox="0 0 20 20" width="18" height="18"><circle cx="10" cy="10" r="8" fill="none" stroke="var(--text-muted)" stroke-width="1.5"/></svg>';
            }
        }

    }

    // ===== Auto-manage =====

    toggleAutoManage(enabled) {
        this._autoManageEnabled = enabled;
        localStorage.setItem('duper_auto_manage', enabled ? '1' : '0');
        if (enabled) {
            this.showToast('Auto-manage', 'Operations will chain automatically', 'success');
        }
    }

    _handleAutoManage(t, m) {
        if (!this._autoManageEnabled) return;

        const timeline = document.getElementById('auto-manage-timeline');

        // ROM transfer completed -> start media transfer
        if (t && !t.active && t.current_system === 'COMPLETE' && !this._autoMediaStarted) {
            if (m && !m.active && m.current_system !== 'COMPLETE') {
                this._autoMediaStarted = true;
                this.startMediaTransfer();
                this._addAutoManageEvent('ROM Transfer complete, starting Media Transfer');
            }
        }

        // Reset flags when transfers are idle
        if (t && !t.active && t.current_system !== 'COMPLETE') {
            this._autoMediaStarted = false;
        }

        // Render timeline
        if (timeline && this._autoManageLog && this._autoManageLog.length > 0) {
            const key = this._autoManageLog.map(e => e.time).join(',');
            if (this._lastAutoManageKey !== key) {
                this._lastAutoManageKey = key;
                timeline.innerHTML = this._autoManageLog.map(e =>
                    `<div class="auto-timeline-entry"><span class="auto-timeline-time">${new Date(e.time).toLocaleTimeString()}</span><span class="auto-timeline-msg">${e.msg}</span></div>`
                ).join('');
            }
        }
    }

    _addAutoManageEvent(msg) {
        if (!this._autoManageLog) this._autoManageLog = [];
        this._autoManageLog.unshift({ msg, time: Date.now() });
        if (this._autoManageLog.length > 10) this._autoManageLog.pop();
    }

    // ===== Devices =====

    async _pollDevices() {
        this._devicesPollCount = (this._devicesPollCount || 0) + 1;
        if (this._devicesPollCount % 10 !== 1) return; // every 5s

        try {
            const health = await this.api('/api/health').catch(() => null);
            // Update device status based on connection
            const glassiteDot = document.getElementById('device-glassite-dot');
            const glassiteStatus = document.getElementById('device-glassite-status');
            if (health) {
                glassiteDot?.classList.add('online');
                glassiteDot?.classList.remove('offline');
                if (glassiteStatus) glassiteStatus.textContent = 'Online';
            } else {
                glassiteDot?.classList.remove('online');
                glassiteDot?.classList.add('offline');
                if (glassiteStatus) glassiteStatus.textContent = 'Offline';
            }

            // Steam Deck - check if this IS the deck based on platform
            const deckDot = document.getElementById('device-steamdeck-dot');
            const deckStatus = document.getElementById('device-steamdeck-status');
            const isLocalDeck = navigator.userAgent.includes('Linux') && window.innerWidth <= 1280;
            if (deckDot) {
                deckDot.classList.toggle('online', isLocalDeck);
                deckDot.classList.toggle('offline', !isLocalDeck);
            }
            if (deckStatus) deckStatus.textContent = isLocalDeck ? 'This device' : 'Unknown';
        } catch (e) {
            // Ignore
        }
    }

    syncDeviceConfigs(device) {
        this.showToast('Sync Configs', `Syncing configs for ${device}...`, 'info');
        // Future: API call to sync RetroArch configs to device
    }

    // ===== Acquisition Page =====

    async loadAcquisitionPage() {
        this._acqLastDataKey = null;
        this._acqLastFeedKey = null;

        const [collections, summary, jobs] = await Promise.all([
            this.api('/api/acquisition/collections').catch(() => []),
            this.api('/api/acquisition/summary').catch(() => []),
            this.api('/api/acquisition/jobs').catch(() => []),
        ]);

        const summaryMap = {};
        let totalFiles = 0, totalBytes = 0;
        (summary || []).forEach(s => {
            summaryMap[s.id] = s;
            totalFiles += s.on_nas || 0;
            totalBytes += s.total_bytes || 0;
        });

        const activeJobs = (jobs || []).filter(j => j.status === 'running');
        const totalSpeed = activeJobs.reduce((sum, j) => sum + ((j.live || {}).current_speed_bps || 0), 0);

        this._acqAnimateCounter('acq-stat-total-files', totalFiles);
        this._setText('acq-stat-total-size', totalBytes > 0 ? this.formatSize(totalBytes) : '-');
        this._acqAnimateCounter('acq-stat-active', activeJobs.length);
        this._acqUpdateSpeed(totalSpeed);

        // Active pulse
        const pulse = document.getElementById('acq-active-pulse');
        if (pulse) pulse.className = 'acq-active-pulse' + (activeJobs.length > 0 ? ' active' : '');

        // Active count badge
        const countBadge = document.getElementById('acq-active-count');
        if (countBadge) {
            countBadge.textContent = activeJobs.length > 0 ? activeJobs.length : '';
            countBadge.className = 'acq-active-count' + (activeJobs.length > 0 ? ' has-active' : '');
        }

        // Collection count
        this._setText('acq-collection-count', (collections || []).length > 0 ? (collections || []).length + ' available' : '');

        this._renderAcqCollections(collections, summaryMap);
        this._renderAcqActiveJobs(jobs || []);
        this._renderAcqHistory(jobs || []);
        this._renderAcqDownloadFeed(jobs || []);
    }

    // Smooth animated counter transition
    _acqAnimateCounter(id, targetValue) {
        const el = document.getElementById(id);
        if (!el) return;
        const current = parseInt(el.getAttribute('data-value') || '0', 10) || 0;
        if (current === targetValue) return;
        el.setAttribute('data-value', targetValue);
        const duration = 400;
        const startTime = performance.now();
        const animate = (now) => {
            const elapsed = now - startTime;
            const progress = Math.min(elapsed / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 3);
            const val = Math.round(current + (targetValue - current) * eased);
            el.textContent = val.toLocaleString();
            if (progress < 1) requestAnimationFrame(animate);
        };
        requestAnimationFrame(animate);
    }

    // Speed display with color tiers
    _acqUpdateSpeed(speedBps) {
        const el = document.getElementById('acq-stat-speed');
        if (!el) return;
        if (speedBps <= 0) {
            el.textContent = '-';
            el.className = 'acq-global-stat-value acq-counter acq-speed-value';
            return;
        }
        el.textContent = this.formatSize(speedBps) + '/s';
        let tier = 'slow';
        if (speedBps >= 10 * 1024 * 1024) tier = 'blazing';
        else if (speedBps >= 2 * 1024 * 1024) tier = 'fast';
        else if (speedBps >= 500 * 1024) tier = 'medium';
        el.className = 'acq-global-stat-value acq-counter acq-speed-value speed-' + tier;
    }

    // Speed tier class for inline speed values
    _acqSpeedTier(bps) {
        if (bps >= 10 * 1024 * 1024) return 'speed-blazing';
        if (bps >= 2 * 1024 * 1024) return 'speed-fast';
        if (bps >= 500 * 1024) return 'speed-medium';
        return 'speed-slow';
    }

    _renderAcqCollections(collections, summaryMap) {
        const container = document.getElementById('acq-collections');
        if (!container) return;

        if (!collections || collections.length === 0) {
            container.innerHTML = '<p class="placeholder-text">No collections available</p>';
            return;
        }

        container.innerHTML = collections.map((coll, idx) => {
            const info = summaryMap[coll.id] || {};
            const onNas = info.on_nas || 0;
            const totalBytes = info.total_bytes || 0;
            const active = info.active_jobs || 0;
            const subs = (coll.sub_collections || []);

            const circumference = 2 * Math.PI * 20;
            let fillPct = 0;
            if (onNas > 0) fillPct = Math.min(95, 5 + 18 * Math.log10(onNas + 1));
            const dashOffset = circumference - (fillPct / 100) * circumference;

            // System badge color
            const sysColors = { 'PS1': '#003791', 'PS2': '#003791', 'PSP': '#003791', 'N64': '#e4000f', 'GBA': '#4b0082', 'SNES': '#bebebe', 'NES': '#c4c4c4', 'GC': '#663399', 'DC': '#f37000', 'Saturn': '#1a1a2e' };
            const sysColor = sysColors[coll.system] || 'var(--accent)';

            return `
                <div class="acq-collection-card${active > 0 ? ' downloading' : ''}" data-collection="${coll.id}" style="animation-delay:${idx * 60}ms">
                    <div class="acq-collection-top">
                        <div class="acq-progress-ring">
                            <svg viewBox="0 0 48 48">
                                <circle class="acq-ring-bg" cx="24" cy="24" r="20"/>
                                <circle class="acq-ring-fill${active > 0 ? ' animating' : ''}" cx="24" cy="24" r="20"
                                    stroke-dasharray="${circumference}" stroke-dashoffset="${dashOffset}"/>
                            </svg>
                            <span class="acq-ring-label">${onNas}</span>
                        </div>
                        <div class="acq-collection-meta">
                            <div class="acq-collection-header">
                                <h4>${coll.label}</h4>
                                <div class="acq-badge-row">
                                    <span class="acq-badge acq-sys-badge" style="--sys-color:${sysColor}">${coll.system}</span>
                                    <span class="acq-badge">${coll.format}</span>
                                    ${active > 0 ? '<span class="acq-badge active">Downloading</span>' : ''}
                                </div>
                            </div>
                            <div class="acq-collection-info">
                                <span>${onNas.toLocaleString()} games on NAS</span>
                                <span>${coll.region}</span>
                            </div>
                            <div class="acq-collection-size-bar">
                                <div class="acq-size-stats">
                                    <span>${onNas.toLocaleString()} files</span>
                                    <span>${totalBytes > 0 ? this.formatSize(totalBytes) : '-'}</span>
                                </div>
                                <div class="acq-size-track">
                                    <div class="acq-size-fill" style="width:${fillPct}%"></div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="acq-subcollections">
                        ${subs.map(sub => `
                            <button class="btn btn-small acq-sub-btn"
                                onclick="app.startCollectionDownload('${coll.id}', '${sub.id}')"
                                title="Download ${sub.label}">
                                ${sub.label}
                            </button>
                        `).join('')}
                    </div>
                </div>
            `;
        }).join('');
    }

    _renderAcqActiveJobs(jobs) {
        const activeEl = document.getElementById('acq-active-jobs');
        if (!activeEl) return;

        const active = jobs.filter(j => j.status === 'running');
        if (active.length === 0) {
            activeEl.innerHTML = '<p class="placeholder-text">No active downloads</p>';
            return;
        }

        activeEl.innerHTML = active.map(j => this._renderAcqJobCard(j)).join('');
    }

    _renderAcqHistory(jobs) {
        const historyEl = document.getElementById('acq-job-history');
        if (!historyEl) return;

        const history = jobs.filter(j => j.status !== 'running');
        if (history.length === 0) {
            historyEl.innerHTML = '<p class="placeholder-text">No previous downloads</p>';
        } else {
            historyEl.innerHTML = history.slice(0, 20).map(j => this._renderAcqHistoryCard(j)).join('');
        }
    }

    _renderAcqJobCard(job) {
        const live = job.live || {};
        const completed = job.completed_files || 0;
        const total = job.total_files || 0;
        const failed = job.failed_files || 0;
        const skipped = job.skipped_files || 0;
        const pct = total > 0 ? ((completed / total) * 100).toFixed(1) : 0;

        // Files progress ring (mini)
        const ringCirc = 2 * Math.PI * 14;
        const ringPct = total > 0 ? (completed / total) * 100 : 0;
        const ringOffset = ringCirc - (ringPct / 100) * ringCirc;

        // Bytes downloaded
        const bytesDown = live.bytes_downloaded || 0;

        // Current download section with animated arrow
        let currentDownload = '';
        if (live.current_file) {
            const speedBps = live.current_speed_bps || 0;
            const speed = speedBps > 0 ? this.formatSize(speedBps) + '/s' : '';
            const speedTier = this._acqSpeedTier(speedBps);
            const eta = live.current_eta_seconds > 0 ? this._formatEta(live.current_eta_seconds) : '';
            currentDownload = `
                <div class="acq-live-download">
                    <div class="acq-live-indicator">
                        <div class="acq-download-arrow">
                            <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" stroke-width="2.5">
                                <path d="M12 3v14M5 12l7 7 7-7"/>
                                <line x1="4" y1="21" x2="20" y2="21" stroke-dasharray="2 2"/>
                            </svg>
                        </div>
                        <div class="acq-live-pulse"></div>
                    </div>
                    <div class="acq-live-info">
                        <div class="acq-live-filename" title="${live.current_file}">${live.current_file}</div>
                        <div class="acq-live-metrics">
                            ${speed ? `<span class="acq-metric speed ${speedTier}">${speed}</span>` : ''}
                            ${eta ? `<span class="acq-metric eta"><svg viewBox="0 0 16 16" width="10" height="10" fill="none" stroke="currentColor" stroke-width="2"><circle cx="8" cy="8" r="6"/><polyline points="8 5 8 8 10 10"/></svg> ${eta}</span>` : ''}
                            <span class="acq-metric files"><svg viewBox="0 0 16 16" width="10" height="10" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 1H4a1 1 0 00-1 1v12a1 1 0 001 1h8a1 1 0 001-1V4z"/></svg> ${completed}/${total}</span>
                            ${bytesDown > 0 ? `<span class="acq-metric bytes">${this.formatSize(bytesDown)}</span>` : ''}
                        </div>
                    </div>
                    <div class="acq-live-progress-ring">
                        <svg viewBox="0 0 36 36">
                            <circle class="acq-mini-ring-bg" cx="18" cy="18" r="14"/>
                            <circle class="acq-mini-ring-fill" cx="18" cy="18" r="14"
                                stroke-dasharray="${ringCirc}" stroke-dashoffset="${ringOffset}"/>
                        </svg>
                        <span class="acq-mini-ring-pct">${Math.round(ringPct)}%</span>
                    </div>
                </div>
                ${speedBps > 0 ? `
                <div class="acq-speed-gauge">
                    <div class="acq-speed-bar-track">
                        <div class="acq-speed-bar-fill ${speedTier}" style="width:${Math.min(100, (speedBps / (20 * 1024 * 1024)) * 100)}%"></div>
                    </div>
                    <div class="acq-speed-labels">
                        <span>0</span><span>5 MB/s</span><span>10 MB/s</span><span>20 MB/s</span>
                    </div>
                </div>` : ''}
            `;
        }

        // Queue preview (5 items)
        let queuePreview = '';
        if (live.queue && live.queue.length > 0) {
            queuePreview = `
                <div class="acq-queue-preview">
                    <span class="acq-queue-label">Up Next</span>
                    <div class="acq-queue-items">
                        ${live.queue.slice(0, 5).map((f, i) => `
                            <div class="acq-queue-item" style="animation-delay:${i * 60}ms">
                                <span class="acq-qi-pos">${i + 1}</span>
                                <span class="acq-qi-name" title="${f}">${f}</span>
                            </div>
                        `).join('')}
                        ${live.queue.length > 5 ? `<div class="acq-queue-more">+${live.queue.length - 5} more in queue</div>` : ''}
                    </div>
                </div>
            `;
        }

        // Completed files scrolling feed inside card
        let completedFeed = '';
        if (live.completed && live.completed.length > 0) {
            completedFeed = `
                <div class="acq-job-completed-feed">
                    <span class="acq-queue-label">Completed</span>
                    <div class="acq-completed-scroll">
                        ${live.completed.slice(-8).reverse().map(c => {
                            const ok = !c.error;
                            const fname = c.file || c.name || '?';
                            const fsize = c.size ? this.formatSize(c.size) : '';
                            const fspeed = c.speed_bps ? this.formatSize(c.speed_bps) + '/s' : '';
                            return `<div class="acq-completed-item ${ok ? 'ok' : 'err'}">
                                <span class="acq-completed-icon">${ok
                                    ? '<svg viewBox="0 0 16 16" width="11" height="11" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M13 4L6 12 3 9"/></svg>'
                                    : '<svg viewBox="0 0 16 16" width="11" height="11" fill="none" stroke="currentColor" stroke-width="2.5"><line x1="12" y1="4" x2="4" y2="12"/><line x1="4" y1="4" x2="12" y2="12"/></svg>'
                                }</span>
                                <span class="acq-completed-name" title="${fname}">${fname}</span>
                                ${fsize ? `<span class="acq-completed-size">${fsize}</span>` : ''}
                                ${fspeed ? `<span class="acq-completed-speed">${fspeed}</span>` : ''}
                            </div>`;
                        }).join('')}
                    </div>
                </div>
            `;
        }

        return `
            <div class="acq-job-card-v2 active">
                <div class="acq-job-pulse-ring"></div>
                <div class="acq-job-card-header">
                    <div class="acq-job-title-row">
                        <div class="acq-job-icon downloading">
                            <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3"/>
                            </svg>
                        </div>
                        <div class="acq-job-title-info">
                            <span class="acq-job-title">${job.collection_id} / ${job.sub_collection}</span>
                            <span class="acq-job-subtitle">${live.current_file ? 'Downloading' : 'Scanning collection'}</span>
                        </div>
                        <span class="acq-job-status-badge running">Running</span>
                        <button class="btn btn-danger btn-tiny" onclick="app.cancelAcqJob('${job.job_id}')">Cancel</button>
                    </div>
                </div>
                <div class="acq-job-card-body">
                    ${currentDownload}
                    <div class="acq-job-progress-v2">
                        <div class="acq-progress-track">
                            <div class="acq-progress-fill-v2" style="width:${pct}%"></div>
                        </div>
                        <div class="acq-progress-labels">
                            <span class="acq-progress-pct">${pct}%</span>
                            <span class="acq-progress-detail">${completed}/${total} files${failed > 0 ? ` | ${failed} failed` : ''}${skipped > 0 ? ` | ${skipped} skipped` : ''}</span>
                        </div>
                    </div>
                    ${queuePreview}
                    ${completedFeed}
                </div>
            </div>
        `;
    }

    _renderAcqHistoryCard(job) {
        const pct = job.total_files > 0 ? ((job.completed_files / job.total_files) * 100).toFixed(1) : 0;
        const statusClass = job.status === 'completed' ? 'success' : job.status === 'cancelled' ? 'warning' : job.status === 'failed' ? 'danger' : '';
        const statusLabel = job.status.charAt(0).toUpperCase() + job.status.slice(1);
        const duration = job.duration_seconds ? this._formatEta(job.duration_seconds) : '';
        const totalSize = job.total_bytes ? this.formatSize(job.total_bytes) : '';

        return `
            <div class="acq-history-card ${statusClass}">
                <div class="acq-history-header">
                    <div class="acq-history-icon ${statusClass}">
                        ${job.status === 'completed'
                            ? '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M20 6L9 17l-5-5"/></svg>'
                            : job.status === 'cancelled'
                                ? '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2.5"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>'
                                : '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>'
                        }
                    </div>
                    <div class="acq-history-title-wrap">
                        <span class="acq-history-title">${job.collection_id} / ${job.sub_collection}</span>
                        <span class="acq-history-meta">${job.completed_files || 0}/${job.total_files || 0} files${totalSize ? ' | ' + totalSize : ''}${duration ? ' | ' + duration : ''}${job.failed_files > 0 ? ' | ' + job.failed_files + ' failed' : ''}</span>
                    </div>
                    <span class="acq-job-status-badge ${statusClass}">${statusLabel}</span>
                </div>
                <div class="acq-history-bar-wrap">
                    <div class="acq-history-bar"><div class="acq-history-fill ${statusClass}" style="width:${pct}%"></div></div>
                </div>
            </div>
        `;
    }

    _renderAcqDownloadFeed(jobs) {
        const feedEl = document.getElementById('acq-download-feed');
        if (!feedEl) return;

        const entries = [];
        (jobs || []).forEach(j => {
            const live = j.live || {};
            (live.completed || []).forEach(c => {
                entries.push({
                    file: c.file || c.name || '?',
                    ok: !c.error,
                    error: c.error || '',
                    size: c.size || 0,
                    speed: c.speed_bps || 0,
                    collection: j.collection_id,
                    timestamp: c.timestamp || '',
                });
            });
        });

        // Update feed count badge
        const feedCount = document.getElementById('acq-feed-count');
        if (feedCount) feedCount.textContent = entries.length > 0 ? entries.length + ' entries' : '';

        // Check if feed data changed
        const feedKey = entries.length + ':' + (entries.length > 0 ? entries[entries.length - 1].file : '');
        if (this._acqLastFeedKey === feedKey) return;
        this._acqLastFeedKey = feedKey;

        if (entries.length === 0) {
            feedEl.innerHTML = '<p class="placeholder-text">No recent downloads</p>';
            return;
        }

        // Show last 50, newest first
        feedEl.innerHTML = entries.slice(-50).reverse().map((e, i) => {
            const speedTier = e.speed > 0 ? this._acqSpeedTier(e.speed) : '';
            return `
            <div class="acq-feed-entry${e.ok ? '' : ' failed'}" style="animation-delay:${Math.min(i * 20, 300)}ms">
                <span class="acq-feed-icon ${e.ok ? 'ok' : 'err'}">
                    ${e.ok
                        ? '<svg viewBox="0 0 16 16" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M13 4L6 12 3 9"/></svg>'
                        : '<svg viewBox="0 0 16 16" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2.5"><line x1="12" y1="4" x2="4" y2="12"/><line x1="4" y1="4" x2="12" y2="12"/></svg>'
                    }
                </span>
                ${e.timestamp ? `<span class="acq-feed-time">${e.timestamp}</span>` : ''}
                <span class="acq-feed-name" title="${e.file}">${e.file}</span>
                <span class="acq-feed-collection">${e.collection}</span>
                ${e.size > 0 ? `<span class="acq-feed-size">${this.formatSize(e.size)}</span>` : ''}
                ${e.speed > 0 ? `<span class="acq-feed-speed ${speedTier}">${this.formatSize(e.speed)}/s</span>` : ''}
            </div>`;
        }).join('');

        // Auto-scroll to top (latest)
        feedEl.scrollTop = 0;
    }

    _formatEta(seconds) {
        if (!seconds || seconds <= 0) return '';
        if (seconds < 60) return Math.round(seconds) + 's';
        if (seconds < 3600) return Math.floor(seconds / 60) + 'm ' + Math.round(seconds % 60) + 's';
        return Math.floor(seconds / 3600) + 'h ' + Math.floor((seconds % 3600) / 60) + 'm';
    }

    async _refreshAcquisitionJobs() {
        const [jobs, summary] = await Promise.all([
            this.api('/api/acquisition/jobs').catch(() => []),
            this.api('/api/acquisition/summary').catch(() => []),
        ]);
        if (!jobs) return;

        const active = jobs.filter(j => j.status === 'running');
        const totalSpeed = active.reduce((sum, j) => sum + ((j.live || {}).current_speed_bps || 0), 0);

        let totalFiles = 0, totalBytes = 0;
        (summary || []).forEach(s => {
            totalFiles += s.on_nas || 0;
            totalBytes += s.total_bytes || 0;
        });
        this._acqAnimateCounter('acq-stat-active', active.length);
        this._acqAnimateCounter('acq-stat-total-files', totalFiles);
        this._setText('acq-stat-total-size', totalBytes > 0 ? this.formatSize(totalBytes) : '-');
        this._acqUpdateSpeed(totalSpeed);

        // Active pulse
        const pulse = document.getElementById('acq-active-pulse');
        if (pulse) pulse.className = 'acq-active-pulse' + (active.length > 0 ? ' active' : '');

        const countBadge = document.getElementById('acq-active-count');
        if (countBadge) {
            countBadge.textContent = active.length > 0 ? active.length : '';
            countBadge.className = 'acq-active-count' + (active.length > 0 ? ' has-active' : '');
        }

        this._renderAcqActiveJobs(jobs);
        this._renderAcqHistory(jobs);
        this._renderAcqDownloadFeed(jobs);
    }

    async _pollAcquisition() {
        // Poll every 4th tick (2s) and skip DOM rebuild if data fingerprint unchanged
        this._acqPollCount = (this._acqPollCount || 0) + 1;
        if (this._acqPollCount % 4 !== 0) return;

        const jobs = await this.api('/api/acquisition/jobs').catch(() => []);
        // Deep fingerprint: includes live state fields to catch speed/file changes
        const dataKey = JSON.stringify((jobs || []).map(j => {
            const l = j.live || {};
            return j.job_id + ':' + j.status + ':' + (j.completed_files || 0) + ':' + (j.failed_files || 0) + ':' + (l.current_file || '') + ':' + (l.current_speed_bps || 0);
        }));
        if (this._acqLastDataKey === dataKey) return;
        this._acqLastDataKey = dataKey;

        await this._refreshAcquisitionJobs();
    }

    async startCollectionDownload(collectionId, subCollection) {
        try {
            const result = await this.api('/api/acquisition/start?collection_id=' + collectionId + '&sub_collection=' + subCollection, { method: 'POST' });
            if (result.error) {
                this.showToast('Acquisition', result.error, 'warning');
                return;
            }
            this.showToast('Acquisition', 'Started ' + collectionId + ' / ' + subCollection, 'success');
            await this._refreshAcquisitionJobs();
        } catch (e) {
            this.showToast('Acquisition', 'Failed: ' + e.message, 'error');
        }
    }

    async cancelAcqJob(jobId) {
        if (!confirm('Cancel this download?')) return;
        await this.api('/api/acquisition/cancel/' + jobId, { method: 'POST' }).catch(function(){});
        await this._refreshAcquisitionJobs();
    }

    // Load cover art for the Now Playing dashboard card
    async _loadNowPlayingCover(filepath, gameName, system) {
        const coverEl = document.getElementById('lp-cover-art');
        if (!coverEl) return;
        const cacheKey = `${system}/${gameName}`;
        if (this._lastNowPlayingPath === cacheKey) return;
        this._lastNowPlayingPath = cacheKey;

        try {
            if (this.coverArtCache.has(cacheKey)) {
                const cached = this.coverArtCache.get(cacheKey);
                if (cached && cached.src) {
                    coverEl.innerHTML = '<img src="' + cached.src + '" alt="' + this._escHtml(gameName) + '" loading="lazy">';
                }
                return;
            }
            // Use the new game-image endpoint that searches by system + name
            const url = `/api/media/game-image/${encodeURIComponent(system)}/${encodeURIComponent(gameName)}`;
            coverEl.innerHTML = `<img src="${url}" alt="${this._escHtml(gameName)}" loading="lazy" onerror="this.style.display='none'">`;
            this.coverArtCache.set(cacheKey, { src: url });
        } catch (e) {
            this.coverArtCache.set(cacheKey, null);
        }
    }

    // Get game image URL for use in game cards etc
    _gameImageUrl(system, filename) {
        return `/api/media/game-image/${encodeURIComponent(system)}/${encodeURIComponent(filename)}`;
    }

    // Load RetroAchievements progress for the Now Playing game
    async _updateLiveFeed(isNowPlaying) {
        const feedEl = document.getElementById('lp-live-feed');
        const feedImg = document.getElementById('lp-live-img');
        if (!feedEl) return;

        if (isNowPlaying) {
            // Check if a frame exists (only available when RA is running a game)
            const status = await this.api('/api/live/status').catch(() => ({}));

            if (status.has_frame && !status.waiting) {
                feedEl.style.display = '';
                if (feedImg) feedImg.src = '/api/live/frame?' + Date.now();
                if (!this._liveFeedTimer) {
                    this._liveFeedTimer = setInterval(() => {
                        if (feedImg) feedImg.src = '/api/live/frame?' + Date.now();
                    }, 10000);
                }
            } else {
                feedEl.style.display = 'none';
            }

            // Start capture if not running
            if (!status.active) {
                await this.api('/api/live/start?interval=10', { method: 'POST' }).catch(() => {});
            }
        } else {
            feedEl.style.display = 'none';
            if (this._liveFeedTimer) {
                clearInterval(this._liveFeedTimer);
                this._liveFeedTimer = null;
            }
            await this.api('/api/live/stop', { method: 'POST' }).catch(() => {});
        }
    }

    async _loadNowPlayingRA(system, gameName) {
        const raSection = document.getElementById('lp-ra-progress');
        if (!raSection) return;

        // Extract filename for the detail API
        const clean = gameName.replace(/^\.\//, '');
        const filename = clean.includes('/') ? clean.split('/').pop() : clean;

        try {
            const detail = await this.api(`/api/game-detail/${encodeURIComponent(system)}/${encodeURIComponent(filename)}`);
            const ra = detail?.ra_progress;

            if (ra && ra.achievements_total > 0) {
                raSection.style.display = '';
                this._setText('lp-ra-earned', ra.achievements_earned);
                this._setText('lp-ra-total', ra.achievements_total);
                this._setText('lp-ra-pct', ra.completion_pct + '%');

                const bar = document.getElementById('lp-ra-bar');
                if (bar) bar.style.width = ra.completion_pct + '%';

                const link = document.getElementById('lp-ra-link');
                if (link) link.href = ra.ra_url;
            } else {
                raSection.style.display = 'none';
            }
        } catch {
            raSection.style.display = 'none';
        }
    }

    // ===== Gamification System =====

    _initGamification() {
        const saved = JSON.parse(localStorage.getItem('duper_gamification') || '{}');
        this._gamerXP = saved.xp || 0;
        this._gamerLevel = saved.level || 1;
        this._gamerStreak = saved.streak || 0;
        this._gamerLastPlayDate = saved.lastPlayDate || null;
        this._gamerAchievementsSeen = new Set(saved.achievementsSeen || []);
        this._lastHeroValues = {};
    }

    _saveGamification() {
        localStorage.setItem('duper_gamification', JSON.stringify({
            xp: this._gamerXP,
            level: this._gamerLevel,
            streak: this._gamerStreak,
            lastPlayDate: this._gamerLastPlayDate,
            achievementsSeen: [...this._gamerAchievementsSeen],
        }));
    }

    // XP thresholds per level
    _getXPForLevel(level) {
        const thresholds = [0, 100, 300, 600, 1000, 2000, 4000, 8000, 15000, 30000, 60000, 100000];
        return thresholds[Math.min(level - 1, thresholds.length - 1)] || 100000;
    }

    _getXPForNextLevel(level) {
        const thresholds = [0, 100, 300, 600, 1000, 2000, 4000, 8000, 15000, 30000, 60000, 100000];
        return thresholds[Math.min(level, thresholds.length - 1)] || 100000;
    }

    _getRankTitle(level) {
        const ranks = [
            'Apprentice', 'Gamer', 'Player', 'Enthusiast', 'Collector',
            'Veteran', 'Pro', 'Master', 'Legend', 'Champion', 'God Tier', 'Ascended'
        ];
        return ranks[Math.min(level - 1, ranks.length - 1)] || 'Ascended';
    }

    // Calculate XP from various sources
    _calculateXP(gamingData, raData) {
        let xp = 0;

        // XP from playing games (10 XP per play session)
        if (gamingData && gamingData.collection) {
            const totalPlayed = gamingData.collection.total_played || 0;
            xp += totalPlayed * 10;

            // XP from playtime (1 XP per minute played)
            xp += gamingData.collection.total_playtime_minutes || 0;
        }

        // XP from achievements (50 XP per achievement)
        if (raData && raData.summary) {
            const points = raData.summary.total_points || 0;
            xp += points; // Use RA points directly as XP
        }

        // XP from collection size (1 XP per game owned)
        if (gamingData && gamingData.collection) {
            xp += gamingData.collection.total_games || 0;
        }

        return xp;
    }

    // Calculate play streak
    _updateStreak(gamingData) {
        if (!gamingData || !gamingData.last_played) return;

        const lastPlayedStr = gamingData.last_played.lastplayed;
        if (!lastPlayedStr) return;

        const lastPlayed = new Date(lastPlayedStr);
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        lastPlayed.setHours(0, 0, 0, 0);

        const diffDays = Math.floor((today - lastPlayed) / (1000 * 60 * 60 * 24));

        if (diffDays === 0) {
            // Played today
            if (this._gamerLastPlayDate !== today.toISOString().split('T')[0]) {
                this._gamerStreak++;
                this._gamerLastPlayDate = today.toISOString().split('T')[0];
            }
        } else if (diffDays === 1) {
            // Played yesterday, streak continues
            if (this._gamerLastPlayDate !== lastPlayed.toISOString().split('T')[0]) {
                this._gamerStreak++;
                this._gamerLastPlayDate = lastPlayed.toISOString().split('T')[0];
            }
        } else if (diffDays > 1) {
            // Streak broken
            this._gamerStreak = 0;
        }
    }

    // Update the gamer profile bar UI
    _updateGamerProfileUI(gamingData, raData) {
        // Calculate XP
        const xp = this._calculateXP(gamingData, raData);
        this._gamerXP = xp;

        // Calculate level
        let level = 1;
        const thresholds = [0, 100, 300, 600, 1000, 2000, 4000, 8000, 15000, 30000, 60000, 100000];
        for (let i = thresholds.length - 1; i >= 0; i--) {
            if (xp >= thresholds[i]) { level = i + 1; break; }
        }

        const oldLevel = this._gamerLevel;
        this._gamerLevel = level;

        // Update streak
        this._updateStreak(gamingData);

        // Update UI elements
        const levelNum = document.getElementById('gamer-level-num');
        if (levelNum) levelNum.textContent = level;

        const rankTitle = document.getElementById('gamer-rank-title');
        if (rankTitle) rankTitle.textContent = this._getRankTitle(level);

        // Update XP bar
        const currentLevelXP = this._getXPForLevel(level);
        const nextLevelXP = this._getXPForNextLevel(level);
        const xpInLevel = xp - currentLevelXP;
        const xpNeeded = nextLevelXP - currentLevelXP;
        const xpPercent = xpNeeded > 0 ? Math.min((xpInLevel / xpNeeded) * 100, 100) : 100;

        const xpFill = document.getElementById('gamer-xp-fill');
        if (xpFill) xpFill.style.width = xpPercent + '%';

        const xpText = document.getElementById('gamer-xp-text');
        if (xpText) xpText.textContent = `${xp.toLocaleString()} / ${nextLevelXP.toLocaleString()} XP`;

        // Update XP ring
        const xpRing = document.getElementById('gamer-xp-ring');
        if (xpRing) {
            const circumference = 2 * Math.PI * 26;
            const offset = circumference - (xpPercent / 100) * circumference;
            xpRing.style.strokeDashoffset = Math.max(0, offset);
        }

        // Update streak UI
        const streakCount = document.getElementById('gamer-streak-count');
        if (streakCount) streakCount.textContent = this._gamerStreak;

        const streakEl = document.getElementById('gamer-streak');
        if (streakEl) {
            streakEl.classList.toggle('active-streak', this._gamerStreak > 0);
        }

        // Level up toast
        if (level > oldLevel && oldLevel > 0) {
            this.showToast('Level Up!', `You reached Level ${level}: ${this._getRankTitle(level)}`, 'success');
            if (levelNum) {
                levelNum.style.animation = 'none';
                levelNum.offsetHeight;
                levelNum.style.animation = 'level-up-flash 1.2s ease-out';
            }
        }

        this._saveGamification();
    }

    // Update hero stat counters with animation
    _updateHeroStats(gamingData, raData, stats) {
        const updates = {};

        if (gamingData && gamingData.collection) {
            updates['hero-total-games'] = (gamingData.collection.total_games || 0).toLocaleString();
            updates['hero-total-systems'] = (gamingData.collection.total_systems || 0).toString();
            updates['hero-completion-pct'] = (gamingData.collection.completion_pct || 0) + '%';

            const totalMins = gamingData.collection.total_playtime_minutes || 0;
            if (totalMins >= 60) {
                updates['hero-total-playtime'] = Math.floor(totalMins / 60) + 'h';
            } else {
                updates['hero-total-playtime'] = totalMins + 'm';
            }
        }

        if (raData && raData.summary) {
            updates['hero-total-achievements'] = (raData.summary.total_points || 0).toLocaleString();
        }

        // Animate counter changes
        for (const [id, value] of Object.entries(updates)) {
            const el = document.getElementById(id);
            if (!el) continue;

            if (this._lastHeroValues[id] !== value) {
                el.textContent = value;
                if (this._lastHeroValues[id] !== undefined) {
                    el.classList.add('counter-pop');
                    setTimeout(() => el.classList.remove('counter-pop'), 400);
                }
                this._lastHeroValues[id] = value;
            }
        }
    }

    // Achievement unlock toast with badge
    _checkNewAchievements(raData) {
        if (!raData || !raData.recent_achievements) return;

        for (const ach of raData.recent_achievements) {
            const key = ach.title + ':' + (ach.date || '');
            if (!this._gamerAchievementsSeen.has(key)) {
                this._gamerAchievementsSeen.add(key);

                // Only show toast for truly new ones (skip initial load)
                if (this._gamerAchievementsSeen.size > raData.recent_achievements.length) {
                    this._showAchievementToast(ach);
                }
            }
        }
        this._saveGamification();
    }

    _showAchievementToast(ach) {
        let container = document.getElementById('toast-container');
        if (!container) {
            container = document.createElement('div');
            container.id = 'toast-container';
            document.body.appendChild(container);
        }

        const toast = document.createElement('div');
        toast.className = 'toast toast-achievement';
        toast.innerHTML = `
            <div class="toast-icon">${ach.badge_url ? '<img src="' + this._escHtml(ach.badge_url) + '" style="width:32px;height:32px;border-radius:4px;">' : '<svg viewBox="0 0 24 24" width="28" height="28" fill="#ffc72c"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>'}</div>
            <div class="toast-content">
                <div class="toast-title" style="color:#ffc72c;">Achievement Unlocked!</div>
                <div class="toast-message">${this._escHtml(ach.title)} (+${ach.points} pts)</div>
            </div>
        `;

        container.appendChild(toast);
        requestAnimationFrame(() => toast.classList.add('show'));
        setTimeout(() => {
            toast.classList.remove('show');
            toast.classList.add('hide');
            setTimeout(() => toast.remove(), 300);
        }, 5000);
    }

    // Update nav badges for active operations
    _updateNavBadges() {
        const acqBadge = document.getElementById('nav-acq-badge');
        if (acqBadge) {
            const count = document.getElementById('acq-active-count');
            const activeCount = count ? count.textContent : '';
            acqBadge.textContent = activeCount && activeCount !== '0' ? activeCount : '';
        }
    }
}

// Initialize app
const app = new DuperApp();
window.duper = app;
