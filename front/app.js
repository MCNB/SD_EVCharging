(function() {
  const state = {
    baseUrl: null,
    cps: [],
    sessions: [],
    drivers: [],
  };

  const centralUrlInput = document.getElementById('centralUrl');
  const lastRefreshSpan = document.getElementById('lastRefresh');
  const errorLine = document.getElementById('error-line');
  const evwStatusSpan = document.getElementById('evw-status');

  const cpsTbody = document.getElementById('cps-tbody');
  const sessionsTbody = document.getElementById('sessions-tbody');
  const driversTbody = document.getElementById('drivers-tbody');
  const alertsList = document.getElementById('alerts-list');

  // --- utils -------------------------------------------------
  function loadSavedUrl() {
    const saved = localStorage.getItem('centralUrl');
    if (saved) {
      centralUrlInput.value = saved;
    }
    state.baseUrl = centralUrlInput.value.trim().replace(/\/+$/, '');
  }

  function saveUrl() {
    const v = centralUrlInput.value.trim();
    if (!v) return;
    localStorage.setItem('centralUrl', v);
    state.baseUrl = v.replace(/\/+$/, '');
  }

  function formatDateTime(msUtc) {
    if (!msUtc || isNaN(msUtc)) return '';
    const d = new Date(msUtc);
    return d.toISOString().replace('T',' ').slice(0,19) + "Z";
  }

  function formatNumber(n, decimals) {
    if (n == null || isNaN(n)) return '';
    return n.toFixed(decimals);
  }

  async function fetchJson(path) {
    const url = state.baseUrl + path;
    const resp = await fetch(url, { cache:'no-store' });
    if (!resp.ok) {
      const text = await resp.text().catch(() => '');
      throw new Error(resp.status + " " + resp.statusText + " - " + text);
    }
    return resp.json();
  }

  // --- renderers ---------------------------------------------
  function renderCps() {
    cpsTbody.innerHTML = '';
    if (!state.cps.length) {
      cpsTbody.innerHTML = '<tr><td colspan="12">Sin CPs.</td></tr>';
      return;
    }
    const fragment = document.createDocumentFragment();
    for (const cp of state.cps) {
      const tr = document.createElement('tr');

      const estado = cp.estado || '';
      const alertClima = !!cp.weatherAlert;
      const lag = typeof cp.lastHbMs === 'number' ? cp.lastHbMs : 0;

      if (alertClima) {
        tr.classList.add('warn');
      }
      if (estado === 'AVERIADO' || estado === 'DESCONECTADO') {
        tr.classList.remove('warn');
        tr.classList.add('bad');
      } else if (estado === 'SUMINISTRANDO') {
        tr.classList.add('ok');
      }

      const tds = [
        cp.cp || '',
        estado,
        cp.parado ? 'SI' : 'NO',
        cp.ocupado ? 'SI' : 'NO',
        typeof lag === 'number' ? String(lag) : '',
        cp.sesion || '',
        cp.driver || '',
        formatNumber(parseFloat(cp.kwh), 4),
        formatNumber(parseFloat(cp.eur), 4),
        formatNumber(parseFloat(cp.precio), 4),
        (cp.tempC == null || isNaN(cp.tempC)) ? '' : formatNumber(parseFloat(cp.tempC), 2),
        alertClima ? 'ALERTA' : ''
      ];
      for (const val of tds) {
        const td = document.createElement('td');
        td.textContent = val;
        tr.appendChild(td);
      }
      fragment.appendChild(tr);
    }
    cpsTbody.appendChild(fragment);
  }

  function renderSessions() {
    sessionsTbody.innerHTML = '';
    if (!state.sessions.length) {
      sessionsTbody.innerHTML = '<tr><td colspan="6">Sin sesiones activas.</td></tr>';
      return;
    }
    const fragment = document.createDocumentFragment();
    for (const s of state.sessions) {
      const tr = document.createElement('tr');
      const tds = [
        s.session || '',
        s.cp || '',
        s.driver || '',
        formatDateTime(s.startUTC),
        formatNumber(parseFloat(s.kwh), 4),
        formatNumber(parseFloat(s.eur), 4)
      ];
      for (const v of tds) {
        const td = document.createElement('td');
        td.textContent = v;
        tr.appendChild(td);
      }
      fragment.appendChild(tr);
    }
    sessionsTbody.appendChild(fragment);
  }

  function renderDrivers() {
    driversTbody.innerHTML = '';
    if (!state.drivers.length) {
      driversTbody.innerHTML = '<tr><td>Sin drivers conocidos.</td></tr>';
      return;
    }
    const fragment = document.createDocumentFragment();
    for (const d of state.drivers) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.textContent = d.driver || '';
      tr.appendChild(td);
      fragment.appendChild(tr);
    }
    driversTbody.appendChild(fragment);
  }

  function renderAlertsAndEvW() {
    alertsList.innerHTML = '';
    const alerts = [];

    let anyWeatherData = false;
    let anyWeatherAlert = false;

    for (const cp of state.cps) {
      const cpId = cp.cp || '?';
      const estado = cp.estado || '';
      const lag = typeof cp.lastHbMs === 'number' ? cp.lastHbMs : 0;

      if (estado === 'AVERIADO') {
        alerts.push(`CP ${cpId}: AVERIADO`);
      } else if (estado === 'DESCONECTADO') {
        alerts.push(`CP ${cpId}: DESCONECTADO (sin heartbeats)`);
      }

      if (lag > 5000 && estado !== 'DESCONECTADO') {
        alerts.push(`CP ${cpId}: latido retrasado (${lag} ms)`);
      }

      if (cp.weatherAlert) {
        anyWeatherAlert = true;
        anyWeatherData = true;
        const t = (cp.tempC == null || isNaN(cp.tempC)) ? '?' : formatNumber(parseFloat(cp.tempC),2);
        alerts.push(`CP ${cpId}: ALERTA CLIMA (temp=${t}°C)`);
      } else if (cp.tempC != null && !isNaN(cp.tempC)) {
        anyWeatherData = true;
      }
    }

    if (!state.cps.length) {
      evwStatusSpan.textContent = "EV_W: sin CPs";
      evwStatusSpan.className = "pill pill-info";
    } else if (!anyWeatherData) {
      evwStatusSpan.textContent = "EV_W: sin datos (¿offline?)";
      evwStatusSpan.className = "pill pill-bad";
      alerts.push("EV_W: Central no ha recibido datos de clima para ningún CP.");
    } else if (anyWeatherAlert) {
      evwStatusSpan.textContent = "EV_W: ALERTA en alguna localización";
      evwStatusSpan.className = "pill pill-warn";
    } else {
      evwStatusSpan.textContent = "EV_W: OK (clima sin alertas)";
      evwStatusSpan.className = "pill pill-ok";
    }

    if (!alerts.length) {
      alertsList.innerHTML = '<li>Sin alertas.</li>';
      return;
    }
    const fragment = document.createDocumentFragment();
    for (const a of alerts) {
      const li = document.createElement('li');
      li.textContent = a;
      fragment.appendChild(li);
    }
    alertsList.appendChild(fragment);
  }

  // --- refresh loop ------------------------------------------
  async function refreshAll() {
    errorLine.textContent = '';
    if (!state.baseUrl) {
      state.baseUrl = centralUrlInput.value.trim().replace(/\/+$/, '');
    }
    const base = state.baseUrl;
    if (!base) {
      errorLine.textContent = "Configura la URL de EV_Central y pulsa Guardar.";
      return;
    }

    try {
      const [cpsJson, sessJson, drvJson] = await Promise.all([
        fetchJson('/api/cps').catch(() => fetchJson('/api/status')), // compat
        fetchJson('/api/sessions'),
        fetchJson('/api/drivers'),
      ]);

      state.cps = (cpsJson && cpsJson.items) || [];
      state.sessions = (sessJson && sessJson.items) || [];
      state.drivers = (drvJson && drvJson.items) || [];

      renderCps();
      renderSessions();
      renderDrivers();
      renderAlertsAndEvW();

      const now = new Date();
      lastRefreshSpan.textContent = now.toLocaleTimeString();
    } catch (e) {
      errorLine.textContent = "Error al contactar con EV_Central: " + e.message;
    }
  }

  // --- init --------------------------------------------------
  loadSavedUrl();
  document.getElementById('saveUrlBtn').addEventListener('click', () => {
    saveUrl();
    refreshAll();
  });
  document.getElementById('refreshBtn').addEventListener('click', () => {
    refreshAll();
  });

  refreshAll();
  setInterval(refreshAll, 2000);
})();