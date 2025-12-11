// URL base de EV_Central (HTTP)
const API_BASE_URL = 'http://127.0.0.1:8080'; // <-- cambia IP/puerto si hace falta

// Endpoints de la API REST de Central
const ENDPOINTS = {
    cps: '/api/cps',
    drivers: '/api/drivers',
    alerts: '/api/alerts',
    audit: '/api/audit'
};

/* ------------------ UTILIDADES DE FETCH ------------------ */

// Función genérica para obtener datos de la API
async function fetchData(endpoint) {
    try {
        const response = await fetch(`${API_BASE_URL}${endpoint}`);
        if (!response.ok) {
            throw new Error(`Error ${response.status}: ${response.statusText}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`Error obteniendo datos de ${endpoint}:`, error);
        return null;
    }
}

// Normaliza respuestas para que siempre tengamos un array
function asArray(data, itemsField = 'items') {
    if (!data) return [];
    if (Array.isArray(data)) return data;
    if (Array.isArray(data[itemsField])) return data[itemsField];
    return [];
}

/* ------------------ BUCLE PRINCIPAL ------------------ */

async function updateInterface() {
    // --- CPs ---
    const cpsRaw = await fetchData(ENDPOINTS.cps);
    const cpsItems = asArray(cpsRaw, 'items');
    updateSection('cps-list', cpsItems, renderCPCard);

    // --- Drivers ---
    const driversRaw = await fetchData(ENDPOINTS.drivers);
    const driversItems = asArray(driversRaw, 'items');
    updateSection('drivers-list', driversItems, renderDriverCard);

    // --- Clima (derivado de los CPs) ---
    const weatherItems = buildWeatherDataFromCps(cpsItems);
    updateSection('weather-list', weatherItems, renderWeatherCard);

    // --- Alertas ---
    const alertsRaw = await fetchData(ENDPOINTS.alerts);
    const alertsItems = asArray(alertsRaw, 'items');
    updateSection('alerts-list', alertsItems, renderAlertCard);

    // --- Auditoría ---
    const auditRaw = await fetchData(ENDPOINTS.audit);
    const auditItems = asArray(auditRaw, 'items');
    updateSection('audit-log', auditItems, renderAuditLog);
}

// Rellena un contenedor con tarjetas generadas por renderFunction
function updateSection(containerId, items, renderFunction) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';

    if (!items || items.length === 0) {
        container.innerHTML = '<div class="card">No hay datos disponibles</div>';
        return;
    }

    items.forEach(item => {
        try {
            container.appendChild(renderFunction(item));
        } catch (e) {
            console.error(`Error renderizando elemento en ${containerId}:`, e, item);
        }
    });
}

/* ------------------ CLIMA A PARTIR DE LOS CPs ------------------ */

function buildWeatherDataFromCps(cps) {
    const list = [];
    if (!Array.isArray(cps)) return list;

    cps.forEach(cp => {
        // Estado tal y como viene de CENTRAL
        const estado = (cp.estado || cp.status || '').toUpperCase();

        // Solo queremos CP que no estén DESCONECTADOS
        const cpEncendido = estado !== 'DESCONECTADO';
        if (!cpEncendido) return;

        const rawTemp = cp.tempC ?? cp.temperature;
        const hasTemp = typeof rawTemp === 'number' && !Number.isNaN(rawTemp);

        const alertFlag = !!(cp.weatherAlert ?? (hasTemp && rawTemp < 0));

        // Si no hay ni temperatura ni alerta, no pintamos tarjeta
        if (!hasTemp && !alertFlag) return;

        const city =
            cp.loc ||
            cp.location ||
            cp.ubicacion ||
            cp.cp ||
            cp.id ||
            'N/A';

        const tsMs = cp.weatherTs || cp.lastWeatherTs || cp.ts || null;
        const tsStr = tsMs ? new Date(tsMs).toLocaleString('es-ES') : '';

        list.push({
            cpId: cp.cp || cp.id || city,
            city,
            temperature: hasTemp ? rawTemp : null,
            alert: alertFlag,
            timestamp: tsStr,
            estado
        });
    });

    return list;
}

/* ------------------ RENDERIZADO DE TARJETAS ------------------ */

function renderCPCard(cp) {
    const div = document.createElement('div');
    div.className = 'card';

    const id = cp.cp || cp.id || 'N/A';
    const loc = cp.loc || cp.location || cp.ubicacion || 'N/A';
    const estado = cp.estado || cp.status || 'N/A';
    const sesion = cp.sesion || cp.session || '';
    const driver = cp.driver || '';
    const precio = (cp.precio != null) ? Number(cp.precio).toFixed(4) : 'N/A';
    const kwh = (cp.kwh != null) ? Number(cp.kwh).toFixed(5) : 0;
    const eur = (cp.eur != null) ? Number(cp.eur).toFixed(4) : 0;

    div.innerHTML = `
        <strong>ID:</strong> ${id}<br>
        <strong>Ubicación:</strong> ${loc}<br>
        <strong>Estado:</strong> ${estado}<br>
        <strong>Sesión:</strong> ${sesion || '—'}<br>
        <strong>kWh / €:</strong> ${kwh} / ${eur}<br>
        <strong>Precio:</strong> ${precio} €/kWh<br>
        <strong>Driver:</strong> ${driver || 'N/A'}<br>
    `;
    return div;
}

function renderDriverCard(driver) {
    const div = document.createElement('div');
    div.className = 'card';

    const id = driver.id || driver.driver || driver.driverId || 'N/A';
    const veh = driver.vehicle || driver.matricula || 'N/A';
    const estado = driver.status || driver.estado || 'N/A';
    const cpAsignado = driver.cp || driver.cpId || driver.assignedCp || 'Ninguno';

    div.innerHTML = `
        <strong>ID:</strong> ${id}<br>
        <strong>Vehículo:</strong> ${veh}<br>
        <strong>Estado:</strong> ${estado}<br>
        <strong>CP asignado:</strong> ${cpAsignado || 'Ninguno'}
    `;
    return div;
}

function renderWeatherCard(weather) {
    const div = document.createElement('div');
    div.className = 'card weather-card';
    if (weather.alert) div.classList.add('alert-card');

    const tempStr =
        weather.temperature == null || Number.isNaN(weather.temperature)
            ? 'N/A'
            : `${weather.temperature.toFixed(1)} °C`;

    div.innerHTML = `
        <strong>Ciudad/CP:</strong> ${weather.city || weather.cpId || 'N/A'}<br>
        <strong>Estado CP:</strong> ${weather.estado || 'N/A'}<br>
        <strong>Temperatura:</strong> ${tempStr}<br>
        <strong>Alerta:</strong> ${weather.alert ? 'ACTIVA (T < 0°C)' : 'No'}<br>
        <strong>Última actualización:</strong> ${weather.timestamp || ''}
    `;
    return div;
}

function renderAlertCard(alert) {
    const div = document.createElement('div');
    div.className = 'card alert-card';

    const ts = alert.ts || alert.timestamp || '';
    const tipo = alert.type || alert.eventType || 'N/A';
    const src = alert.src || alert.source || 'N/A';
    const msg = alert.msg || alert.message || alert.detail || '';

    const tsStr = ts
        ? new Date(ts).toLocaleString('es-ES')
        : (alert.timestampStr || '');

    div.innerHTML = `
        <strong>${tsStr}</strong><br>
        <strong>Tipo:</strong> ${tipo}<br>
        <strong>Origen:</strong> ${src}<br>
        <strong>Mensaje:</strong> ${msg}
    `;
    return div;
}

function renderAuditLog(log) {
    const div = document.createElement('div');
    div.className = 'card';

    const ts = log.ts || log.timestamp || '';
    const tsStr = ts
        ? new Date(ts).toLocaleString('es-ES')
        : (log.timestampStr || '');

    div.innerHTML = `
        <strong>Fecha/Hora:</strong> ${tsStr}<br>
        <strong>IP Origen:</strong> ${log.sourceIp || log.ip || 'N/A'}<br>
        <strong>Acción:</strong> ${log.action || log.event || 'N/A'}<br>
        <strong>Detalles:</strong> ${log.details || log.detail || ''}
    `;
    return div;
}

/* ------------------ ARRANQUE PERIÓDICO ------------------ */

// Carga inicial
updateInterface();
// Refresco cada 5 segundos
setInterval(updateInterface, 5000);