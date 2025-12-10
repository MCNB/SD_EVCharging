// ⬇️ Pon aquí la URL HTTP de tu EV_Central (puerto central.httpPort)
const API_BASE_URL = 'http://127.0.0.1:8080';

// Endpoints de la API de EV_Central
// - cps:     /api/cps  (fallback a /api/status si no existe)
// - drivers: /api/drivers
const ENDPOINTS = {
    cps: '/api/cps',
    cpsFallback: '/api/status',
    drivers: '/api/drivers'
};

// ---------- Utilidades de fetch / normalización -----------------

// Normaliza respuestas {items:[...]} → [...] o deja tal cual si ya es array
function normalizeItems(data) {
    if (!data) return null;
    if (Array.isArray(data)) return data;
    if (Array.isArray(data.items)) return data.items;
    return data;
}

// fetch genérico con posible endpoint de respaldo (fallback)
async function fetchData(endpoint, fallback) {
    try {
        const response = await fetch(`${API_BASE_URL}${endpoint}`, { cache: 'no-store' });
        if (!response.ok) {
            if (fallback) {
                console.warn(`[FRONT] Falló ${endpoint}, probando fallback ${fallback}`);
                const r2 = await fetch(`${API_BASE_URL}${fallback}`, { cache: 'no-store' });
                if (!r2.ok) throw new Error(`Error ${r2.status}: ${r2.statusText}`);
                const d2 = await r2.json();
                return normalizeItems(d2);
            }
            throw new Error(`Error ${response.status}: ${response.statusText}`);
        }
        const data = await response.json();
        return normalizeItems(data);
    } catch (error) {
        console.error(`Error obteniendo datos de ${endpoint}:`, error);
        return null;
    }
}

// Atajo para poner “No hay datos disponibles”
function showNoData(containerId) {
    const c = document.getElementById(containerId);
    if (!c) return;
    c.innerHTML = '<div class="card">No hay datos disponibles</div>';
}

// ---------- Construcción de datos derivados (clima, alertas) -----

// Deriva “datos de clima” a partir de la lista de CPs
function buildWeatherDataFromCps(cps) {
    const list = [];
    if (!Array.isArray(cps)) return list;

    cps.forEach(cp => {
        // Intentamos encontrar temperatura y alerta según vuestra API
        const rawTemp = cp.tempC ?? cp.temperature;
        const temp = rawTemp != null ? Number(rawTemp) : NaN;
        const hasTemp = !Number.isNaN(temp);
        const weatherAlertFlag = cp.weatherAlert ?? (hasTemp && temp < 0);

        if (!hasTemp && !weatherAlertFlag) {
            // si no hay temperatura ni alerta, no lo mostramos
            return;
        }

        const city =
            cp.location ||
            cp.loc ||
            cp.ubicacion ||
            cp.cp ||
            cp.id ||
            'N/A';

        const ts =
            cp.lastWeatherTs || // si lo tenéis en la API
            '';                 // si no, vacío

        list.push({
            city,
            temperature: hasTemp ? temp : null,
            alert: !!weatherAlertFlag,
            timestamp: ts || ''
        });
    });

    return list;
}

// Deriva “alertas” a partir de estados de CP y clima
function buildAlertsFromCps(cps) {
    const list = [];
    if (!Array.isArray(cps)) return list;

    const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

    cps.forEach(cp => {
        const cpId = cp.cp || cp.id || 'N/A';
        const estado = (cp.estado || cp.status || '').toUpperCase();
        const lag = typeof cp.lastHbMs === 'number' ? cp.lastHbMs : null;

        const rawTemp = cp.tempC ?? cp.temperature;
        const temp = rawTemp != null ? Number(rawTemp) : NaN;
        const hasTemp = !Number.isNaN(temp);
        const weatherAlertFlag = cp.weatherAlert ?? (hasTemp && temp < 0);

        // Estados “fuertes”
        if (estado === 'AVERIADO' || estado === 'DESCONECTADO') {
            list.push({
                timestamp: now,
                type: 'CP_STATE',
                source: cpId,
                message: `Estado ${estado}`
            });
        } else if (estado === 'PARADO') {
            list.push({
                timestamp: now,
                type: 'CP_STATE',
                source: cpId,
                message: 'CP parado'
            });
        }

        // Latido retrasado
        if (lag != null && lag > 5000 && estado !== 'DESCONECTADO') {
            list.push({
                timestamp: now,
                type: 'HEARTBEAT',
                source: cpId,
                message: `Latido retrasado (${lag} ms)`
            });
        }

        // Alerta de clima
        if (weatherAlertFlag) {
            list.push({
                timestamp: now,
                type: 'WEATHER',
                source: cpId,
                message: `Alerta de clima${hasTemp ? ` (T=${temp}°C)` : ''}`
            });
        }
    });

    return list;
}

// ---------- Actualizar secciones de la interfaz ------------------

// Actualiza un contenedor con datos usando una función de renderizado
function updateSection(containerId, data, renderFunction) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = '';
    if (Array.isArray(data) && data.length > 0) {
        data.forEach(item => container.appendChild(renderFunction(item)));
    } else if (typeof data === 'object' && data !== null) {
        container.appendChild(renderFunction(data));
    } else {
        container.innerHTML = '<div class="card">No hay datos disponibles</div>';
    }
}

// Función principal de refresco
async function updateInterface() {
    // 1) CPs (y a partir de ellos: clima + alertas)
    const cpsData = await fetchData(ENDPOINTS.cps, ENDPOINTS.cpsFallback);
    if (cpsData) {
        updateSection('cps-list', cpsData, renderCPCard);

        const weatherData = buildWeatherDataFromCps(cpsData);
        updateSection('weather-list', weatherData, renderWeatherCard);

        const alertsData = buildAlertsFromCps(cpsData);
        updateSection('alerts-list', alertsData, renderAlertCard);
    } else {
        showNoData('cps-list');
        showNoData('weather-list');
        showNoData('alerts-list');
    }

    // 2) Drivers
    const driversData = await fetchData(ENDPOINTS.drivers);
    if (driversData) {
        updateSection('drivers-list', driversData, renderDriverCard);
    } else {
        showNoData('drivers-list');
    }

    // 3) Auditoría (no tenemos /api/audit en esta implementación)
    const auditContainer = document.getElementById('audit-log');
    if (auditContainer) {
        auditContainer.innerHTML =
            '<div class="card">Funcionalidad de auditoría no disponible en esta instancia de EV_Central.</div>';
    }
}

// ---------- Render de tarjetas ----------------------------------

function renderCPCard(cp) {
    const div = document.createElement('div');
    div.className = 'card';

    const id =
        cp.id ||
        cp.cp ||
        cp.cpID ||
        'N/A';

    const location =
        cp.location ||
        cp.loc ||
        cp.ubicacion ||
        'N/A';

    const status =
        cp.status ||
        cp.estado ||
        'N/A';

    const sesion = cp.sesion || cp.session || '';
    const kwh = cp.kwh != null ? cp.kwh : '';
    const eur = cp.eur != null ? cp.eur : '';
    const precio = cp.precio != null ? cp.precio : '';

    const registeredFlag = cp.registered;
    const registeredText =
        registeredFlag === undefined ? 'N/A' : (registeredFlag ? 'Sí' : 'No');

    const token = cp.token || cp.key_b64 || 'N/A';

    div.innerHTML = `
        <strong>ID:</strong> ${id}<br>
        <strong>Ubicación:</strong> ${location}<br>
        <strong>Estado:</strong> ${status}<br>
        <strong>Sesión:</strong> ${sesion || '—'}<br>
        <strong>kWh / €:</strong> ${kwh || '0'} / ${eur || '0'}<br>
        <strong>Precio:</strong> ${precio || 'N/A'} €/kWh<br>
        <strong>Registrado:</strong> ${registeredText}<br>
        <strong>Token:</strong> ${token}
    `;
    return div;
}

function renderDriverCard(driver) {
    const div = document.createElement('div');
    div.className = 'card';

    const id = driver.id || driver.driver || 'N/A';
    const vehicle = driver.vehicle || 'N/A';
    const status = driver.status || 'N/A';
    const assignedCp = driver.assignedCp || driver.cp || 'Ninguno';

    div.innerHTML = `
        <strong>ID:</strong> ${id}<br>
        <strong>Vehículo:</strong> ${vehicle}<br>
        <strong>Estado:</strong> ${status}<br>
        <strong>CP asignado:</strong> ${assignedCp}
    `;
    return div;
}

function renderWeatherCard(weather) {
    const div = document.createElement('div');
    div.className = 'card weather-card';

    const alertClass = weather.alert ? 'alert-card' : '';
    if (alertClass) div.classList.add(alertClass);

    const tempText =
        weather.temperature == null || Number.isNaN(weather.temperature)
            ? 'N/A'
            : `${weather.temperature} °C`;

    div.innerHTML = `
        <strong>Ciudad/CP:</strong> ${weather.city}<br>
        <strong>Temperatura:</strong> ${tempText}<br>
        <strong>Alerta:</strong> ${weather.alert ? 'ACTIVA (T < 0°C)' : 'No'}<br>
        <strong>Última actualización:</strong> ${weather.timestamp || ''}
    `;
    return div;
}

function renderAlertCard(alert) {
    const div = document.createElement('div');
    div.className = 'card alert-card';
    div.innerHTML = `
        <strong>${alert.timestamp}</strong><br>
        <strong>Tipo:</strong> ${alert.type}<br>
        <strong>Origen:</strong> ${alert.source}<br>
        <strong>Mensaje:</strong> ${alert.message}
    `;
    return div;
}

function renderAuditLog(log) {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `
        <strong>Fecha/Hora:</strong> ${log.timestamp}<br>
        <strong>IP Origen:</strong> ${log.sourceIp}<br>
        <strong>Acción:</strong> ${log.action}<br>
        <strong>Detalles:</strong> ${log.details}
    `;
    return div;
}

// ---------- Bucle de refresco -----------------------------------

// Actualizar la interfaz cada 5 segundos
setInterval(updateInterface, 5000);

// Carga inicial
updateInterface();
