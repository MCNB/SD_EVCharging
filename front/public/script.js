const API_BASE_URL = 'http://<IP_EV_Central>:<Puerto>'; // Reemplazar con IP y puerto reales de EV_Central

// Endpoints esperados de la API (ajustar según tu implementación Java)
const ENDPOINTS = {
    cps: '/api/cps',
    drivers: '/api/drivers',
    weather: '/api/weather',
    alerts: '/api/alerts',
    audit: '/api/audit'
};

// Función genérica para obtener datos de la API
async function fetchData(endpoint) {
    try {
        const response = await fetch(`${API_BASE_URL}${endpoint}`);
        if (!response.ok) throw new Error(`Error ${response.status}: ${response.statusText}`);
        return await response.json();
    } catch (error) {
        console.error(`Error obteniendo datos de ${endpoint}:`, error);
        return null;
    }
}

// Actualizar cada sección de la interfaz
async function updateInterface() {
    const cpsData = await fetchData(ENDPOINTS.cps);
    if (cpsData) updateSection('cps-list', cpsData, renderCPCard);

    const driversData = await fetchData(ENDPOINTS.drivers);
    if (driversData) updateSection('drivers-list', driversData, renderDriverCard);

    const weatherData = await fetchData(ENDPOINTS.weather);
    if (weatherData) updateSection('weather-list', weatherData, renderWeatherCard);

    const alertsData = await fetchData(ENDPOINTS.alerts);
    if (alertsData) updateSection('alerts-list', alertsData, renderAlertCard);

    const auditData = await fetchData(ENDPOINTS.audit);
    if (auditData) updateSection('audit-log', auditData, renderAuditLog);
}

// Actualizar un contenedor con datos usando una función de renderizado
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

// Funciones de renderizado para cada tipo de dato
function renderCPCard(cp) {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `
        <strong>ID:</strong> ${cp.id}<br>
        <strong>Ubicación:</strong> ${cp.location}<br>
        <strong>Estado:</strong> ${cp.status}<br>
        <strong>Registrado:</strong> ${cp.registered ? 'Sí' : 'No'}<br>
        <strong>Token:</strong> ${cp.token || 'N/A'}
    `;
    return div;
}

function renderDriverCard(driver) {
    const div = document.createElement('div');
    div.className = 'card';
    div.innerHTML = `
        <strong>ID:</strong> ${driver.id}<br>
        <strong>Vehículo:</strong> ${driver.vehicle}<br>
        <strong>Estado:</strong> ${driver.status}<br>
        <strong>CP asignado:</strong> ${driver.assignedCp || 'Ninguno'}
    `;
    return div;
}

function renderWeatherCard(weather) {
    const div = document.createElement('div');
    div.className = 'card weather-card';
    const alertClass = weather.temperature < 0 ? 'alert-card' : '';
    div.classList.add(alertClass);
    div.innerHTML = `
        <strong>Ciudad:</strong> ${weather.city}<br>
        <strong>Temperatura:</strong> ${weather.temperature} °C<br>
        <strong>Alerta:</strong> ${weather.alert ? 'ACTIVA (T < 0°C)' : 'No'}<br>
        <strong>Última actualización:</strong> ${weather.timestamp}
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

// Actualizar la interfaz cada 5 segundos
setInterval(updateInterface, 5000);

// Carga inicial
updateInterface();