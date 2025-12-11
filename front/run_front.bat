@echo off
setlocal

REM Puerto donde quieres servir el front
set PORT=3000

REM Ir a la carpeta public relativa a donde está este .bat
cd /d "%~dp0public"

echo ============================================
echo  Servidor front: http://localhost:%PORT%
echo  (Cierra esta ventana para parar el servidor)
echo ============================================
echo.

REM Abrir el navegador automáticamente
start "" "http://localhost:%PORT%"

REM Lanzar el servidor HTTP simple de Python
py -m http.server %PORT%

REM Si el servidor se cae con error, espera tecla para ver el mensaje
echo.
echo Servidor detenido. Pulsa una tecla para cerrar...
pause >nul