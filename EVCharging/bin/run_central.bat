@echo off
setlocal enabledelayedexpansion
set DIR=%~dp0..
if "%1"=="" (set CONF=%DIR%\config\app.config) else (set CONF=%~1)
java -cp "%DIR%\out;%DIR%\lib\*" -Dlogback.configurationFile=%DIR%\config\logback.xml central.EVCentral --config "%CONF%"
