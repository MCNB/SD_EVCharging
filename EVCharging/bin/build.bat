@echo off
setlocal enabledelayedexpansion
set DIR=%~dp0..
if not exist "%DIR%\out" mkdir "%DIR%\out"
(for /r "%DIR%\src" %%f in (*.java) do @echo %%f) > "%DIR%\sources.txt"
javac --release 21 -cp "%DIR%\lib\*" -d "%DIR%\out" @"%DIR%\sources.txt"
echo OK: clases compiladas en %DIR%\out