@echo off
rem SixVM Token Proxy - keeps the server running and restarts it if it crashes.
rem Output goes to proxy.log next to this file.
cd /d "%~dp0"
:loop
node server.js >> proxy.log 2>&1
timeout /t 5 /nobreak >nul
goto loop
