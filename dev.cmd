@echo off
rem Local dev server: starts PostgreSQL/Redis containers, then runs Spring Boot (stop: Ctrl + C)
cd /d "%~dp0"
chcp 65001 > nul
docker compose up -d postgres redis
call "%~dp0gradlew.bat" bootRun
pause
