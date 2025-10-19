@echo off
set CP=app\cp-monitor.jar;lib\*
java -cp "%CP%" cp_monitor.CPMonitor --config config\monitor.config