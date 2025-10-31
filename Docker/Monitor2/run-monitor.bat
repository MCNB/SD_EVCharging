@echo off
set CP=app\CP_Monitor.jar;lib\*
java -cp "%CP%" cp_monitor.CPMonitor --config config\monitor.config