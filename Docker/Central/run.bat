@echo off
set CP=app\central-app.jar;lib\*
java -cp "%CP%" central.EVCentral --config config\central.config