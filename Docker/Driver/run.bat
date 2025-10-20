@echo off
set CP=app\driver-app.jar;lib\*
java -cp "%CP%" driver.EVDriver --config config\driver.config