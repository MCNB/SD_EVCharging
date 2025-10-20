@echo off
set CP=app\cp-engine.jar;lib\*
java -cp "%CP%" cp_engine.CPEngine --config config\engine.config