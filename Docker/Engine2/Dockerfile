FROM eclipse-temurin:25-jre-jammy
WORKDIR /opt/app
COPY app/cp-engine.jar ./app/
COPY app/cp-monitor.jar ./app/
COPY lib/ ./lib/
RUN useradd -m appuser
USER appuser
# Optional: expón 6100 si quieres testear el health externamente
EXPOSE 6100
# Arrancamos ambos procesos y gestionamos señales correctamente
COPY --chown=appuser:appuser entrypoint.sh /opt/app/entrypoint.sh
ENTRYPOINT ["/bin/bash","/opt/app/entrypoint.sh"]