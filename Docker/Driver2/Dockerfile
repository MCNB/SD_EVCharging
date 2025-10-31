FROM eclipse-temurin:25-jre-jammy
WORKDIR /opt/app
COPY app/ ./app/
COPY lib/ ./lib/
RUN useradd -m appuser
USER appuser
# Modo manual (stdin/tty). Si usas fichero, ver compose.
CMD ["bash","-lc","java -cp app/driver-app.jar:lib/* driver.EVDriver --config /config/driver.config"]