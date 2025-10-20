FROM eclipse-temurin:25-jre-jammy
WORKDIR /opt/app
# Copiamos jars “tal cual” de tu paquete
COPY app/ ./app/
COPY lib/ ./lib/
COPY config/central.config /opt/app/config/central.config
# Usuario no root
RUN useradd -m appuser
USER appuser
# Puertos: socket monitor y panel HTTP
EXPOSE 5000 8080
# Config por bind-mount en /config/central.config
CMD ["bash","-lc","java -cp app/central-app.jar:lib/* central.EVCentral --config /config/central.config"]
