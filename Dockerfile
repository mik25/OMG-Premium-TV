# Usa un'immagine Node.js come base
FROM node:16-slim

# Imposta la directory di lavoro
WORKDIR /app

# Installa git, Python, pip, curl e sqlite3
RUN apt-get update && \
    apt-get install -y git python3 python3-pip curl sqlite3 && \
    pip3 install requests && \
    rm -rf /var/lib/apt/lists/*

# Copia i file del progetto
COPY package.json package-lock.json ./

# Installa le dipendenze incluso sqlite3
RUN npm install && npm install sqlite3

# Copia il resto del codice
COPY . .

# Crea directory per i dati e imposta i permessi
RUN mkdir -p /app/data && chown -R node:node /app/data

# Crea la directory temp e imposta i permessi
RUN mkdir -p /app/temp && \
    chmod 777 /app/temp
RUN mkdir -p /app/uploads && \
    chmod 777 /app/uploads

# Esponi la porta 7860 (usata dal server)
EXPOSE 7860

# Avvia l'add-on
CMD ["node", "index.js"]
