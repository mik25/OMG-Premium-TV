const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite3').Database;
const path = require('path');
const fs = require('fs');
const { Worker } = require('worker_threads');
const os = require('os');

class EPGDatabaseManager {
    constructor() {
        this.dbPath = path.join(__dirname, 'data', 'epg.db');
        this.db = null;
        this.isInitialized = false;
        this.workers = [];
        this.maxWorkers = Math.max(1, os.cpus().length - 1); // Leave one CPU for main thread
        this.ensureDataDirExists();
    }

    ensureDataDirExists() {
        const dataDir = path.join(__dirname, 'data');
        if (!fs.existsSync(dataDir)) {
            fs.mkdirSync(dataDir, { recursive: true });
        }
    }

    async initialize() {
        if (this.isInitialized) return;

        return new Promise((resolve, reject) => {
            // Ensure the data directory exists
            this.ensureDataDirExists();

            // Create or open the database
            this.db = new sqlite3.Database(this.dbPath, (err) => {
                if (err) {
                    console.error('Errore nell\'apertura del database:', err.message);
                    reject(err);
                    return;
                }

                console.log('Database EPG connesso con successo');
                this.setupDatabase()
                    .then(() => {
                        this.isInitialized = true;
                        resolve();
                    })
                    .catch(reject);
            });
        });
    }

    async setupDatabase() {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                // Create channels table
                this.db.run(`
                    CREATE TABLE IF NOT EXISTS channels (
                        id TEXT PRIMARY KEY,
                        name TEXT,
                        icon TEXT
                    )
                `, (err) => {
                    if (err) {
                        console.error('Errore nella creazione della tabella channels:', err.message);
                        reject(err);
                        return;
                    }
                });

                // Create programs table with indexes for efficient queries
                this.db.run(`
                    CREATE TABLE IF NOT EXISTS programs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        channel_id TEXT,
                        title TEXT,
                        description TEXT,
                        category TEXT,
                        start_time INTEGER,
                        end_time INTEGER,
                        FOREIGN KEY (channel_id) REFERENCES channels(id)
                    )
                `, (err) => {
                    if (err) {
                        console.error('Errore nella creazione della tabella programs:', err.message);
                        reject(err);
                        return;
                    }
                });

                // Create indexes for faster queries
                this.db.run(`CREATE INDEX IF NOT EXISTS idx_channel_id ON programs(channel_id)`, (err) => {
                    if (err) console.error('Errore nella creazione dell\'indice idx_channel_id:', err.message);
                });

                this.db.run(`CREATE INDEX IF NOT EXISTS idx_start_time ON programs(start_time)`, (err) => {
                    if (err) console.error('Errore nella creazione dell\'indice idx_start_time:', err.message);
                });

                this.db.run(`CREATE INDEX IF NOT EXISTS idx_end_time ON programs(end_time)`, (err) => {
                    if (err) console.error('Errore nella creazione dell\'indice idx_end_time:', err.message);
                });

                // Create metadata table for storing last update time and other info
                this.db.run(`
                    CREATE TABLE IF NOT EXISTS metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                `, (err) => {
                    if (err) {
                        console.error('Errore nella creazione della tabella metadata:', err.message);
                        reject(err);
                        return;
                    }
                    resolve();
                });
            });
        });
    }

    async clearDatabase() {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                this.db.run('DELETE FROM programs', (err) => {
                    if (err) {
                        console.error('Errore nella pulizia della tabella programs:', err.message);
                        reject(err);
                        return;
                    }
                });

                this.db.run('DELETE FROM channels', (err) => {
                    if (err) {
                        console.error('Errore nella pulizia della tabella channels:', err.message);
                        reject(err);
                        return;
                    }
                    resolve();
                });
            });
        });
    }

    async saveChannel(id, name, icon) {
        return new Promise((resolve, reject) => {
            const normalizedId = this.normalizeId(id);
            this.db.run(
                'INSERT OR REPLACE INTO channels (id, name, icon) VALUES (?, ?, ?)',
                [normalizedId, name, icon],
                function(err) {
                    if (err) {
                        console.error('Errore nel salvataggio del canale:', err.message);
                        reject(err);
                        return;
                    }
                    resolve(this.lastID);
                }
            );
        });
    }

    async saveProgram(channelId, title, description, category, startTime, endTime) {
        return new Promise((resolve, reject) => {
            const normalizedChannelId = this.normalizeId(channelId);
            this.db.run(
                'INSERT INTO programs (channel_id, title, description, category, start_time, end_time) VALUES (?, ?, ?, ?, ?, ?)',
                [normalizedChannelId, title, description, category, startTime.getTime(), endTime.getTime()],
                function(err) {
                    if (err) {
                        console.error('Errore nel salvataggio del programma:', err.message);
                        reject(err);
                        return;
                    }
                    resolve(this.lastID);
                }
            );
        });
    }

    async savePrograms(programs) {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                const stmt = this.db.prepare(
                    'INSERT INTO programs (channel_id, title, description, category, start_time, end_time) VALUES (?, ?, ?, ?, ?, ?)'
                );

                this.db.run('BEGIN TRANSACTION');

                for (const program of programs) {
                    const normalizedChannelId = this.normalizeId(program.channelId);
                    stmt.run(
                        normalizedChannelId,
                        program.title,
                        program.description,
                        program.category,
                        program.startTime.getTime(),
                        program.endTime.getTime()
                    );
                }

                stmt.finalize();

                this.db.run('COMMIT', (err) => {
                    if (err) {
                        console.error('Errore nel commit della transazione:', err.message);
                        reject(err);
                        return;
                    }
                    resolve();
                });
            });
        });
    }

    async getCurrentProgram(channelId) {
        return new Promise((resolve, reject) => {
            const normalizedChannelId = this.normalizeId(channelId);
            const now = Date.now();

            this.db.get(
                `SELECT p.*, c.name as channel_name, c.icon as channel_icon 
                FROM programs p 
                JOIN channels c ON p.channel_id = c.id 
                WHERE p.channel_id = ? AND p.start_time <= ? AND p.end_time >= ? 
                ORDER BY p.start_time ASC 
                LIMIT 1`,
                [normalizedChannelId, now, now],
                (err, row) => {
                    if (err) {
                        console.error('Errore nella ricerca del programma corrente:', err.message);
                        reject(err);
                        return;
                    }

                    if (!row) {
                        resolve(null);
                        return;
                    }

                    resolve({
                        title: row.title,
                        description: row.description,
                        category: row.category,
                        start: this.formatDateIT(new Date(row.start_time)),
                        stop: this.formatDateIT(new Date(row.end_time)),
                        channelName: row.channel_name,
                        channelIcon: row.channel_icon
                    });
                }
            );
        });
    }

    async getUpcomingPrograms(channelId, limit = 2) {
        return new Promise((resolve, reject) => {
            const normalizedChannelId = this.normalizeId(channelId);
            const now = Date.now();

            this.db.all(
                `SELECT p.*, c.name as channel_name, c.icon as channel_icon 
                FROM programs p 
                JOIN channels c ON p.channel_id = c.id 
                WHERE p.channel_id = ? AND p.start_time >= ? 
                ORDER BY p.start_time ASC 
                LIMIT ?`,
                [normalizedChannelId, now, limit],
                (err, rows) => {
                    if (err) {
                        console.error('Errore nella ricerca dei programmi futuri:', err.message);
                        reject(err);
                        return;
                    }

                    if (!rows || rows.length === 0) {
                        resolve([]);
                        return;
                    }

                    const programs = rows.map(row => ({
                        title: row.title,
                        description: row.description,
                        category: row.category,
                        start: this.formatDateIT(new Date(row.start_time)),
                        stop: this.formatDateIT(new Date(row.end_time)),
                        channelName: row.channel_name,
                        channelIcon: row.channel_icon
                    }));

                    resolve(programs);
                }
            );
        });
    }

    async getChannelIcon(channelId) {
        return new Promise((resolve, reject) => {
            const normalizedChannelId = this.normalizeId(channelId);

            this.db.get(
                'SELECT icon FROM channels WHERE id = ?',
                [normalizedChannelId],
                (err, row) => {
                    if (err) {
                        console.error('Errore nella ricerca dell\'icona del canale:', err.message);
                        reject(err);
                        return;
                    }

                    resolve(row ? row.icon : null);
                }
            );
        });
    }

    async getLastUpdateTime() {
        return new Promise((resolve, reject) => {
            this.db.get(
                'SELECT value FROM metadata WHERE key = "last_update"',
                (err, row) => {
                    if (err) {
                        console.error('Errore nella ricerca dell\'ultimo aggiornamento:', err.message);
                        reject(err);
                        return;
                    }

                    resolve(row ? parseInt(row.value) : null);
                }
            );
        });
    }

    async setLastUpdateTime(timestamp) {
        return new Promise((resolve, reject) => {
            this.db.run(
                'INSERT OR REPLACE INTO metadata (key, value) VALUES ("last_update", ?)',
                [timestamp.toString()],
                (err) => {
                    if (err) {
                        console.error('Errore nell\'aggiornamento del timestamp:', err.message);
                        reject(err);
                        return;
                    }
                    resolve();
                }
            );
        });
    }

    async processEPGDataInParallel(xmlData) {
        if (!xmlData || !xmlData.tv) {
            console.error('Struttura XML EPG non valida');
            return;
        }

        console.log('Inizio processamento EPG in parallelo...');
        const startTime = Date.now();

        // Process channels first (this is fast and can be done in the main thread)
        if (xmlData.tv.channel) {
            console.log(`Processamento di ${xmlData.tv.channel.length} canali...`);
            for (const channel of xmlData.tv.channel) {
                const id = channel.$.id;
                const name = channel.display_name?.[0]?._ || channel.display_name?.[0] || id;
                const icon = channel.icon?.[0]?.$?.src;
                if (id) {
                    await this.saveChannel(id, name, icon);
                }
            }
        }

        // Process programs in parallel using worker threads
        if (xmlData.tv.programme && xmlData.tv.programme.length > 0) {
            const programs = xmlData.tv.programme;
            const totalPrograms = programs.length;
            console.log(`Processamento di ${totalPrograms} programmi in parallelo...`);

            // Split the programs into chunks for each worker
            const chunkSize = Math.ceil(totalPrograms / this.maxWorkers);
            const chunks = [];

            for (let i = 0; i < totalPrograms; i += chunkSize) {
                chunks.push(programs.slice(i, Math.min(i + chunkSize, totalPrograms)));
            }

            // Create and start workers for each chunk
            const workerPromises = chunks.map((chunk, index) => {
                return this.processChunkWithWorker(chunk, index);
            });

            // Wait for all workers to complete
            const results = await Promise.all(workerPromises);

            // Combine and save all processed programs
            let allProcessedPrograms = [];
            for (const result of results) {
                allProcessedPrograms = allProcessedPrograms.concat(result);
            }

            // Save all programs to the database in batches
            const BATCH_SIZE = 1000;
            for (let i = 0; i < allProcessedPrograms.length; i += BATCH_SIZE) {
                const batch = allProcessedPrograms.slice(i, i + BATCH_SIZE);
                await this.savePrograms(batch);
                console.log(`Salvati ${Math.min(i + BATCH_SIZE, allProcessedPrograms.length)}/${allProcessedPrograms.length} programmi...`);
            }

            const duration = ((Date.now() - startTime) / 1000).toFixed(1);
            console.log(`Processamento EPG completato in ${duration} secondi`);
            console.log(`Totale programmi processati: ${allProcessedPrograms.length}`);
            
            // Update last update time
            await this.setLastUpdateTime(Date.now());
        }
    }

    async processChunkWithWorker(chunk, workerId) {
        return new Promise((resolve, reject) => {
            // Create a worker file path
            const workerFilePath = path.join(__dirname, 'epg-worker.js');
            
            // Check if worker file exists, if not create it
            if (!fs.existsSync(workerFilePath)) {
                const workerCode = `
                const { parentPort, workerData } = require('worker_threads');

                function parseEPGDate(dateString) {
                    if (!dateString) return null;
                    try {
                        const regex = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\s*([+-]\d{4})$/;
                        const match = dateString.match(regex);
                        
                        if (!match) return null;
                        
                        const [_, year, month, day, hour, minute, second, timezone] = match;
                        const tzHours = timezone.substring(0, 3);
                        const tzMinutes = timezone.substring(3);
                        const isoString = \`\${year}-\${month}-\${day}T\${hour}:\${minute}:\${second}\${tzHours}:\${tzMinutes}\`;
                        
                        const date = new Date(isoString);
                        return isNaN(date.getTime()) ? null : date;
                    } catch (error) {
                        return null;
                    }
                }

                function processPrograms(programs) {
                    const result = [];
                    
                    for (const program of programs) {
                        const channelId = program.$.channel;
                        const start = parseEPGDate(program.$.start);
                        const stop = parseEPGDate(program.$.stop);

                        if (!start || !stop) continue;

                        result.push({
                            channelId,
                            title: program.title?.[0]?._ || program.title?.[0]?.$?.text || program.title?.[0] || 'Nessun Titolo',
                            description: program.desc?.[0]?._ || program.desc?.[0]?.$?.text || program.desc?.[0] || '',
                            category: program.category?.[0]?._ || program.category?.[0]?.$?.text || program.category?.[0] || '',
                            startTime: start,
                            endTime: stop
                        });
                    }
                    
                    return result;
                }

                // Process the chunk of programs
                const processedPrograms = processPrograms(workerData.programs);
                
                // Send the result back to the main thread
                parentPort.postMessage(processedPrograms);
                `;
                
                fs.writeFileSync(workerFilePath, workerCode);
            }
            
            // Create a new worker
            const worker = new Worker(workerFilePath, {
                workerData: { programs: chunk }
            });
            
            // Store the worker reference
            this.workers.push(worker);
            
            // Handle worker messages
            worker.on('message', (processedPrograms) => {
                console.log(`Worker ${workerId} ha completato il processamento di ${processedPrograms.length} programmi`);
                resolve(processedPrograms);
            });
            
            // Handle worker errors
            worker.on('error', (error) => {
                console.error(`Errore nel worker ${workerId}:`, error);
                reject(error);
            });
            
            // Handle worker exit
            worker.on('exit', (code) => {
                this.workers = this.workers.filter(w => w !== worker);
                if (code !== 0) {
                    reject(new Error(`Worker ${workerId} terminato con codice di uscita ${code}`));
                }
            });
        });
    }

    async close() {
        return new Promise((resolve, reject) => {
            // Terminate all workers
            for (const worker of this.workers) {
                worker.terminate();
            }
            this.workers = [];
            
            // Close the database connection
            if (this.db) {
                this.db.close((err) => {
                    if (err) {
                        console.error('Errore nella chiusura del database:', err.message);
                        reject(err);
                        return;
                    }
                    this.isInitialized = false;
                    resolve();
                });
            } else {
                resolve();
            }
        });
    }

    normalizeId(id) {
        return id?.toLowerCase().replace(/[^\w.]/g, '').trim() || '';
    }

    formatDateIT(date) {
        if (!date) return '';
        const timeZoneOffset = process.env.TIMEZONE_OFFSET || '+1:00';
        const [hours, minutes] = timeZoneOffset.substring(1).split(':');
        const offsetMinutes = (parseInt(hours) * 60 + parseInt(minutes)) * 
                           (timeZoneOffset.startsWith('+') ? 1 : -1);
        
        const localDate = new Date(date.getTime() + (offsetMinutes * 60000));
        return localDate.toLocaleString('it-IT', {
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
        }).replace(/\./g, ':');
    }
}