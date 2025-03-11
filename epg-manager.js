const axios = require('axios');
const { parseStringPromise } = require('xml2js');
const zlib = require('zlib');
const { promisify } = require('util');
const gunzip = promisify(zlib.gunzip);
const cron = require('node-cron');

class EPGManager {
    constructor() {
        this.epgData = null;
        this.programGuide = new Map(); // Manteniamo per retrocompatibilità
        this.channelIcons = new Map(); // Manteniamo per retrocompatibilità
        this.lastUpdate = null;
        this.isUpdating = false;
        this.CHUNK_SIZE = 10000;
        this.lastEpgUrl = null;  // Proprietà per tracciare l'ultimo URL EPG
        this.cronJob = null;     // Proprietà per il job cron
        this.dbManager = null;   // Riferimento al database manager
        this.validateAndSetTimezone();
        this.initDatabaseManager();
    }
    
    async initDatabaseManager() {
        try {
            const EPGDatabaseManager = require('./epg-db-manager');
            this.dbManager = new EPGDatabaseManager();
            await this.dbManager.initialize();
            console.log('EPG Database Manager inizializzato con successo');
            
            // Recupera l'ultimo timestamp di aggiornamento
            const lastUpdateTime = await this.dbManager.getLastUpdateTime();
            if (lastUpdateTime) {
                this.lastUpdate = lastUpdateTime;
                console.log(`Ultimo aggiornamento EPG: ${new Date(lastUpdateTime).toLocaleString()}`);
            }
        } catch (error) {
            console.error('Errore nell\'inizializzazione del database manager:', error);
        }
    }

    normalizeId(id) {
        return id?.toLowerCase().replace(/[^\w.]/g, '').trim() || '';
    }

    validateAndSetTimezone() {
        const tzRegex = /^[+-]\d{1,2}:\d{2}$/;
        const timeZone = process.env.TIMEZONE_OFFSET || '+1:00';
        
        if (!tzRegex.test(timeZone)) {
            this.timeZoneOffset = '+1:00';
            return;
        }
        
        this.timeZoneOffset = timeZone;
        const [hours, minutes] = this.timeZoneOffset.substring(1).split(':');
        this.offsetMinutes = (parseInt(hours) * 60 + parseInt(minutes)) * 
                           (this.timeZoneOffset.startsWith('+') ? 1 : -1);
    }

    formatDateIT(date) {
        if (!date) return '';
        const localDate = new Date(date.getTime() + (this.offsetMinutes * 60000));
        return localDate.toLocaleString('it-IT', {
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
        }).replace(/\./g, ':');
    }

    parseEPGDate(dateString) {
        if (!dateString) return null;
        try {
            const regex = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\s*([+-]\d{4})$/;
            const match = dateString.match(regex);
            
            if (!match) return null;
            
            const [_, year, month, day, hour, minute, second, timezone] = match;
            const tzHours = timezone.substring(0, 3);
            const tzMinutes = timezone.substring(3);
            const isoString = `${year}-${month}-${day}T${hour}:${minute}:${second}${tzHours}:${tzMinutes}`;
            
            const date = new Date(isoString);
            return isNaN(date.getTime()) ? null : date;
        } catch (error) {
            console.error('Errore nel parsing della data EPG:', error);
            return null;
        }
    }

    async initializeEPG(url) {
    // Se l'URL è lo stesso e la guida non è vuota, skip
        if (this.lastEpgUrl === url && this.programGuide.size > 0) {
            console.log('EPG già inizializzato e valido, skip...');
            return;
        }

    // Se l'URL è cambiato o la guida è vuota, aggiorna
        console.log('\n=== Inizializzazione EPG ===');
        console.log('URL EPG:', url);
        this.lastEpgUrl = url;
        await this.startEPGUpdate(url);
        
    // Se non esiste già un cron job, crealo
        if (!this.cronJob) {
            console.log('Schedulazione aggiornamento EPG giornaliero alle 3:00');
            this.cronJob = cron.schedule('0 3 * * *', () => {
                console.log('Esecuzione aggiornamento EPG programmato');
                this.startEPGUpdate(this.lastEpgUrl);
            });
        }
        console.log('=== Inizializzazione EPG completata ===\n');
    }

    async downloadAndProcessEPG(epgUrl) {
        console.log('\nDownload EPG da:', epgUrl.trim());
        try {
            const response = await axios.get(epgUrl.trim(), { 
                responseType: 'arraybuffer',
                timeout: 100000,
                headers: {
                    'User-Agent': 'Mozilla/5.0',
                    'Accept-Encoding': 'gzip, deflate, br'
                }
            });
            
            let xmlString;
            try {
                xmlString = await gunzip(response.data);
                xmlString = xmlString.toString('utf8');
            } catch (gzipError) {
                try {
                    xmlString = zlib.inflateSync(response.data);
                    xmlString = xmlString.toString('utf8');
                } catch (zlibError) {
                    xmlString = response.data.toString('utf8');
                }
            }
            
            console.log('Inizio parsing XML...');
            const xmlData = await parseStringPromise(xmlString);
            console.log('Parsing XML completato');
            
            if (!xmlData || !xmlData.tv) {
                throw new Error('Struttura XML EPG non valida');
            }
            
            await this.processEPGInChunks(xmlData);
        } catch (error) {
            console.error(`❌ Errore EPG: ${error.message}`);
        }
    }

    async processEPGInChunks(data) {
        console.log('Inizio processamento EPG...');
        
        if (!data.tv) {
            console.error('❌ Errore: Nessun oggetto tv trovato nel file EPG');
            return;
        }

        // Verifica se il database manager è inizializzato
        if (!this.dbManager || !this.dbManager.isInitialized) {
            console.log('Database manager non inizializzato, utilizzo metodo legacy...');
            await this.processEPGInChunksLegacy(data);
            return;
        }

        try {
            // Pulisci il database prima di inserire nuovi dati
            await this.dbManager.clearDatabase();
            console.log('Database EPG pulito con successo');

            // Utilizza il database manager per processare i dati in parallelo
            await this.dbManager.processEPGDataInParallel(data);

            // Aggiorna anche le strutture dati in memoria per retrocompatibilità
            // Canali e icone
            if (data.tv && data.tv.channel) {
                console.log(`Aggiornamento cache in memoria per ${data.tv.channel.length} canali...`);
                data.tv.channel.forEach(channel => {
                    const id = channel.$.id;
                    const icon = channel.icon?.[0]?.$?.src;
                    if (id && icon) {
                        this.channelIcons.set(this.normalizeId(id), icon);
                    }
                });
            }

            console.log('✓ Processamento EPG completato con successo');
            this.lastUpdate = Date.now();
        } catch (error) {
            console.error('❌ Errore durante il processamento EPG con database:', error);
            console.log('Tentativo di fallback al metodo legacy...');
            await this.processEPGInChunksLegacy(data);
        }
    }

    // Manteniamo il metodo originale come fallback
    async processEPGInChunksLegacy(data) {
        console.log('Utilizzo processamento EPG legacy (in memoria)...');
        
        if (data.tv && data.tv.channel) {
            console.log(`Trovati ${data.tv.channel.length} canali nel file EPG`);
            data.tv.channel.forEach(channel => {
                const id = channel.$.id;
                const icon = channel.icon?.[0]?.$?.src;
                if (id && icon) {
                    this.channelIcons.set(this.normalizeId(id), icon);
                }
            });
        } else {
            console.error('❌ Errore: Nessun canale trovato nel file EPG');
        }

        if (!data.tv || !data.tv.programme) {
            console.error('❌ Errore: Nessun programma trovato nel file EPG');
            return;
        }

        const programs = data.tv.programme;
        let totalProcessed = 0;
        
        console.log(`\nProcessamento di ${programs.length} voci EPG in blocchi di ${this.CHUNK_SIZE}`);
        
        for (let i = 0; i < programs.length; i += this.CHUNK_SIZE) {
            const chunk = programs.slice(i, i + this.CHUNK_SIZE);
            
            for (const program of chunk) {
                const channelId = program.$.channel;
                const normalizedChannelId = this.normalizeId(channelId);

                if (!this.programGuide.has(normalizedChannelId)) {
                    this.programGuide.set(normalizedChannelId, []);
                }

                const start = this.parseEPGDate(program.$.start);
                const stop = this.parseEPGDate(program.$.stop);

                if (!start || !stop) continue;

                const programData = {
                    start,
                    stop,
                    title: program.title?.[0]?._ || program.title?.[0]?.$?.text || program.title?.[0] || 'Nessun Titolo',
                    description: program.desc?.[0]?._ || program.desc?.[0]?.$?.text || program.desc?.[0] || '',
                    category: program.category?.[0]?._ || program.category?.[0]?.$?.text || program.category?.[0] || ''
                };

                this.programGuide.get(normalizedChannelId).push(programData);
                totalProcessed++;
            }

            if ((i + this.CHUNK_SIZE) % 50000 === 0) {
                console.log(`Progresso: processate ${i + this.CHUNK_SIZE} voci...`);
            }
        }

        for (const [channelId, programs] of this.programGuide.entries()) {
            this.programGuide.set(channelId, programs.sort((a, b) => a.start - b.start));
        }

        console.log('Riepilogo Processamento EPG:');
        console.log(`✓ Totale voci processate: ${totalProcessed}`);
    }

    async readExternalFile(url) {
        if (Array.isArray(url)) {
            return url;
        }

        if (url.includes(',')) {
            return url.split(',').map(u => u.trim());
        }

        try {
            console.log('Tentativo lettura file:', url);
            
            if (url.endsWith('.gz')) {
                console.log('File gzipped EPG trovato');
                return [url];
            }
            
            const response = await axios.get(url.trim());
            const content = response.data;
            
            if (typeof content === 'string' && 
                (content.includes('<?xml') || content.includes('<tv'))) {
                console.log('File EPG trovato direttamente');
                return [url];
            }
            
            const urls = content.split('\n')
                .filter(line => line.trim() !== '' && line.startsWith('http'));
                
            if (urls.length > 0) {
                console.log('Lista URLs trovata:', urls);
                return urls;
            }
            
            console.log('Nessun URL trovato, uso URL originale');
            return [url];
            
        } catch (error) {
            console.error('Errore nella lettura del file:', error);
            return [url];
        }
    }

    async startEPGUpdate(url) {
        if (this.isUpdating) {
            console.log('⚠️  Aggiornamento EPG già in corso, skip...');
            return;
        }

        console.log('\n=== Inizio Aggiornamento EPG ===');
        const startTime = Date.now();

        try {
            this.isUpdating = true;
            console.log('Inizio lettura URLs EPG...');
            
            const epgUrls = await this.readExternalFile(url);
            console.log('URLs trovati:', epgUrls);

            this.programGuide.clear();
            this.channelIcons.clear();

            for (const epgUrl of epgUrls) {
                console.log('\nProcesso URL EPG:', epgUrl);
                await this.downloadAndProcessEPG(epgUrl);
            }

            const duration = ((Date.now() - startTime) / 1000).toFixed(1);
            console.log(`\n✓ Aggiornamento EPG completato in ${duration} secondi`);
            console.log(`✓ Totale canali con dati EPG: ${this.programGuide.size}`);
            console.log(`✓ Totale canali con icone: ${this.channelIcons.size}`);
            console.log('=== Aggiornamento EPG Completato ===\n');

        } catch (error) {
            console.error('❌ Errore dettagliato durante l\'aggiornamento EPG:', error);
            console.error('Stack:', error.stack);
        } finally {
            this.isUpdating = false;
            this.lastUpdate = Date.now();
        }
    }

    async getCurrentProgram(channelId) {
        if (!channelId) return null;
        
        // Se il database manager è disponibile, usa quello
        if (this.dbManager && this.dbManager.isInitialized) {
            try {
                return await this.dbManager.getCurrentProgram(channelId);
            } catch (error) {
                console.error('Errore nel recupero del programma corrente dal database:', error);
                // Fallback al metodo legacy
            }
        }
        
        // Metodo legacy (in memoria)
        const normalizedChannelId = this.normalizeId(channelId);
        const programs = this.programGuide.get(normalizedChannelId);
        
        if (!programs?.length) return null;

        const now = new Date();
        const currentProgram = programs.find(program => program.start <= now && program.stop >= now);
        
        if (currentProgram) {
            return {
                ...currentProgram,
                start: this.formatDateIT(currentProgram.start),
                stop: this.formatDateIT(currentProgram.stop)
            };
        }
        
        return null;
    }

    async getUpcomingPrograms(channelId) {
        if (!channelId) return [];
        
        // Se il database manager è disponibile, usa quello
        if (this.dbManager && this.dbManager.isInitialized) {
            try {
                return await this.dbManager.getUpcomingPrograms(channelId, 2);
            } catch (error) {
                console.error('Errore nel recupero dei programmi futuri dal database:', error);
                // Fallback al metodo legacy
            }
        }
        
        // Metodo legacy (in memoria)
        const normalizedChannelId = this.normalizeId(channelId);
        const programs = this.programGuide.get(normalizedChannelId);
        
        if (!programs?.length) return [];

        const now = new Date();
        
        return programs
            .filter(program => program.start >= now)
            .slice(0, 2)
            .map(program => ({
                ...program,
                start: this.formatDateIT(program.start),
                stop: this.formatDateIT(program.stop)
            }));
    }

    async getChannelIcon(channelId) {
        if (!channelId) return null;
        
        // Se il database manager è disponibile, usa quello
        if (this.dbManager && this.dbManager.isInitialized) {
            try {
                const icon = await this.dbManager.getChannelIcon(channelId);
                if (icon) return icon;
                // Se non trova l'icona nel database, fallback alla memoria
            } catch (error) {
                console.error('Errore nel recupero dell\'icona dal database:', error);
                // Fallback al metodo legacy
            }
        }
        
        // Metodo legacy (in memoria)
        return this.channelIcons?.get(this.normalizeId(channelId)) || null;
    }

    needsUpdate() {
        if (!this.lastUpdate) return true;
        return (Date.now() - this.lastUpdate) >= (24 * 60 * 60 * 1000);
    }

    isEPGAvailable() {
        return this.programGuide.size > 0 && !this.isUpdating;
    }

    getStatus() {
        return {
            isUpdating: this.isUpdating,
            lastUpdate: this.lastUpdate ? this.formatDateIT(new Date(this.lastUpdate)) : 'Mai',
            channelsCount: this.programGuide.size,
            iconsCount: this.channelIcons.size,
            programsCount: Array.from(this.programGuide.values())
                          .reduce((acc, progs) => acc + progs.length, 0),
            timezone: this.timeZoneOffset
        };
    }

    checkMissingEPG(m3uChannels) {
        const epgChannels = Array.from(this.programGuide.keys());
        const missingEPG = [];

        m3uChannels.forEach(ch => {
            const tvgId = ch.streamInfo?.tvg?.id;
            if (tvgId) {
                const normalizedTvgId = this.normalizeId(tvgId);
                if (!epgChannels.some(epgId => this.normalizeId(epgId) === normalizedTvgId)) {
                    missingEPG.push(ch);
                }
            }
        });

        if (missingEPG.length > 0) {
            console.log('\n=== Canali M3U senza EPG ===');
            missingEPG.forEach(ch => {
                console.log(`${ch.streamInfo?.tvg?.id}=`);
            });
            console.log(`✓ Totale canali M3U senza EPG: ${missingEPG.length}`);
            console.log('=============================\n');
        }
    }
}

module.exports = new EPGManager();
