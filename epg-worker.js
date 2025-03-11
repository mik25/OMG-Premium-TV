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
        const isoString = `${year}-${month}-${day}T${hour}:${minute}:${second}${tzHours}:${tzMinutes}`;
        
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