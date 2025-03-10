const fs = require('fs');
const path = require('path');
const EventEmitter = require('events');

class FileWatcher extends EventEmitter {
    constructor(cacheManager) {
        super();
        this.cacheManager = cacheManager;
        this.watchedFiles = new Map(); // Map to store watched files and their watchers
        this.uploadsDir = path.join(__dirname, 'uploads');
        this.ensureUploadsDir();
        this.initWatcher();
    }

    ensureUploadsDir() {
        // Create uploads directory if it doesn't exist
        if (!fs.existsSync(this.uploadsDir)) {
            try {
                fs.mkdirSync(this.uploadsDir, { recursive: true });
                console.log('✓ Created uploads directory');
            } catch (error) {
                console.error('❌ Error creating uploads directory:', error);
            }
        }
    }

    initWatcher() {
        // Watch the uploads directory for changes
        try {
            console.log('🔍 Initializing directory watcher for:', this.uploadsDir);
            
            // Watch the uploads directory for file additions/deletions
            fs.watch(this.uploadsDir, (eventType, filename) => {
                if (!filename) return;
                
                const filePath = path.join(this.uploadsDir, filename);
                
                // Only process M3U/playlist files
                if (filename.endsWith('.txt') || filename.endsWith('.m3u') || filename.endsWith('.m3u8')) {
                    console.log(`📄 ${eventType === 'rename' ? 'File added/removed' : 'File changed'}: ${filename}`);
                    
                    // Check if the file exists (was added) or was deleted
                    if (fs.existsSync(filePath)) {
                        // File was added or modified
                        this.watchFile(filePath);
                        
                        // If this is a user_playlist.txt or starts with user_playlist_, trigger cache rebuild
                        if (filename === 'user_playlist.txt' || filename.startsWith('user_playlist_')) {
                            this.triggerCacheRebuild(filePath);
                        }
                    } else {
                        // File was deleted, remove from watched files
                        this.unwatchFile(filePath);
                    }
                }
            });
            
            // Scan existing files and start watching them
            this.scanExistingFiles();
            
            console.log('✓ Directory watcher initialized');
        } catch (error) {
            console.error('❌ Error initializing directory watcher:', error);
        }
    }

    scanExistingFiles() {
        try {
            if (fs.existsSync(this.uploadsDir)) {
                const files = fs.readdirSync(this.uploadsDir);
                let playlistFiles = 0;
                
                for (const file of files) {
                    if (file.endsWith('.txt') || file.endsWith('.m3u') || file.endsWith('.m3u8')) {
                        const filePath = path.join(this.uploadsDir, file);
                        this.watchFile(filePath);
                        playlistFiles++;
                    }
                }
                
                console.log(`✓ Found and watching ${playlistFiles} existing playlist files`);
            }
        } catch (error) {
            console.error('❌ Error scanning existing files:', error);
        }
    }

    watchFile(filePath) {
        // Don't watch if already watching
        if (this.watchedFiles.has(filePath)) return;
        
        try {
            // Get initial file stats
            const stats = fs.statSync(filePath);
            const initialMtime = stats.mtime.getTime();
            
            // Create a watcher for this specific file
            const watcher = fs.watchFile(filePath, { persistent: true, interval: 2000 }, (curr, prev) => {
                // Check if modification time has changed
                if (curr.mtime.getTime() !== prev.mtime.getTime()) {
                    console.log(`📄 File modified: ${path.basename(filePath)}`);
                    this.triggerCacheRebuild(filePath);
                }
            });
            
            // Store the watcher and initial mtime
            this.watchedFiles.set(filePath, { 
                watcher, 
                mtime: initialMtime,
                fileUrl: `file://${filePath}`
            });
            
            console.log(`🔍 Watching file: ${path.basename(filePath)}`);
        } catch (error) {
            console.error(`❌ Error watching file ${filePath}:`, error);
        }
    }

    unwatchFile(filePath) {
        if (this.watchedFiles.has(filePath)) {
            // Stop watching the file
            fs.unwatchFile(filePath);
            this.watchedFiles.delete(filePath);
            console.log(`✓ Stopped watching file: ${path.basename(filePath)}`);
        }
    }

    triggerCacheRebuild(filePath) {
        if (!this.cacheManager) return;
        
        const fileUrl = `file://${filePath}`;
        const currentConfig = this.cacheManager.config || {};
        
        // Only rebuild if the file is being used as the M3U source
        if (currentConfig.use_local_file === 'true') {
            console.log(`🔄 Triggering cache rebuild for modified file: ${path.basename(filePath)}`);
            
            // Update the m3u URL in the config to point to this file
            const updatedConfig = { ...currentConfig, m3u: fileUrl };
            
            // Rebuild the cache with the updated config
            this.cacheManager.rebuildCache(fileUrl, updatedConfig)
                .then(() => {
                    console.log(`✓ Cache rebuilt successfully for ${path.basename(filePath)}`);
                    this.emit('cacheRebuilt', { filePath, fileUrl });
                })
                .catch(error => {
                    console.error(`❌ Error rebuilding cache for ${path.basename(filePath)}:`, error);
                });
        }
    }

    // Get the most recent playlist file
    getMostRecentPlaylistFile() {
        try {
            if (!fs.existsSync(this.uploadsDir)) {
                return null;
            }
            
            // Find all playlist files
            const playlistFiles = fs.readdirSync(this.uploadsDir)
                .filter(file => file.startsWith('user_playlist_') && file.endsWith('.txt'))
                .map(file => {
                    const filePath = path.join(this.uploadsDir, file);
                    return {
                        name: file,
                        path: filePath,
                        time: fs.statSync(filePath).mtime.getTime()
                    };
                })
                .sort((a, b) => b.time - a.time);  // Sort by modification time (most recent first)
            
            // Return the path of the most recent file or null if there are no files
            return playlistFiles.length > 0 ? `file://${playlistFiles[0].path}` : null;
        } catch (error) {
            console.error('❌ Error getting most recent playlist file:', error);
            return null;
        }
    }

    // Stop watching all files
    stopAllWatchers() {
        for (const [filePath, { watcher }] of this.watchedFiles.entries()) {
            fs.unwatchFile(filePath);
        }
        this.watchedFiles.clear();
        console.log('✓ Stopped all file watchers');
    }
}

module.exports = FileWatcher;