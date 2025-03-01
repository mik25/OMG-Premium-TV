# 🐳 Installation de OMG Premium TV via Docker
***[🇮🇹 Leggi in italiano](README.md)*** - ***[🇬🇧 Read in English](docker-install-en.md)*** - ***[🇫🇷 Lire en Français](docker-install-fr.md)*** - ***[🇪🇸 Leer en español](docker-install-es.md)***

## 🖥️ Pourquoi utiliser Docker ?

Installer OMG Premium TV via Docker offre plusieurs avantages :
- 🔒 Isolation de l'application
- 🔄 Mise à jour facile
- 📦 Pas de conflit avec d'autres applications
- 🚀 Déploiement rapide sur n'importe quelle plateforme

## 🌐 Option 1 : Installation sur VPS avec Portainer

[Portainer](https://www.portainer.io/) est une interface graphique pour gérer facilement les conteneurs Docker.

### 📋 Prérequis
- Un VPS avec Docker et Portainer installés
- Accès à l'interface web de Portainer

### 🚀 Procédure d'installation

1. **Connectez-vous à Portainer** 🔑
   - Ouvrez le navigateur et accédez à l'adresse de votre serveur Portainer (généralement `http://votre-serveur:9000`)
   - Saisissez vos identifiants de connexion

2. **Créez un Stack** 📚
   - Dans le menu latéral, allez sur **Stacks**
   - Cliquez sur **+ Add stack**
   - Attribuez un nom au stack (ex. `omg-premium-tv`)

3. **Configurez le Stack** ⚙️
   - Dans la section "Web editor", collez le fichier [docker-compose.portainer](docker-compose.portainer) présent dans le dépôt.

4. **Lancez le Stack** ▶️
   - Cliquez sur le bouton **Deploy the stack**
   - Portainer créera et démarrera automatiquement le conteneur

5. **Accédez à l'Addon** 🌐
   - Une fois démarré, accédez à l'interface web de OMG Premium TV à l'adresse :
   - `http://votre-serveur:7860`

## 🤗 Option 2 : Installation sur Hugging Face Spaces

[Hugging Face Spaces](https://huggingface.co/spaces) est une plateforme qui permet d'héberger gratuitement des applications web.

### 📋 Prérequis
- Un compte sur Hugging Face

### 🚀 Procédure d'installation

1. **Connectez-vous à Hugging Face** 🔑
   - Allez sur [https://huggingface.co/](https://huggingface.co/)
   - Connectez-vous à votre compte ou créez-en un nouveau

2. **Créez un nouveau Space** 🆕
   - Cliquez sur **New Space**
   - Choisissez un nom pour votre space (ex. `omg-premium-tv`)
   - Sélectionnez **Docker** comme type de Space
   - Choisissez la visibilité publique
   - Cliquez sur **Create Space**

3. **Configurez le Dockerfile** 📝
   - Créez un nouveau fichier nommé `Dockerfile`
   - Collez le contenu du fichier [Dockerfile.hf](Dockerfile.hf) présent dans le dépôt.

4. **Commit et Lancez le Déploiement** 📤
   - Cliquez sur **Commit changes to main**
   - Hugging Face lancera automatiquement le processus de build et de déploiement

5. **Accédez à l'Addon** 🌐
   - Une fois le déploiement terminé, accédez à l'interface web de OMG Premium TV à l'URL :
   - `https://huggingface.co/spaces/votre-nom-utilisateur/omg-premium-tv`

## ⚙️ Configuration de l'Addon

Après avoir installé l'addon via Docker, suivez ces étapes pour le configurer :

1. **Accédez à l'interface web** 🌐
   - Ouvrez le navigateur et allez à l'URL de votre addon (VPS ou Hugging Face)

2. **Configurez votre playlist M3U** 📋
   - Suivez les instructions dans la section "Configuration de base" de ce manuel

3. **Générez et copiez l'URL du manifest** 📝
   - Cliquez sur **COPIER URL MANIFEST**
   - Ou cliquez sur **INSTALLER SUR STREMIO** si vous utilisez le même appareil

4. **Ajoutez l'addon à Stremio** ➕
   - Ouvrez Stremio
   - Allez dans **Paramètres** > **Addon**
   - Cliquez sur **Ajouter un addon**
   - Collez l'URL du manifest
   - Cliquez sur **Installer**

## 🔧 Maintenance du conteneur Docker

### 🔄 Mise à jour de l'addon
Pour mettre à jour l'addon vers la dernière version :

#### Sur Portainer :
1. Allez au stack de OMG Premium TV
2. Cliquez sur **Stop** pour arrêter le stack
3. Cliquez sur **Remove** pour supprimer les conteneurs
4. Cliquez sur **Deploy** pour recréer les conteneurs avec la dernière version

#### Sur Hugging Face :
1. Modifiez le Dockerfile
2. Mettez à jour le paramètre `BRANCH` si nécessaire
3. Committez les modifications pour lancer la reconstruction

### 📊 Surveillance des logs
Pour visualiser les logs de l'addon :

#### Sur Portainer :
1. Allez à la section **Containers**
2. Trouvez le conteneur `omg-premium-tv`
3. Cliquez sur le nom du conteneur
4. Allez à l'onglet **Logs**

#### Sur Hugging Face :
1. Allez à votre Space
2. Cliquez sur l'onglet **Factory**
3. Visualisez les logs dans la section en bas

## ⚠️ Résolution des problèmes Docker

### 🛑 Le conteneur ne démarre pas
- Vérifiez que les ports ne sont pas déjà utilisés
- Consultez les logs pour d'éventuelles erreurs
- Assurez-vous que Docker dispose de ressources suffisantes (CPU/RAM)

### 🔌 Addon inaccessible
- Vérifiez que le pare-feu autorise le trafic sur le port 7860
- Vérifiez que le conteneur est en cours d'exécution
- Vérifiez que HOST et PORT sont correctement configurés

### 📵 Problèmes de réseau
- Assurez-vous que le conteneur a accès à Internet
- Vérifiez qu'il n'y a pas de restrictions réseau sur les appels externes

### 💾 Problèmes de persistance
- Pour conserver les données entre les redémarrages, vous pouvez essayer d'ajouter des volumes dédiés dans le docker-compose

```yaml
volumes:
  - ./data:/app/data
  - ./temp:/app/temp
```

Cela garantira que la configuration et les fichiers temporaires seront conservés même après le redémarrage du conteneur.

Si vous avez suivi toutes les étapes et n'avez pas d'erreurs, [vous pouvez maintenant retourner au guide principal](README-FR.md)
