# 🐳 Installing OMG Premium TV using Docker
***[🇮🇹 Leggi in italiano](README.md)*** - ***[🇬🇧 Read in English](docker-install-en.md)*** - ***[🇫🇷 Lire en Français](docker-install-fr.md)*** - ***[🇪🇸 Leer en español](docker-install-es.md)***

## 🖥️ Why use Docker?

Installing OMG Premium TV via Docker offers several advantages:
- 🔒 Application isolation
- 🔄 Easy updates
- 📦 No conflicts with other applications
- 🚀 Rapid deployment on any platform

## 🌐 Option 1: Installation on VPS with Portainer

[Portainer](https://www.portainer.io/) is a graphical interface for easily managing Docker containers.

### 📋 Prerequisites
- A VPS with Docker and Portainer installed
- Access to the Portainer web interface

### 🚀 Installation Procedure

1. **Access Portainer** 🔑
   - Open your browser and go to your Portainer server address (typically `http://your-server:9000`)
   - Enter your login credentials

2. **Create a Stack** 📚
   - In the side menu, go to **Stacks**
   - Click on **+ Add stack**
   - Assign a name to the stack (e.g., `omg-premium-tv`)

3. **Configure the Stack** ⚙️
   - In the "Web editor" section, paste the [docker-compose.portainer](docker-compose.portainer) file from the repository.

4. **Launch the Stack** ▶️
   - Click on the **Deploy the stack** button
   - Portainer will automatically create and start the container

5. **Access the Addon** 🌐
   - Once started, access the OMG Premium TV web interface at:
   - `http://your-server:7860`

## 🤗 Option 2: Installation on Hugging Face Spaces

[Hugging Face Spaces](https://huggingface.co/spaces) is a platform that allows you to host web applications for free.

### 📋 Prerequisites
- A Hugging Face account

### 🚀 Installation Procedure

1. **Log in to Hugging Face** 🔑
   - Go to [https://huggingface.co/](https://huggingface.co/)
   - Log in to your account or create a new one

2. **Create a new Space** 🆕
   - Click on **New Space**
   - Choose a name for your space (e.g., `omg-premium-tv`)
   - Select **Docker** as the Space type
   - Choose visibility public
   - Click on **Create Space**

3. **Configure the Dockerfile** 📝
   - Create a new file called `Dockerfile`
   - Paste the contents of the [Dockerfile.hf](Dockerfile.hf) file from the repository.

4. **Commit and Start Deployment** 📤
   - Click on **Commit changes to main**
   - Hugging Face will automatically start the build and deployment process

5. **Access the Addon** 🌐
   - Once deployment is complete, access the OMG Premium TV web interface at:
   - `https://huggingface.co/spaces/your-username/omg-premium-tv`

## ⚙️ Addon Configuration

After installing the addon via Docker, follow these steps to configure it:

1. **Access the web interface** 🌐
   - Open your browser and go to your addon URL (VPS or Hugging Face)

2. **Configure your M3U playlist** 📋
   - Follow the instructions in the "Basic Configuration" section of this manual

3. **Generate and copy the manifest URL** 📝
   - Click on **COPY MANIFEST URL**
   - Or click on **INSTALL ON STREMIO** if you're using the same device

4. **Add the addon to Stremio** ➕
   - Open Stremio
   - Go to **Settings** > **Addons**
   - Click on **Add addon**
   - Paste the manifest URL
   - Click on **Install**

## 🔧 Docker Container Maintenance

### 🔄 Updating the addon
To update the addon to the latest version:

#### On Portainer:
1. Go to the OMG Premium TV stack
2. Click on **Stop** to stop the stack
3. Click on **Remove** to remove the containers
4. Click on **Deploy** to recreate the containers with the latest version

#### On Hugging Face:
1. Edit the Dockerfile
2. Update the `BRANCH` parameter if necessary
3. Commit the changes to start the rebuild

### 📊 Log Monitoring
To view the addon logs:

#### On Portainer:
1. Go to the **Containers** section
2. Find the `omg-premium-tv` container
3. Click on the container name
4. Go to the **Logs** tab

#### On Hugging Face:
1. Go to your Space
2. Click on the **Factory** tab
3. View the logs in the section below

## ⚠️ Docker Troubleshooting

### 🛑 Container doesn't start
- Check that the ports are not already in use
- Check the logs for any errors
- Make sure Docker has sufficient resources (CPU/RAM)

### 🔌 Addon not reachable
- Verify that the firewall allows traffic on port 7860
- Check that the container is running
- Verify that HOST and PORT are set correctly

### 📵 Network problems
- Make sure the container has internet access
- Verify that there are no network restrictions on external calls

### 💾 Persistence problems
- To maintain data between restarts, you can try adding dedicated volumes in docker-compose

```yaml
volumes:
  - ./data:/app/data
  - ./temp:/app/temp
```

This will ensure that the configuration and temporary files are maintained even after restarting the container.

If you have followed all the steps and have no errors, [you can now return to the main guide](README-ES.md)
