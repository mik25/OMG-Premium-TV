# 🐳 Instalación de OMG Premium TV mediante Docker
***[🇮🇹 Leggi in italiano](README.md)*** - ***[🇬🇧 Read in English](docker-install-en.md)*** - ***[🇫🇷 Lire en Français](docker-install-fr.md)*** - ***[🇪🇸 Leer en español](docker-install-es.md)***

## 🖥️ ¿Por qué usar Docker?

Instalar OMG Premium TV mediante Docker ofrece varias ventajas:
- 🔒 Aislamiento de la aplicación
- 🔄 Fácil actualización
- 📦 Sin conflictos con otras aplicaciones
- 🚀 Despliegue rápido en cualquier plataforma

## 🌐 Opción 1: Instalación en VPS con Portainer

[Portainer](https://www.portainer.io/) es una interfaz gráfica para gestionar fácilmente los contenedores Docker.

### 📋 Requisitos previos
- Un VPS con Docker y Portainer instalados
- Acceso a la interfaz web de Portainer

### 🚀 Procedimiento de instalación

1. **Accede a Portainer** 🔑
   - Abre el navegador y ve a la dirección de tu servidor Portainer (típicamente `http://tu-servidor:9000`)
   - Introduce tus credenciales de acceso

2. **Crea un Stack** 📚
   - En el menú lateral, ve a **Stacks**
   - Haz clic en **+ Add stack**
   - Asigna un nombre al stack (ej. `omg-premium-tv`)

3. **Configura el Stack** ⚙️
   - En la sección "Web editor", pega el siguiente archivo [docker-compose.portainer](docker-compose.portainer) presente en el repositorio.

4. **Inicia el Stack** ▶️
   - Haz clic en el botón **Deploy the stack**
   - Portainer creará e iniciará automáticamente el contenedor

5. **Accede al Addon** 🌐
   - Una vez iniciado, accede a la interfaz web de OMG Premium TV en la dirección:
   - `http://tu-servidor:7860`

## 🤗 Opción 2: Instalación en Hugging Face Spaces

[Hugging Face Spaces](https://huggingface.co/spaces) es una plataforma que permite alojar aplicaciones web de forma gratuita.

### 📋 Requisitos previos
- Una cuenta en Hugging Face

### 🚀 Procedimiento de instalación

1. **Accede a Hugging Face** 🔑
   - Ve a [https://huggingface.co/](https://huggingface.co/)
   - Accede a tu cuenta o crea una nueva

2. **Crea un nuevo Space** 🆕
   - Haz clic en **New Space**
   - Elige un nombre para tu space (ej. `omg-premium-tv`)
   - Selecciona **Docker** como tipo de Space
   - Elige la visibilidad pública
   - Haz clic en **Create Space**

3. **Configura el Dockerfile** 📝
   - Crea un nuevo archivo llamado `Dockerfile`
   - Pega el contenido del archivo [Dockerfile.hf](Dockerfile.hf) presente en el repositorio.

4. **Commit e Inicia el Despliegue** 📤
   - Haz clic en **Commit changes to main**
   - Hugging Face iniciará automáticamente el proceso de build y despliegue

5. **Accede al Addon** 🌐
   - Una vez completado el despliegue, accede a la interfaz web de OMG Premium TV en la URL:
   - `https://huggingface.co/spaces/tu-nombre-usuario/omg-premium-tv`

## ⚙️ Configuración del Addon

Después de instalar el addon mediante Docker, sigue estos pasos para configurarlo:

1. **Accede a la interfaz web** 🌐
   - Abre el navegador y ve a la URL de tu addon (VPS o Hugging Face)

2. **Configura tu playlist M3U** 📋
   - Sigue las instrucciones en la sección "Configuración básica" de este manual

3. **Genera y copia la URL del manifiesto** 📝
   - Haz clic en **COPIAR URL MANIFIESTO**
   - O haz clic en **INSTALAR EN STREMIO** si estás usando el mismo dispositivo

4. **Añade el addon a Stremio** ➕
   - Abre Stremio
   - Ve a **Configuración** > **Addon**
   - Haz clic en **Añadir addon**
   - Pega la URL del manifiesto
   - Haz clic en **Instalar**

## 🔧 Mantenimiento del contenedor Docker

### 🔄 Actualización del addon
Para actualizar el addon a la última versión:

#### En Portainer:
1. Ve al stack de OMG Premium TV
2. Haz clic en **Stop** para detener el stack
3. Haz clic en **Remove** para eliminar los contenedores
4. Haz clic en **Deploy** para recrear los contenedores con la última versión

#### En Hugging Face:
1. Modifica el Dockerfile
2. Actualiza el parámetro `BRANCH` si es necesario
3. Haz commit de los cambios para iniciar la reconstrucción

### 📊 Monitorización de logs
Para visualizar los logs del addon:

#### En Portainer:
1. Ve a la sección **Containers**
2. Encuentra el contenedor `omg-premium-tv`
3. Haz clic en el nombre del contenedor
4. Ve a la pestaña **Logs**

#### En Hugging Face:
1. Ve a tu Space
2. Haz clic en la pestaña **Factory**
3. Visualiza los logs en la sección inferior

## ⚠️ Resolución de problemas Docker

### 🛑 El contenedor no se inicia
- Verifica que los puertos no estén ya en uso
- Comprueba los logs para posibles errores
- Asegúrate de que Docker tenga suficientes recursos (CPU/RAM)

### 🔌 Addon no accesible
- Verifica que el firewall permita el tráfico en el puerto 7860
- Comprueba que el contenedor esté en ejecución
- Verifica que HOST y PORT estén configurados correctamente

### 📵 Problemas de red
- Asegúrate de que el contenedor tenga acceso a internet
- Verifica que no haya restricciones de red en las llamadas externas

### 💾 Problemas de persistencia
- Para mantener los datos entre reinicios, puedes intentar añadir volúmenes dedicados en el docker-compose

```yaml
volumes:
  - ./data:/app/data
  - ./temp:/app/temp
```

Esto garantizará que la configuración y los archivos temporales se mantengan incluso después de reiniciar el contenedor.

Si has seguido todos los pasos y no tienes errores, [puedes ahora volver a la guía principal](README-ES.md)
