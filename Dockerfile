FROM prefecthq/prefect:3.4.8-python3.11

# Installer les dépendances système de base avant le reste
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    apt-transport-https \
    ca-certificates \
    gcc \
    g++ \
    make \
    software-properties-common \
    unixodbc \
    unixodbc-dev

# Télécharger et installer le dépôt Microsoft
RUN curl -sSL -O https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb

# Installer les drivers ODBC et outils SQL
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 unixodbc-dev && \
    ln -s /opt/mssql-tools18/bin/* /usr/local/bin && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Ajouter les outils mssql-tools au PATH
ENV PATH="${PATH}:/opt/mssql-tools18/bin"

# Installe les composants nécessaires au backend (API + UI)
RUN pip install --upgrade pip && \
    pip install "prefect[server]"


# Copie le projet et installe les dépendances
WORKDIR directory-path
# Copier les fichiers
COPY . .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install pyarrow fastparquet


