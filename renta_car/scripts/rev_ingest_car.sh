#!/bin/bash

# Mensaje de inicio 
echo "****** Inicio Ingesta Rent a Car ******"

# Directorio landing en hadoop
LANDING_DIR="/home/hadoop/landing"

# Directorio destino en HDFS
DEST_DIR="/ingest4"

# Nombre archivos
rentacar="CarRentalData.csv"
georef="georef_usa.csv" # georef-united-states-of-america-state.csv

# Descarga archivos
wget -O "$LANDING_DIR/$rentacar" "https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv"
wget -O "$LANDING_DIR/$georef" "https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv/georef_usa.csv"

# Verifica si los archivos se descargaron correctamente
if [[ -f "$LANDING_DIR/$rentacar" && -f "$LANDING_DIR/$georef" ]]; then
  # Mover archivos a HDFS
  hdfs dfs -put "$LANDING_DIR/$rentacar" "$DEST_DIR"
  hdfs dfs -put "$LANDING_DIR/$georef" "$DEST_DIR"
  
  # Asignar permisos a los archivos
  hdfs dfs -chmod 644 "$DEST_DIR/$rentacar"
  hdfs dfs -chmod 644 "$DEST_DIR/$georef"
  
  # Asignar permisos al directorio
  hdfs dfs -chmod 755 "$DEST_DIR"
  
  # Remueve archivos locales, asegurando que existen antes
  rm -f "$LANDING_DIR/$rentacar"
  rm -f "$LANDING_DIR/$georef"
else
  echo "Error: Uno o ambos archivos no se descargaron correctamente."
fi

# Mensaje de finalización
echo "****** Fin Ingesta Rent a Car ******"


