# itba-pod-tpe2

Trabajo Práctico 2 para Programación de Objetos Distribuidos. ITBA 2Q 2021.



## Instalación

Ejecutar el siguiente comando de maven luego de clonar el proyecto y situarse dentro de este.

```bash
mvn clean install
```
Luego, en los directorios server/target y client/target se encuentran archvos .tar.gz que deben ser descomprimidos. Las carpetas obtenidas contienen los scripts para ejecutar las distintas partes del proyecto.

```bash
tar -xzvf server/target/tpe2-g2-server-1.0-SNAPSHOT-bin.tar.gz
tar -xzvf client/target/tpe2-g2-client-1.0-SNAPSHOT-bin.tar.gz
```


## Uso

Otorgar permisos a los scripts obtenidos al descomprimir el .tar.gz en el server, realizado en el paso anterior.

```bash
cd tpe2-g2-server-1.0-SNAPSHOT
chmod 777 ./server.sh
```

Luego encender el server ejecutando el bash disponible en la carpeta actual.

```bash
./server.sh
```

Para ejecutar los clientes de cada query debemos acceder al directorio generado al descomprimir su archivo al inicio. Deberemos también asignar los permisos correspondientes.

```bash
cd tpe2-g2-client-1.0-SNAPSHOT
chmod 777 query1.sh
chmod 777 query2.sh
chmod 777 query3.sh
chmod 777 query4.sh
chmod 777 query5.sh
```
Luego los clientes disponibles junto a su modo de uso son los siguientes:

#
```bash
./query1.sh -Dcity=XXX -Daddresses=xx.xx.xx.xx:yyyy -DinPath=PATHIN -DoutPath=PATHOUT
```
donde:
- XXX es BUE o VAN de acuerdo al conjunto de datos a procesar.
- xx.xx.xx.xx:yyyy es la dirección IP y el puerto donde está el servidor de Hazelcast.
- PATHIN es la ruta donde se encuentran los archivos arbolesBUE.csv, arbolesVAN.csv, barriosBUE.csv y barriosVAN.csv.
- PATHOUT es la ruta donde el cliente creará los archivos con los reportes.

#
```bash
./query2.sh -Dcity=XXX -Daddresses=xx.xx.xx.xx:yyyy -DinPath=PATHIN -DoutPath=PATHOUT
```
donde:
- XXX es BUE o VAN de acuerdo al conjunto de datos a procesar.
- xx.xx.xx.xx:yyyy es la dirección IP y el puerto donde está el servidor de Hazelcast.
- PATHIN es la ruta donde se encuentran los archivos arbolesBUE.csv, arbolesVAN.csv, barriosBUE.csv y barriosVAN.csv.
- PATHOUT es la ruta donde el cliente creará los archivos con los reportes.

#
```bash
./query3.sh -Dcity=XXX -Daddresses=xx.xx.xx.xx:yyyy -DinPath=PATHIN -DoutPath=PATHOUT -Dn=N
```
donde:
- XXX es BUE o VAN de acuerdo al conjunto de datos a procesar.
- xx.xx.xx.xx:yyyy es la dirección IP y el puerto donde está el servidor de Hazelcast.
- PATHIN es la ruta donde se encuentran los archivos arbolesBUE.csv, arbolesVAN.csv, barriosBUE.csv y barriosVAN.csv.
- PATHOUT es la ruta donde el cliente creará los archivos con los reportes.
- N es el número de barrios a listar.


#
```bash
./query4.sh -Dcity=XXX -Daddresses=xx.xx.xx.xx:yyyy -DinPath=PATHIN -DoutPath=PATHOUT
```
donde:
- XXX es BUE o VAN de acuerdo al conjunto de datos a procesar.
- xx.xx.xx.xx:yyyy es la dirección IP y el puerto donde está el servidor de Hazelcast.
- PATHIN es la ruta donde se encuentran los archivos arbolesBUE.csv, arbolesVAN.csv, barriosBUE.csv y barriosVAN.csv.
- PATHOUT es la ruta donde el cliente creará los archivos con los reportes.

#
```bash
./query5.sh -Dcity=XXX -Daddresses=xx.xx.xx.xx:yyyy -DinPath=PATHIN -DoutPath=PATHOUT -Dneighbourhood=NEIGHBOURHOOD
-DcommonName=COMMONNAME
```
donde:
- XXX es BUE o VAN de acuerdo al conjunto de datos a procesar.
- xx.xx.xx.xx:yyyy es la dirección IP y el puerto donde está el servidor de Hazelcast.
- PATHIN es la ruta donde se encuentran los archivos arbolesBUE.csv, arbolesVAN.csv, barriosBUE.csv y barriosVAN.csv.
- PATHOUT es la ruta donde el cliente creará los archivos con los reportes.
- NEIGHBOURHOOD es un string no vacío con el nombre del barrio.
- COMMONNAME es un string no vacío con el nombre de la especie.

