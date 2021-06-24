# Age of empires II DE Data Analysis, TP2: Middleware y Coordinación de Procesos, Sistemas Distribuidos I (75.74), FIUBA

Sistema distribuidos que procesa el detalle de partidas jugadas en el videojuego Age of Empires DE en base a la información provista en archivos CSV.

## Modo de uso

1. Construir las imagenes base de proyecto
```bash
cd base-images/ && ./build.sh
```

2. Agregar los datasets de entrada en una carpeta llamada datasets en la raíz de este proyecto. Los datasets de ejemplo se encuentran en https://www.kaggle.com/ezetowers/aoe2-tp2-draft/data.

3. Levantar el proyecto

```bash
cd ..
./start_up.sh n1 n2 n3 n4 n5
```

n1: Cantidad de group by match reducer
n2: Cantidad de join matches and players reducer 1v1
n3: Cantidad group players of matches by civ reducer 1v1
n4: Cantidad de join matches and players reducer team
n5: Cantidad group players of matches by civ reducer team

4. Al terminar la ejecución las salidas estarán impresas en la pantalla junto al resto de logs. Para ver solo las salidas usar

```bash
./watch_results.sh
```

El resultado esperado para los datasets de ejemplo se puede encontrar en:
1. long_matches_results.txt
2. weaker_winner_results.txt
3. winner_rate_by_civ_results.txt
4. top5_civs_results.txt

5. Para eliminar los containers de docker creados:

```bash
./stop_all.sh
```

## Configuraciones de despliegue
Son configurables desde `docker-compose-client-and-servers.yaml`

### Recomendadas

* `CHUCKSIZE_IN_LINES`: Permite modificar el tamaño de los chunks en los que se envían los archivos de entrada desde el cliente a los servidores. Menor tamaño significa más uso de colas.
* `PLAYERS_CHUNK_SIZE`: Permite modificar el tamaño de los chunks en los que se envían los players por key desde los group by master a los reducers. Menor tamaño significa más uso de colas.
* `ROWS_CHUNK_SIZE`: Permite modificar el tamaño de los chunks en los que se envían los matches y players por key desde el join master a los reducers. Menor tamaño significa más uso de colas.

### Otros
* Para el cliente, es posible configurar el indice en la que se encuentran cada una de las columnas en los archivos de entrada por si estos llegacen a cambiar entre datasets. Los mismos son los parametros: ENTRY_MATCH_TOKEN_INDEX , ENTRY_MATCH_AVERAGE_RATING_INDEX, ENTRY_MATCH_SERVER_INDEX, ENTRY_MATCH_DURATION_INDEX, ENTRY_MATCH_LADDER_INDEX, ENTRY_MATCH_MAP_INDEX, ENTRY_MATCH_MIRROR_INDEX, ENTRY_PLAYER_MATCH_INDEX, ENTRY_PLAYER_RATING_INDEX, ENTRY_PLAYER_WINNER_INDEX, ENTRY_PLAYER_CIV_INDEX
* Para los filtros, es posible configurar los valores por lo que se filtra, por si estos llegacen a cambiar de formato entre datasets o si se desea dar un comportamiento distinto a los mismos. Los mismos son: MINIMUM_AVERAGE_RATING, MINIMUM_DURATION, DURATION_FORMAT, KOREA_CENTRAL_SERVER, SOUTH_EAST_ASIA_SERVER, EAST_US_SERVER, MIN_RATING, LADDER_1V1, MAP_ARENA, NO_MIRROR, LADDER_TEAM, MAP_ISLANDS, MINIMUM_RATING, MINIMUM_RATING_PORCENTAGE_DIFF

## Requerimientos funcionales
El procesamiento de los datos debe brindar la siguiente información:
* IDs de matches que excedieron las dos horas de juego por pro players (average_rating > 2000) en los servers koreacentral, southeastasia y eastus
* IDs de matches en partidas 1v1 donde el ganador tiene un rating 30% menor al perdedor y el rating del ganador es superior a 1000
* Porcentaje de victorias por civilización en partidas 1v1 (ladder == RM_1v1) con civilizaciones diferentes en mapa arena
* Top 5 civilizaciones más usadas por pro players (rating > 2000) en team games (ladder == RM_TEAM) en mapa islands

## Requerimientos no funcionales

* El sistema debe estar optimizado para entornos multicomputadoras
* El sistema debe ser invocado desde un nodo que transmite los datos a ser procesados.
* Se debe soportar el escalamiento de los elementos de cómputo
* De ser necesaria una comunicación basada en grupos, se requiere la definición de un middleware
* El diseño debe permitir una fácil adaptación a otros datasets de partidas de Age of Empires II DE
* Debido a restricciones en el tiempo de implementación, se permite la construcción de un sistema acoplado al modelo de negocio. No es un requerimiento la creación de una plataforma de procesamiento de datos