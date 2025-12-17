from pymongo import MongoClient
from dotenv import load_dotenv
import os
from faker import Faker
import random
from bson import json_util
import json

# ------------------------------
# Clase para gestionar la conexión a MongoDB
# ------------------------------


class MongoDBClient:
    def __init__(self):
        load_dotenv()
        self.mongo_username = os.getenv("MONGO_USERNAME")
        self.mongo_password = os.getenv("MONGO_PASSWORD")
        self.cluster_mongodb = os.getenv("CLUSTER_MONGODB")
        self.db_name = os.getenv("DB_NAME")
        self.collection_name = os.getenv("DB_COLLECTION")

        self.client = self.connect()
        self.db = self.get_database()
        self.collection = self.get_collection()

    def connect(self):
        try:
            client = MongoClient(
                f"mongodb+srv://{self.mongo_username}:{self.mongo_password}@{self.cluster_mongodb}.1gmm9ic.mongodb.net/"
            )
            client.admin.command("ping")
            print("Conexión a MongoDB exitosa ✅")
            return client
        except Exception as e:
            print(f"Error al conectar con MongoDB: {e}")
            return None

    def get_database(self):
        if self.client is not None:
            return self.client[self.db_name]
        return None

    def get_collection(self):
        if self.db is not None:
            return self.db[self.collection_name]
        return None


# ------------------------------
# Función para generar datos de series
# ------------------------------

fake = Faker("es_ES")

PLATAFORMAS = ["Netflix", "HBO", "Amazon Prime", "Disney+", "Apple TV+"]

GENEROS = [
    "Drama",
    "Comedia",
    "Thriller",
    "Sci-Fi",
    "Fantasía",
    "Acción",
    "Crimen",
    "Misterio",
]


def generar_series(n=50, incompletos=False):
    series = []
    titulos_usados = set()

    while len(series) < n:
        titulo = fake.catch_phrase()
        if titulo in titulos_usados:
            continue
        titulos_usados.add(titulo)

        temporadas = random.randint(1, 6)
        año_estreno = random.randint(2000, 2023)
        finalizada = random.choice([True, False])

        serie = {
            "titulo": titulo,
            "plataforma": random.choice(PLATAFORMAS),
            "temporadas": temporadas,
            "genero": random.sample(GENEROS, k=random.randint(1, 2)),
            "finalizada": finalizada,
            "año_estreno": año_estreno,
        }

        if not incompletos:
            serie["puntuacion"] = round(random.uniform(6.0, 9.5), 1)

        series.append(serie)

    return series


# ------------------------------
# Creación de la instancia y carga de datos
# ------------------------------


mongo = MongoDBClient()

# Eliminar la colección si existe
if mongo.collection is not None:
    mongo.collection.drop()
    print(f"Colección '{mongo.collection.name}' eliminada ✅")


# Generar e insertar series completas
series = generar_series(50)
result = mongo.collection.insert_many(series)

print(f"{len(result.inserted_ids)} series insertadas 🎬")

# Generar e insertar series incompletas
series_incompletas = generar_series(10, incompletos=True)
mongo.collection.insert_many(series_incompletas)
print(f"{len(series_incompletas)} series incompletas insertadas 🎬")


# ------------------------------
# Función para exportar resultados a JSON
# ------------------------------


def to_json(docs, filename=None):
    # Convertimos _id a string y generamos JSON
    json_data = json.dumps(
        [{**doc, "_id": str(doc["_id"])} for doc in docs],
        indent=2,
        ensure_ascii=False,
        default=json_util.default,
    )

    if filename:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(json_data)
        print(f"\nArchivo '{filename}' generado.")

    return json_data


# ------------------------------
# Consultas y exportación de resultados
# ------------------------------

maratones_json = []
maratones_largas = list(
    mongo.collection.find({"temporadas": {"$gt": 5}, "puntuacion": {"$gt": 8.0}})
)

print("\nSeries para maratones largas (5+ temporadas, puntuación >= 8.0):")
for serie in maratones_largas:
    serie["_id"] = str(serie["_id"])
    maratones_json.append(serie)
    print(
        f"- {serie['titulo']} ({serie['temporadas']} temporadas, Puntuación: {serie.get('puntuacion', 'N/A')})"
    )

to_json(maratones_json, filename="maratones.json")

joyas_comedia = list(
    mongo.collection.find({"genero": "Comedia", "año_estreno": {"$gt": 2020}})
)

print("\nJoyas de la comedia (estrenadas desde 2020):")
for serie in joyas_comedia:
    print(
        f"- {serie['titulo']} (Año de estreno: {serie['año_estreno']}), Género: {', '.join(serie['genero'])})"
    )
to_json(joyas_comedia, filename="comedias_recientes.json")

finalizadas = list(mongo.collection.find({"finalizada": True}))
print("\nSeries finalizadas:")
for serie in finalizadas:
    print(f"- {serie['titulo']} (Finalizada: {serie['finalizada']})")
to_json(finalizadas, filename="series_finalizadas.json")

series_netflix = list(mongo.collection.find({"plataforma": "Netflix"}))
print("\nSeries disponibles en Netflix:")
for serie in series_netflix:
    print(f"- {serie['titulo']} (Plataforma: {serie['plataforma']})")
to_json(series_netflix, filename="series_netflix.json")

joyas_apple = list(
    mongo.collection.find({"plataforma": "Apple TV+", "puntuacion": {"$gte": 9.0}})
)

print("\nJoyas en Apple TV+ (puntuación >= 9.0):")
for serie in joyas_apple:
    print(f"- {serie['titulo']} (Puntuación: {serie.get('puntuacion', 'N/A')})")
to_json(joyas_apple, filename="joyas_apple.json")

# ------------------------------
# Puntiación media de todas las series
# ------------------------------
query = [
    {"$match": {"puntuacion": {"$exists": True}}},
    {"$group": {"_id": None, "puntuacion_media": {"$avg": "$puntuacion"}}},
]

resultado = list(mongo.collection.aggregate(query))
if resultado:
    puntuacion_media = round(resultado[0]["puntuacion_media"], 2)
    print(f"\nPuntuación media de todas las series: {puntuacion_media}")

# ------------------------------
# Colección unificada
# ------------------------------

# Obtener primero la colección de series
series = list(mongo.collection.find({}))

# Cambiar a la nueva colección
mongo.collection = mongo.db["detalles_produccion"]

# Eliminar la colección si ya existe
mongo.collection.drop()
print(f"\nColección '{mongo.collection.name}' eliminada ✅")

# Crear documentos unificados
detalles = []

PAISES = ["EE.UU.", "Corea del Sur", "España", "Reino Unido"]

for serie in series:
    detalle = {
        "titulo": serie["titulo"],
        "pais_origen": random.choice(PAISES),
        "reparto_principal": [fake.name() for _ in range(3)],
        "presupuesto_por_episodio": round(random.uniform(0.5, 10.0), 2)  # en millones
    }
    detalles.append(detalle)

# Insertar en la nueva colección
mongo.collection.insert_many(detalles)
print(f"{len(detalles)} documentos insertados en 'detalles_produccion' ✅")

series_exitosas = list(mongo.collection.aggregate([
    {"$lookup": {
        "from": "series",
        "localField": "titulo",
        "foreignField": "titulo",
        "as": "serie_info"
    }},
    {"$unwind": "$serie_info"},
    {"$match": {
        "serie_info.finalizada": True,
        "serie_info.puntuacion": {"$exists": True, "$gt": 8},
        "pais_origen": "EE.UU."
    }}
]))

print("\nSeries exitosas finalizadas de EE.UU. con puntuación > 8:")
for serie in series_exitosas:
    print(f"- {serie['titulo']} (Puntuación: {serie['serie_info']['puntuacion']})")
    
to_json(series_exitosas, filename="series_exitosas_eeuu.json")

#------------------------------
# Calcular gasto financiero de ambas colecciones
#------------------------------

gasto_financiero = [
    {
        "$lookup": {
            "from": "detalles_produccion",
            "localField": "titulo",
            "foreignField": "titulo",
            "as": "detalles"
        }
    },
    {"$unwind": "$detalles"},
        {
        "$project": {
            "_id": 0,
            "titulo": 1,
            "coste_total": {
                "$multiply": [
                    "$temporadas",
                    8,
                    "$detalles.presupuesto_por_episodio"
                ]
            }
        }
    }
]


print("\nGasto financiero estimado por serie:")
gastos = list(mongo.db["series"].aggregate(gasto_financiero))
for gasto in gastos:
    print(f"- {gasto['titulo']}: ${gasto['coste_total']:.2f} millones")
    
# Limpiar _id (aunque normalmente no aparece si haces $project con "_id": 0)
gastos_sin_id = [{k: v for k, v in doc.items() if k != "_id"} for doc in gastos]

# Guardar en archivo JSON
with open("gasto_financiero_series.json", "w", encoding="utf-8") as f:
    json.dump(gastos_sin_id, f, indent=2, ensure_ascii=False)

print(f"{len(gastos_sin_id)} registros guardados en 'gasto_financiero_series.json' ✅")