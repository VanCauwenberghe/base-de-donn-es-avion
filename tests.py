from neo4j import GraphDatabase
import pandas as pd

airport = pd.read_csv("airports.dat")
routes = pd.read_csv("routes.dat")
compagnies = pd.read_csv("airlines.dat")

urlid = "bolt://52.90.115.19:7687"
id = "neo4j"
password = "motions-fruits-sounds"

driver = GraphDatabase.driver(urlid, auth=(id, password), encrypted=False)

def create_noeud_airport(session):
    query = """
        CREATE (:Airport {name: $name, city: $city, country: $country, id: $id, id_compagnie: $compagnie_id})
        """

    for index, row in airport.iterrows():
        parameters = {
            "id": row[4],
            "name": row[1],
            "city": row[2],
            "country": row[3],
            "compagnie_id": row[5]
        }
        session.run(query, parameters)

def create_relation_route(session):
    query = """
        MATCH (a:Airport {id: $id_dep})
        MATCH (b:Airport {id: $id_arr})
        MERGE (al:Airline {name: $airline})
        CREATE (a)-[:ROUTE {airline: $airline, name2: $name2}]->(b)
        """

    for index, row in routes.iterrows():
        parameters = {
            "id_dep": row[2],
            "id_arr": row[4],
            "airline": row[1],
            "name2": row[3]
        }
        session.run(query, parameters)

def shortest_path(session):
    query = """
        MATCH (d:Airport {city: $ville_depart})
        MATCH (a:Airport {city: $ville_arrivee})
        MATCH p = shortestPath((d)-[:ROUTE*]-(a))
        WHERE all(r IN relationships(p) WHERE type(r) = 'ROUTE')
        RETURN p
        """
    ville_depart = input("Entrer une ville de départ : ")
    ville_arrivee = input("Entrer une ville d'arrivée : ")

    parameters = {
        "ville_depart": ville_depart,
        "ville_arrivee": ville_arrivee,
    }

    result = session.run(query, parameters)
    return result

with driver.session() as session:
    create_noeud_airport(session)
    create_relation_route(session)
    shortest_path_result = shortest_path(session)

    for archives in shortest_path_result:
        path = archives["p"]
        airports_count = len(path.relationships) + 1
        print("Chemin le plus court entre les aéroports entrés:", airports_count)

driver.close()
# ====================================================================
#def create_link_airlines(session):
#    query = """
#        MATCH
#        MATCH
#        CREATE
#            """

#    for index, row in routes.iterrows():
#        route_id = row [1]

#    parametres_airl = {
#        "id_airlines": airlines_id,
#        "names_airlines": airlines_name,
#        "id_route": route_id,
#    }
#    session.run(query, parametres_airl)
