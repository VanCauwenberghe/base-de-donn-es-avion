from neo4j import GraphDatabase
import pandas as pd

airport = pd.read_csv("airports.dat")
routes = pd.read_csv("routes.dat")
compagnies = pd.read_csv("airlines.dat")

urlid = "bolt://52.90.115.19:7687"
id = "neo4j"
password = "motions-fruits-sounds"

driver = GraphDatabase.driver(urlid, auth=(id, password), encrypted=False)
#====================================================================
#Fonctions
#====================================================================
def create_noeud_airport(session):
    query = """
        CREATE (:Airport {name: $name, city: $city, country: $country, id: $id, id_compagnie:$compagnie_id})
            """

    for index, row in airport.iterrows():
        name = row[1]
        city = row[2]
        country = row[3]
        airport_id = row[4]
        pilote_id = row[5]

        parameters = {
            "id": airport_id,
            "name": name,
            "city": city,
            "country": country,
        }
        session.run(query, parameters)

#====================================================================
def create_relation_route(session):
    query = """
               MATCH (a:Airport {id: $id_dep})
               MATCH (b:Airport {id: $id_arr})
               MERGE (al:Airline {noms: $compagnie})
               CREATE (a)-[:ROUTE {airline: $compagnie,noms: $noms}]->(b)
            """
    for index, row in routes.iterrows():
        airline = row[1]
        id_dep = row[2]
        id_arr = row[4]
        for index, row in compagnies.iterrows():
            compagnie = row[0]
            noms = row[1]
            if compagnie == airline:
                session.run(query, parameters_route)

        parameters_route = {
            "id_dep": depart,
            "id_arr": arrivee,
            "compagnie":compagnie,
            "airline":airline,
            "noms":noms
            }

        session.run(query, parameters_route)



#====================================================================
def shortest_path(session):
    query = """
    MATCH (d:Airport {city: $ville_depart})
    MATCH (a:Airport {city: $ville_arrivee})
    MATCH p = shortestPath((d)-[:ROUTE*]-(a))
    WHERE all(r IN relationships(p) WHERE type(r) = 'ROUTE')
    RETURN p
    """
    ville_depart = str(input("Entrer une ville de départ : "))
    ville_arrivee = str(input("Entrer une ville d'arrivée : "))

    parameters = {
        "ville_depart": ville_depart,
        "ville_arrivee": ville_arrivee,
    }

    result = session.run(query, parameters)
    return result

with driver.session() as session:
#    create_noeud_airport(session)
#    create_relation_route(session)
    shortest_path_result = shortest_path(session)

    for archives in shortest_path_result:
         path = archives ["p"]
         start_city = path.start_node["city"]
         end_city = path.end_node["city"]
         airports_count = len(path.relationships) + 1
         print("Chemin le plus court entre les aéroports entrés : ", airports_count)

driver.close()
