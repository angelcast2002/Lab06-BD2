from neo4j import GraphDatabase
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

def encontrarUsuarioPelicula(user_id, movie_id):
    # encontrar un usuario con su relación rate a película.
    URI = "neo4j+s://36b45ea1.databases.neo4j.io"
    AUTH = ("neo4j", "AfNNtOdJULe-TqmSSjmBGru6JzSyRUfyKxVd_rDIVCg")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(f"MATCH (u:USER {{userId: {user_id}}})-[r:RATED]->(m:MOVIE {{movieId: {movie_id}}}) RETURN u, r, m")
            # imprimir el resultado con formato
            for record in result:
                print(f"User: {record['u']['name']} rated {record['m']['title']} with {record['r']['rating']} stars")
        

def createNodes(tiposNodo, propiedades):
    URI = "neo4j+s://36b45ea1.databases.neo4j.io"
    AUTH = ("neo4j", "AfNNtOdJULe-TqmSSjmBGru6JzSyRUfyKxVd_rDIVCg")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            for tipoNodo, propiedad in zip(tiposNodo, propiedades):
                session.run(f"CREATE (n:{tipoNodo} {propiedad})")
                print(f"Node {tipoNodo} created with properties {propiedad}")
        
def delete_all():
    URI = "neo4j+s://36b45ea1.databases.neo4j.io"
    AUTH = ("neo4j", "AfNNtOdJULe-TqmSSjmBGru6JzSyRUfyKxVd_rDIVCg")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("All nodes deleted")

def createRelations(nodes1, propertiesToSearch1, ids1, nodes2, propertiesToSearch2, ids2, relations, properties):
    URI = "neo4j+s://36b45ea1.databases.neo4j.io"
    AUTH = ("neo4j", "AfNNtOdJULe-TqmSSjmBGru6JzSyRUfyKxVd_rDIVCg")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            for node1, propertyToSearch1, id1, node2, propertyToSearch2, id2, relation, property in zip(nodes1, propertiesToSearch1, ids1, nodes2, propertiesToSearch2, ids2, relations, properties):
                session.run(f"MATCH (a:{node1} {{{propertyToSearch1}: {id1}}}), (b:{node2} {{{propertyToSearch2}: {id2}}}) CREATE (a)-[r:{relation} {property}]->(b)")
                print(f"Relation {relation} created between {node1} and {node2} with properties {property}")
    
def encontrarNodo(tipoNodo, parametroABuscar, id):
    URI = "neo4j+s://36b45ea1.databases.neo4j.io"
    AUTH = ("neo4j", "AfNNtOdJULe-TqmSSjmBGru6JzSyRUfyKxVd_rDIVCg")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(f"MATCH (n:{tipoNodo} {{{parametroABuscar}: {id}}}) RETURN n")
            for record in result:
                print(f"Node: {tipoNodo} with properties {record['n'].items()}")
        
def script2():
    nodos = ["Person:Actor:Director", "Person:Actor", "Person:Director", "Movie", "User", "Genre"]
    proiedadesNodos = [
        "{name: 'Carlitos', tmdbId: 1, born: datetime('2024-01-09T15:30:00'), died: datetime('2024-04-09T15:30:00'), bornIn: 'Mexico', url: 'https://www.google.com', imdbId: 1, bio: 'Un actor muy famoso', poster: 'https://www.google.com'}", 
        "{name: 'Juan', tmdbId: 2, born: datetime('2022-01-09T15:30:00'), died: datetime('2024-04-09T15:30:00'), bornIn: 'Guatemala', url: 'https://www.google.com', imdbId: 2, bio: 'Un actor no tan famoso', poster: 'https://www.google.com'}",
        "{name: 'Melvin', tmdbId: 3, born: datetime('1999-03-09T15:30:00'), died: datetime('2023-06-09T15:30:00'), bornIn: 'United States', url: 'https://www.google.com', imdbId: 3, bio: 'Un actor más o menos famoso', poster: 'https://www.google.com'}",
        "{title: 'Godzilla', tmdbId: 4, released: datetime('2016-04-09T15:30:00'), imbdRating: 4.5, movieId: 4, year: 2016, imdbId: 4, runtime: 120, countries: ['United States', 'Mexico', 'Guatemala'], imbdVotes: 4, url: 'https://www.google.com', revenue: 160000000, plot: 'Big Scary Monster destroys Japan', poster: 'https://www.google.com', budget: 50000000, languages: ['English', 'Spanish']}",
        "{name: 'Sonic', userId: 5}",
        "{name: 'Acción'}"
        ]
    
    createNodes(nodos, proiedadesNodos)

    propiedadesABuscar = ["tmdbId", "tmdbId", "tmdbId", "tmdbId", "userId", "movieId"]
    propiedadesABuscar2 = ["movieId", "movieId", "movieId", "movieId", "movieId", "name"]
    nodo = ["Person:Actor:Director", "Person:Actor:Director", "Person:Actor", "Person:Director", "User", "Movie"]
    nodo2 = ["Movie", "Movie", "Movie", "Movie", "Movie", "Genre"]
    ids1 = [1, 1, 2, 3, 5, 4]
    ids2 = [4, 4, 4, 4, 4, "'Acción'"]
    relations = ["ACTED_IN", "DIRECTED", "ACTED_IN", "DIRECTED", "RATED", "IN_GENRE"]
    
    proiedadesRelacion = [
        "",
        "{role: 'Director'}",
        "{role: 'Capitan America'}",
        "{role: 'Director'}",
        "{rating: 1, timestamp: timestamp('2024-04-09T15:30:00')}",
        ""
        ]
    
    createRelations(nodo, propiedadesABuscar, ids1, nodo2, propiedadesABuscar2, ids2, relations, proiedadesRelacion)

def script1():
    nodos = ["USER", "USER", "USER", "USER", "USER", "MOVIE", "MOVIE", "MOVIE"]
    proiedadesNodos = [
        "{name: 'Carlitos', userId: 1}",
        "{name: 'Enrique', userId: 2}",
        "{name: 'Dolores', userId: 3}",
        "{name: 'Pancracio', userId: 4}",
        "{name: 'Sonic', userId: 5}",
        "{title: 'Gigantes de acero', movieId: 1, year: 2011, plot: 'Una buena movie', countries: ['United States', 'Mexico', 'Guatemala', 'Mexico', 'Canada', 'Canada']}",
        "{title: 'El señor de los anillos', movieId: 2, year: 2001, plot: 'Una buena movie', countries: ['United States', 'Mexico', 'China', 'Guatemala', 'Canada']}",
        "{title: 'Warhammer 40K: La caida de Cadia', movieId: 3, year: 2024, plot: 'The last stand of Cadia', countries: ['United States', 'Mexico', 'Guatemala', 'Canada']}",
        ]
   
    createNodes(nodos, proiedadesNodos)
    propiedadesABuscar = ["userId"] * 10
    propiedadesABuscar2 = ["movieId"] * 10
    nodo = ["USER"] * 10
    nodo2 = ["MOVIE"] * 10
    ids1 = [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]
    ids2 = [1, 2, 3, 2, 1, 3, 1, 2, 3, 2]
    relations = ["RATED", "RATED", "RATED", "RATED", "RATED", "RATED", "RATED", "RATED", "RATED", "RATED"]
   
    proiedadesRelacion = [
        "{rating: 3, timestamp: timestamp('2024-04-09T15:30:00')}",
        "{rating: 4, timestamp: timestamp('2024-04-09T15:30:00')}",
        "{rating: 5, timestamp: timestamp('2024-04-09T15:30:00')}",
        "{rating: 2, timestamp: timestamp('2024-04-09T15:30:00')}",
        "{rating: 1, timestamp: timestamp('2024-04-09T15:30:00')}",
        "{rating: 2, timestamp: timestamp('2024-04-09T15:30:00')}",
        "{rating: 3, timestamp: timestamp('2024-04-09T15:30:00')}",
        "{rating: 4, timestamp: timestamp('2024-04-09T15:30:00')}",
        "{rating: 5, timestamp: timestamp('2024-04-09T15:30:00')}",
        "{rating: 6, timestamp: timestamp('2024-04-09T15:30:00')}",
        ]
   
    createRelations(nodo, propiedadesABuscar, ids1, nodo2, propiedadesABuscar2, ids2, relations, proiedadesRelacion)

def calcularPromedioPelicula(movie_id):
    # Calcular el promedio de una película
    URI = "neo4j+s://36b45ea1.databases.neo4j.io"
    AUTH = ("neo4j", "AfNNtOdJULe-TqmSSjmBGru6JzSyRUfyKxVd_rDIVCg")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(f"MATCH (m:MOVIE {{movieId: {movie_id}}})<-[r:RATED]-(u:USER) RETURN avg(r.rating) as promedio")
            for record in result:
                print(f"The average rating of the movie is {record['promedio']}")

def calcularMaximoPelicula(movie_id):
    # Calcular el máximo de una película
    URI = "neo4j+s://36b45ea1.databases.neo4j.io"
    AUTH = ("neo4j", "AfNNtOdJULe-TqmSSjmBGru6JzSyRUfyKxVd_rDIVCg")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(f"MATCH (m:MOVIE {{movieId: {movie_id}}})<-[r:RATED]-(u:USER) RETURN max(r.rating) as maximo")
            for record in result:
                print(f"The maximum rating of the movie is {record['maximo']}")

def calcularMinimoPelicula(movie_id):
    # Calcular el mínimo de una película
    URI = "neo4j+s://36b45ea1.databases.neo4j.io"
    AUTH = ("neo4j", "AfNNtOdJULe-TqmSSjmBGru6JzSyRUfyKxVd_rDIVCg")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(f"MATCH (m:MOVIE {{movieId: {movie_id}}})<-[r:RATED]-(u:USER) RETURN min(r.rating) as minimo")
            for record in result:
                print(f"The minimum rating of the movie is {record['minimo']}")

def calcularPromedioTodaBase():
    # Calcular el promedio de todas las películas
    URI = "neo4j+s://36b45ea1.databases.neo4j.io"
    AUTH = ("neo4j", "AfNNtOdJULe-TqmSSjmBGru6JzSyRUfyKxVd_rDIVCg")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(f"MATCH (m:MOVIE)<-[r:RATED]-(u:USER) RETURN avg(r.rating) as promedio")
            for record in result:
                print(f"The average rating of the movies is {record['promedio']}")

def calcularSumaRating():
    # Calcular la suma de los ratings
    URI = "neo4j+s://36b45ea1.databases.neo4j.io"
    AUTH = ("neo4j", "AfNNtOdJULe-TqmSSjmBGru6JzSyRUfyKxVd_rDIVCg")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(f"MATCH (m:MOVIE)<-[r:RATED]-(u:USER) RETURN sum(r.rating) as suma")
            for record in result:
                print(f"The sum of the ratings is {record['suma']}")
    
if __name__ == "__main__":
    delete_all()
    script1()
    #script2()
    #encontrarNodo("USER", "userId", 1)
    #encontrarNodo("MOVIE", "movieId", 1)
    #encontrarUsuarioPelicula(1, 1)
    calcularPromedioPelicula(1)
    calcularMaximoPelicula(1)
    calcularMinimoPelicula(1)
    calcularPromedioTodaBase()
    calcularSumaRating()