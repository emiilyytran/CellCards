import mysql.connector
from SPARQLWrapper import SPARQLWrapper, JSON
from mysql.connector import Error


def create_database_connection(host: str, port: int, user: str, password: str, database_name: str):
    try:
        connection = mysql.connector.connect(
            host=host,  # '127.0.0.1',
            port=port,  # 3307,
            user=user,  # 'admin',
            password=password,  # 'root',
            database=database_name,  # 'testdb'
        )

        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            return connection, cursor

    except Error as e:
        print("Error while connecting to MySQL", e)


connection, cursor = create_database_connection(host="127.0.0.1", port=3307, user="admin", password="root",
                                                database_name="testdb")


def create_database(connection, query):
    # connection cursor executes SQL statements with python
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


#     cell_id varchar(1000),
record = """ CREATE TABLE IF NOT EXISTS CELLCARDS (
    num int auto_increment primary key,
    label varchar(1000), 
    definition varchar(1000), 
    exactSynonyms varchar(1000), 
    broadSynonyms varchar(1000),
    partOf varchar(1000)
    );
"""
# execute the CREATE TABLE command
cursor.execute(record)

sparql = SPARQLWrapper("https://sparql.hegroup.org/sparql")
sparql.setReturnFormat(JSON)

sparql.setQuery("""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX obo-term: <http://purl.obolibrary.org/obo/>
PREFIX obo-owl: <http://www.geneontology.org/formats/oboInOwl#>
PREFIX kidney-cell: <http://purl.obolibrary.org/obo/CL_1000497>

SELECT ?label ?definition 
    (GROUP_CONCAT(DISTINCT ?exactSynonym; SEPARATOR=", ") AS ?exactSynonyms)
    (GROUP_CONCAT(DISTINCT ?broadSynonym; SEPARATOR=", ") AS ?broadSynonyms)
    ?partOf 

FROM <http://purl.obolibrary.org/obo/merged/CL>

WHERE {

   ?cell rdfs:subClassOf* kidney-cell: .

        ?cell  rdfs:label ?label.
        # Retrieve exact synonyms
        OPTIONAL { ?cell  obo-owl:hasExactSynonym ?exactSynonym. }

        # Retrieve broad synonyms
        OPTIONAL { ?cell obo-owl:hasBroadSynonym ?broadSynonym. }

        # Retrieve the definition
        OPTIONAL { ?cell  obo-term:IAO_0000115 ?definition. }
        # Retrieve "part of" information
        OPTIONAL {
            ?cell  rdfs:subClassOf ?restriction.
            ?restriction rdf:type owl:Restriction.
            ?restriction owl:onProperty obo-term:BFO_0000050.
            ?restriction owl:someValuesFrom ?part.
            ?part rdfs:label ?partOf.
        }

}
GROUP BY ?label ?definition ?partOf

""")

ret = sparql.query().convert()


def get_value_from_result(key: str, obj: object) -> None:
    if key in obj:
        return result[key]["value"]
    return None


for result in ret["results"]["bindings"]:
    label = result["label"]["value"]
    definition = get_value_from_result("definition", result)
    exactSynonyms = get_value_from_result("exactSynonyms", result)
    broadSynonyms = get_value_from_result("broadSynonyms", result)
    partOf = get_value_from_result("partOf", result)

    insert_query = ("INSERT INTO CELLCARDS (label,definition,"
                    "exactSynonyms,broadSynonyms,partOf) VALUES (%s, %s, %s, %s, %s)")
    insert_values = (label, definition, exactSynonyms, broadSynonyms, partOf)
    cursor.execute(insert_query, insert_values)
    connection.commit()

# def __main__():


#
# if connection.is_connected():
#    cursor.close()
#    connection.close()
#    print("MySQL connection is closed")
