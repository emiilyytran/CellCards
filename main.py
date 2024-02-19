import mysql.connector
from SPARQLWrapper import SPARQLWrapper, JSON
from mysql.connector import Error


# Database connection function
def create_database_connection(host, port, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            cursor = connection.cursor()
            return connection, cursor
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None, None


# Function to get value from result safely
def get_value_from_result(key, result):
    return result[key]["value"] if key in result else None

def perform_sparql_query(cell_iri):
    sparql = SPARQLWrapper("https://sparql.hegroup.org/sparql")
    sparql.setReturnFormat(JSON)
    d = "http://purl.obolibrary.org/obo/" + cell_iri


    query_template = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX obo-term: <http://purl.obolibrary.org/obo/>
    PREFIX obo-owl: <http://www.geneontology.org/formats/oboInOwl#>

    SELECT ?label ?definition 
        (GROUP_CONCAT(DISTINCT ?exactSynonym; SEPARATOR=", ") AS ?exactSynonyms)
        (GROUP_CONCAT(DISTINCT ?broadSynonym; SEPARATOR=", ") AS ?broadSynonyms)
        ?partOf 

    FROM <http://purl.obolibrary.org/obo/merged/CL>

    WHERE {

       ?cell rdfs:subClassOf* <{d}> .

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
    """

    query = query_template.format(d=d)
    sparql.setQuery(query)

    try:
        results = sparql.query().convert()
        return results
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


# Main script execution
if __name__ == "__main__":
    connection, cursor = create_database_connection("127.0.0.1", 3307, "admin", "root", "testdb")

    # Check if connection was successful
    if connection is not None and cursor is not None:
        try:
            # Define and execute a SQL statement that creates the table
            create_table_query = """CREATE TABLE IF NOT EXISTS CELLCARDS (
                num INT AUTO_INCREMENT PRIMARY KEY,
                label VARCHAR(1000), 
                definition TEXT, 
                exactSynonyms TEXT, 
                broadSynonyms TEXT,
                partOf VARCHAR(1000)
            );"""
            cursor.execute(create_table_query)

            # Set up SPARQL query
            cell_iri = input("Enter the cell iri: ")
            results = perform_sparql_query(cell_iri)


            if results:  # Check if there are any results
                # Iterating over query results and insert into the SQL table
                insert_query = ("INSERT INTO CELLCARDS (label, definition, exactSynonyms, broadSynonyms, partOf) "
                                "VALUES (%s, %s, %s, %s, %s)")

                for result in results["results"]["bindings"]:
                    label = get_value_from_result("label", result)
                    definition = get_value_from_result("definition", result)
                    exactSynonyms = get_value_from_result("exactSynonyms", result)
                    broadSynonyms = get_value_from_result("broadSynonyms", result)
                    partOf = get_value_from_result("partOf", result)

                    insert_values = (label, definition, exactSynonyms, broadSynonyms, partOf)
                    cursor.execute(insert_query, insert_values)

                connection.commit()
            else:
                print("No results to insert.")

        except Exception as e:
            print(f"An error occurred when working with MySQL: {e}")
        finally:
            # Close connections
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
