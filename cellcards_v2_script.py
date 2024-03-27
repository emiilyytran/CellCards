import mysql.connector
from SPARQLWrapper import SPARQLWrapper, JSON
from mysql.connector import Error


# Database connection function
# Only for connection to local DB
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


# Appends Cell ID to IRI and store as d
# Runs query with d and returns as JSON format
def perform_sparql_query(cell_id):
    sparql = SPARQLWrapper("https://sparql.hegroup.org/sparql")
    sparql.setReturnFormat(JSON)
    d = f"http://purl.obolibrary.org/obo/{cell_id}"

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
            ?cell rdf:type owl:Class .
            FILTER(?cell = <{d}>)
             ?cell rdfs:label ?label.
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

    query = query_template.replace("{d}", d)
    sparql.setQuery(query)

    try:
        results = sparql.query().convert()

        return results
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def insert_ontology_term(cursor1, cell_id, label, definition):
    x = """
    INSERT INTO t_ontology_term (c_ontology_term_id, c_name, c_definition)
    VALUES (%s, %s, %s);
"""
    cursor1.execute(x, (cell_id, label, definition))
def insert_ontology_term_relation(cursor2,label, partOf):
    x = """
        INSERT INTO t_ontology_term_relation (c_subject_term_label, c_predicate_term_label, c_object_term_label)
        VALUES (%s, %s, %s);
    """
    cursor2.execute(x, (label, "part of", partOf))
def insert_t_cells(cursor3, cell_id, label):
    x = """
        INSERT INTO t_cells (c_cell_ontology_term_id, c_cell_name)
        VALUES (%s, %s);
    """
    cursor3.execute(x, (cell_id, label))
def insert_t_synonym(cursor, exactSyn, broadSyn, cell_id):
    insert_query = """
        INSERT INTO t_synonym (c_synonym_label, c_ontology_term_id)
        VALUES (%s, %s);
    """
    # Function to insert synonyms
    def insert_synonyms(synonyms, cell_id):
        # Split the string by commas to get individual synonyms
        synonyms_list = synonyms.split(',')
        # Trim whitespace and insert each synonym
        for synonym in synonyms_list:
            synonym = synonym.strip()  # Remove leading/trailing whitespace
            if synonym:  # Check if synonym is not an empty string
                cursor.execute(insert_query, (synonym, cell_id))

    # Insert exact synonyms
    insert_synonyms(exactSyn, cell_id)

    # Insert broad synonyms
    insert_synonyms(broadSyn, cell_id)


if __name__ == "__main__":
    connection, cursor = create_database_connection("127.0.0.1", 3307, "root", "root", "cellcards_v2")

    # Check if connection was successful
    if connection is not None and cursor is not None:
        try:
            cell_iri = input("Enter the cell iri: ")
            results = perform_sparql_query(cell_iri)

            if results:  # Check if there are any results
                for result in results["results"]["bindings"]:
                    label = get_value_from_result("label", result)
                    definition = get_value_from_result("definition", result)
                    exactSynonyms = get_value_from_result("exactSynonyms", result)
                    broadSynonyms = get_value_from_result("broadSynonyms", result)
                    partOf = get_value_from_result("partOf", result)

                insert_ontology_term(cursor, cell_iri, label, definition)
                insert_ontology_term_relation(cursor, label, partOf)
                insert_t_cells(cursor, cell_iri, label)
                insert_t_synonym(cursor, exactSynonyms, broadSynonyms, cell_iri)

                connection.commit()
            else:
                print("No results to insert.")

        except Exception as e:
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {e}")
        finally:
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
