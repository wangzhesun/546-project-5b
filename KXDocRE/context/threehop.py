import requests
import time
import math

def find_property_labels_batch(property_ids):
    """
    Fetch human-readable labels for a batch of property IDs from Wikidata API.
    """
    labels = {}
    if not property_ids:
        return labels
    
    ids = "|".join(property_ids)
    url = f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={ids}&format=json&props=labels"
    response = requests.get(url)
    
    if response.status_code == 200:
        if not response.text.strip():
            print(f"Warning: Empty response for property IDs: {property_ids}")
            return {property_id: property_id for property_id in property_ids}
        # print(response)
        data = response.json()
        # print(data)
        
        # Check if the response contains 'entities'
        if "entities" in data:
            for property_id in property_ids:
                if property_id in data["entities"]:
                    entity = data["entities"][property_id]
                    if "labels" in entity and "en" in entity["labels"]:
                        labels[property_id] = entity["labels"]["en"]["value"]
                    else:
                        labels[property_id] = property_id  # Fallback to ID if no label is found
                else:
                    labels[property_id] = property_id  # Fallback to ID if property is not found
        else:
            # If 'entities' key is missing, we log a warning and return the IDs as fallback
            print(f"Warning: 'entities' key missing in response for IDs: {property_ids}")
            for property_id in property_ids:
                labels[property_id] = property_id  # Fallback to ID
        
    else:
        # Handle non-200 status codes (API failure)
        print(f"Error: Received status code {response.status_code} for property IDs: {property_ids}")
        for property_id in property_ids:
            labels[property_id] = property_id  # Fallback to ID if there's an API error

    return labels


def batch_sparql_query(entity_pairs):
    """
    Queries relationships for a batch of entity pairs using SPARQL.
    """
    endpoint_url = "https://query.wikidata.org/sparql"
    headers = {'User-Agent': 'YourBotName/0.1 (your@email.com)'}
    
    # Construct the VALUES clause with the batch of pairs
    values_clause = " ".join(f"(wd:{e1.strip()} wd:{e2.strip()})" for e1, e2 in entity_pairs)
    # print(values_clause)
    # query = f"""
    # SELECT ?entity1 ?relation1 ?x ?relation2 ?y ?relation3 ?z ?relation4 ?entity2
    # WHERE {{
    # VALUES (?entity1 ?entity2) {{
    #     {values_clause}
    # }}
    # ?entity1 ?relation1 ?x.
    # ?x ?relation2 ?y.
    # ?y ?relation3 ?z.
    # ?z ?relation4 ?entity2.
    # GROUP BY ?entity1 ?entity2
    # LIMIT 1
    # }}
    # """
    query = f"""
    SELECT ?entity1 ?entity2 
        (SAMPLE(?relation1) AS ?relation1) (SAMPLE(?x) AS ?x)
        (SAMPLE(?relation2) AS ?relation2) (SAMPLE(?y) AS ?y)
        (SAMPLE(?relation3) AS ?relation3) (SAMPLE(?z) AS ?z)
        (SAMPLE(?relation4) AS ?relation4)
    WHERE {{
        VALUES (?entity1 ?entity2) {{
            {values_clause}
        }}
        ?entity1 ?relation1 ?x.
        ?x ?relation2 ?y.
        ?y ?relation3 ?z.
        ?z ?relation4 ?entity2.
    }}
    GROUP BY ?entity1 ?entity2
    """

    # print(query)
    
    # Perform the SPARQL query
    response = requests.get(endpoint_url, params={"query": query, "format": "json"}, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}, retrying after 5 seconds...")
        time.sleep(5)
        return None

def process_entity_pairs(input_file, output_file, batch_size=120, label_batch_size=40):
    """
    Reads entity pairs from a file, processes them in batches, and writes results to an output file.
    """
    # Read all entity pairs
    with open(input_file, "r") as f:
        entity_pairs = [line.strip().split("\t") for line in f.readlines()]

    # Divide into batches for entity pairs
    total_batches = math.ceil(len(entity_pairs) / batch_size)
    for batch_idx in range(total_batches):
        batch_start = batch_idx * batch_size
        batch_end = batch_start + batch_size
        batch = entity_pairs[batch_start:batch_end]
        
        print(f"Processing batch {batch_idx + 1}/{total_batches} for entity pairs...")
        
        # Query relationships for the batch (entity pairs in batch of 120)
        result = batch_sparql_query(batch)
        if result:
            # Use a dictionary to store relations for each entity pair
            entity_relations = {}
            
            for binding in result["results"]["bindings"]:
                # print(binding)
                # Parse the SPARQL binding results
                entity1 = binding.get("entity1", {}).get("value", "").split("/")[-1]
                entity2 = binding.get("entity2", {}).get("value", "").split("/")[-1]
                relation1 = binding.get("relation1", {}).get("value", "").split("/")[-1]
                relation2 = binding.get("relation2", {}).get("value", "").split("/")[-1]
                relation3 = binding.get("relation3", {}).get("value", "").split("/")[-1]
                relation4 = binding.get("relation4", {}).get("value", "").split("/")[-1]
                intermediate_x = binding.get("x", {}).get("value", "").split("/")[-1].split("-")[0].upper()
                intermediate_y = binding.get("y", {}).get("value", "").split("/")[-1].split("-")[0].upper()
                intermediate_z = binding.get("z", {}).get("value", "").split("/")[-1].split("-")[0].upper()

                # Store the relationship data
                if (entity1, entity2) not in entity_relations:
                    entity_relations[(entity1, entity2)] = []
                entity_relations[(entity1, entity2)].append({
                    "relation1": relation1,
                    "intermediate_x": intermediate_x,
                    "relation2": relation2,
                    "intermediate_y": intermediate_y,
                    "relation3": relation3,
                    "intermediate_z": intermediate_z,
                    "relation4": relation4,
                })
            
            # Now, fetch the relation labels in batches of 40 (max 50 per request)
            all_relations = [
                relation["relation1"] for relations in entity_relations.values() for relation in relations
            ] + [
                relation["relation2"] for relations in entity_relations.values() for relation in relations
            ] + [
                relation["relation3"] for relations in entity_relations.values() for relation in relations
            ] + [
                relation["relation4"] for relations in entity_relations.values() for relation in relations
            ]

            all_intermediate_entities = [
                relation["intermediate_x"]
                for relations in entity_relations.values() for relation in relations
            ] + [
                relation["intermediate_y"]
                for relations in entity_relations.values() for relation in relations
            ] + [
                relation["intermediate_z"]
                for relations in entity_relations.values() for relation in relations
            ]

            # print([
            #     relation["intermediate_x"]
            #     for relations in entity_relations.values() for relation in relations
            # ])
            
            # Calculate the exact number of batches required
            unique_relations = list(set(all_relations))  # Remove duplicates
            num_batches_relation = math.ceil(len(unique_relations) / label_batch_size)

            unique_entities = list(set(all_intermediate_entities))  # Remove duplicates
            num_batches_entity = math.ceil(len(unique_entities) / label_batch_size)
            
            # Fetch labels for each batch of property IDs
            property_labels = {}
            for i in range(num_batches_relation):
                print(f"Fetching relation labels batch {i + 1}/{num_batches_relation}...")
                label_batch_start = i * label_batch_size
                label_batch_end = label_batch_start + label_batch_size
                label_batch = unique_relations[label_batch_start:label_batch_end]
                
                labels_result = find_property_labels_batch(label_batch)
                property_labels.update(labels_result)

                time.sleep(1.5)
            
            entity_labels = {}
            # print(unique_entities)
            for i in range(num_batches_entity):
                print(f"Fetching entity labels batch {i + 1}/{num_batches_entity}...")
                label_batch_start = i * label_batch_size
                label_batch_end = label_batch_start + label_batch_size
                label_batch = unique_entities[label_batch_start:label_batch_end]
                
                # print(label_batch)
                labels_result = find_property_labels_batch(label_batch)
                entity_labels.update(labels_result)

                time.sleep(1.5)
            
            # print(entity_labels)

            with open(output_file, "a") as out_f:
                for (entity1, entity2), relations in entity_relations.items():
                    for relation in relations:
                        # Fetch labels for relations
                        relation1_label = property_labels.get(relation["relation1"], relation["relation1"])
                        relation2_label = property_labels.get(relation["relation2"], relation["relation2"])
                        relation3_label = property_labels.get(relation["relation3"], relation["relation3"])
                        relation4_label = property_labels.get(relation["relation4"], relation["relation4"])
                        
                        # Fetch labels for intermediate entities
                        intermediate_x_label = entity_labels.get(relation["intermediate_x"], relation["intermediate_x"])
                        intermediate_y_label = entity_labels.get(relation["intermediate_y"], relation["intermediate_y"])
                        intermediate_z_label = entity_labels.get(relation["intermediate_z"], relation["intermediate_z"])
                        
                        # Write the correctly labeled output
                        out_f.write(
                            f"{entity1}#{entity2}\t"
                            f"{relation1_label}#{intermediate_x_label}#"
                            f"{relation2_label}#{intermediate_y_label}#"
                            f"{relation3_label}#{intermediate_z_label}#"
                            f"{relation4_label}\n"
                        )
        
        time.sleep(1.5)


# Input and output file paths
input_file = "final_entity_pair.txt"  # Replace with your input file
output_file = "full_fourhop.txt"       # Replace with your output file

# Process pairs in batches of 100
process_entity_pairs(input_file, output_file, batch_size=10, label_batch_size=10)
