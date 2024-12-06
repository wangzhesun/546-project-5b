import requests
import time
import math

def find_property_labels_batch(property_ids):
    """
    Fetch human-readable labels for a batch of property IDs from Wikidata API.
    """
    ids = "|".join(property_ids)
    url = f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={ids}&format=json&props=labels"
    response = requests.get(url)
    
    labels = {}
    
    if response.status_code == 200:
        data = response.json()
        
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
    query = f"""
    SELECT ?entity1 ?entity2 ?property
    WHERE {{
      VALUES (?entity1 ?entity2) {{
        {values_clause}
      }}
      ?entity1 ?property ?entity2.
    }}
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
                entity1 = binding["entity1"]["value"].split("/")[-1]
                entity2 = binding["entity2"]["value"].split("/")[-1]
                property_id = binding["property"]["value"].split("/")[-1]
                
                # Append the property ID to the relations list for the entity pair
                if (entity1, entity2) not in entity_relations:
                    entity_relations[(entity1, entity2)] = []
                entity_relations[(entity1, entity2)].append(property_id)
            
            # Now, fetch the relation labels in batches of 40 (max 50 per request)
            all_property_ids = [property_id for relations in entity_relations.values() for property_id in relations]
            
            # Calculate the exact number of batches required
            num_batches = math.ceil(len(all_property_ids) / label_batch_size)
            
            # Fetch labels for each batch of property IDs
            property_labels = {}
            for i in range(num_batches):
                batch = all_property_ids[i * label_batch_size:(i + 1) * label_batch_size]
                print(f"Fetching labels batch {i + 1}/{num_batches}...")
                batch_labels = find_property_labels_batch(batch)
                property_labels.update(batch_labels)
                
                # Respect API rate limits
                time.sleep(1.5)
            
            # Write results to file with property labels
            with open(output_file, "a+") as out_file:
                for (entity1, entity2), property_ids in entity_relations.items():
                    # Concatenate the labels for all relations of this entity pair
                    labels = [property_labels.get(property_id, "No Relation") for property_id in property_ids]
                    out_file.write(f"{entity1}#{entity2}\t{'$'.join(labels)}\n")
        
        # Respect API rate limits
        time.sleep(1.5)

# Input and output file paths
input_file = "final_entity_pair.txt"  # Replace with your input file
output_file = "full_onehop.txt"       # Replace with your output file

# Process pairs in batches of 100
process_entity_pairs(input_file, output_file, batch_size=120, label_batch_size=40)
