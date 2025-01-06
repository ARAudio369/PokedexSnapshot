import requests
from google.cloud import storage, bigquery
import json
import os
from datetime import datetime

#credentials_path = '<xxxxxxxxx>json' -- used when testing script from local machine
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path used when testing script from local machine

# Func 1: Fetch Pokémon data
def fetch_pokedex_data():
    url = 'https://play.pokemonshowdown.com/data/pokedex.json'
    response = requests.get(url)
    return response.json()

# Func 2: Transform data
def transform_data(raw_data):
    extracted_data = []
    for pokemon_name, pokemon_info in raw_data.items():
        abilities = pokemon_info.get('abilities', {})
        
        abilities_record = {
            '0': abilities.get('0'),
            '1': abilities.get('1'),
            'H': abilities.get('H'),
        }

        # Check if dexnum is less than 0 and skip the entry
        if pokemon_info.get('num') is not None and pokemon_info.get('num') < 0:
            continue


            # Set default gender ratio to 50/50
        gender_ratio = {'M': 0.5, 'F': 0.5}

        # Modify genderRatio based on the gender field
        if pokemon_info.get('gender') == 'F':
            gender_ratio = {'M': 0.0, 'F': 1.0}
        elif pokemon_info.get('gender') == 'M':
            gender_ratio = {'M': 1.0, 'F': 0.0}
        elif pokemon_info.get('gender') == 'N':
            gender_ratio = {'M': None, 'F': None}
            # No need to change gender_ratio in this case, as it's already set to 50/50
            pass
        elif 'genderRatio' in pokemon_info and isinstance(pokemon_info['genderRatio'].get('M'), (float, int)) and isinstance(pokemon_info['genderRatio'].get('F'), (float, int)):
            gender_ratio = pokemon_info['genderRatio']


        pokemon_data = {
            'upload_timestamp': datetime.utcnow(),
            'dexnum': pokemon_info.get('num'),
            'name': pokemon_info.get('name'),
            'types': pokemon_info.get('types'),
            'genderRatio': gender_ratio,
            'baseStats': pokemon_info.get('baseStats'),
            'abilities': abilities_record,
            'heightm': pokemon_info.get('heightm'),
            'weightkg': pokemon_info.get('weightkg'),
            'color': pokemon_info.get('color'),
            'evos': pokemon_info.get('evos'),
            'prevo': pokemon_info.get('prevo'),
            'evoLevel': pokemon_info.get('evoLevel'),
            'eggGroups': pokemon_info.get('eggGroups'),
            'tier': pokemon_info.get('tier'),
            'isNonstandard': pokemon_info.get('isNonstandard'),
            'otherForms': pokemon_info.get('otherFormes') or [],
            'cosmeticForms': pokemon_info.get('cosmeticFormes') or [],
            'formOrder': pokemon_info.get('formeOrder') or [], 
            'canGigantamax': pokemon_info.get('canGigantamax'),
            'baseSpecies': pokemon_info.get('baseSpecies'),
            'form': pokemon_info.get('forme'),
            'requiredItem': pokemon_info.get('requiredItem'),
            'changesFrom': pokemon_info.get('changesFrom'),
        }
        
        extracted_data.append(pokemon_data)

    return extracted_data

# Func 3: Upload to GCP -  used when testing script from local machine
# def upload_to_gcp(data):
    #storage_client = storage.Client() 
    #bucket_name = 'my-pokedex-project-bucket'
    #bucket = storage_client.get_bucket(bucket_name)

    #for pokemon_data in data:
        #file_name = f'{pokemon_data["name"].lower()}.json'
        #blob = bucket.blob(file_name)
        #blob.upload_from_string(json.dumps(pokemon_data), content_type='application/json')
        #print(f'Uploaded {file_name} to {bucket_name}')


# New Func 4: Take snapshot and insert into BigQuery
def take_snapshot(data):
    project_id = 'pokedex-407016'
    dataset_id = 'pokedex_dataset'
    table_id = 'pokedex_daily_snapshot'
    client=bigquery.Client(project_id)
    dataset=client.dataset(dataset_id)
    table=dataset.table(table_id)

    
    # Convert datetime objects to ISO format strings
    for item in data:
        item['upload_timestamp'] = item['upload_timestamp'].isoformat()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_json(data, table, job_config=job_config)
    job.result()

    print(f'Data snapshot inserted into {dataset_id}.{table_id}')

# Cloud Function Entry Point
def main(data, context):
    raw_data = fetch_pokedex_data()
    transformed_data = transform_data(raw_data)
    take_snapshot(transformed_data)

    return "Function executed successfully!"
