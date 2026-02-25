import os

import psycopg2

source_dir = "/home/raghava/Raghava/Marketplace/AWSMarketplace/AWSMarketplace"
model_names = next(os.walk(source_dir))[1]

if ".git" in model_names:
    model_names.remove(".git")
    print("Git temp folder removed")

place_holder = "https://github.com/MphasisLimited/AWSMarketplace/tree/master/"

list_of_models_with_loc = []

count = 0
for model in model_names:

    model_name_in_git = model
    model_location_in_git = str(place_holder) + str(model_name_in_git.replace(" ", "%20"))

    json_object = {
        "model_name": model_name_in_git,
        "model_location": model_location_in_git,
    }

    list_of_models_with_loc.append(json_object)

    try:
        connection = psycopg2.connect(
            user="mlflow",
            password="mlflow",
            host="52.14.183.242",
            port="5432",
            database="modelhub",
        )
        cursor = connection.cursor()

        postgres_insert_query = (
            """ INSERT INTO models (model_name, model_location) VALUES (%s,%s)"""
        )
        record_to_insert = (model_name_in_git, model_location_in_git)
        cursor.execute(postgres_insert_query, record_to_insert)

        connection.commit()
        count = count + 1

    except (Exception, psycopg2.Error) as error:
        if connection:
            print("Failed to insert record into models table", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

print(count, "Record(s) inserted successfully into modelhub")
