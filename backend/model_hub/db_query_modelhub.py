import os

import psycopg2

mh_user = os.getenv("MODELHUB_USER")
mh_password = os.getenv("MODELHUB_DB_PW")
mh_host = os.getenv("MODELHUB_DB_SERVER_IP")
mh_port = os.getenv("MODELHUB_DB_PORT")
mh_database = os.getenv("MODELHUB_DB")


def return_models_info():
    list_of_models_with_loc = []
    count = 0

    try:
        connection = psycopg2.connect(
            user=mh_user,
            password=mh_password,
            host=mh_host,
            port=mh_port,
            database=mh_database,
        )
        cursor = connection.cursor()

        postgres_insert_query = "SELECT * FROM models_aws_listings"

        cursor.execute(postgres_insert_query)

        git_records = cursor.fetchall()

        for record in git_records:

            json_object = {
                "model_name": record[0],
                "model_location": record[1],
                "short_description": record[2],
            }
            list_of_models_with_loc.append(json_object)
            count = count + 1

    except (Exception, psycopg2.Error) as error:
        if connection:
            print("Failed to insert record into mobile table", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    # output_json = {"models":list_of_models_with_loc}
    # print(count, "Models found in Model Hub")
    # print(output_json)
    return list_of_models_with_loc
