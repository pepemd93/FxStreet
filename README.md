# FXStreet Senior Data Engineer assignment

## Exercice 1
The file `load_data.py` includes the code to fetch the data from the address specified and create/append it to a table in the duckdb database.
It also includes a couple of test: 1. the table has been created, 2. the table has at least the same number of records as the ones uploaded.

## Exercice 2
I performed the transformation via dbt.
- The view of section a. is called `event_parameters_columns`
- The table to perform the metrics is called `funnel metrics`
- The sql queries to get the metrics were created as tables in dbt with the following names:
    - `revenues`
    - `users_per_step`
    - `funnel_conversion`

## Exercice 3
The extract and load proposed on `load_data.py` is simple yet effective. More error handling and testing could be implemented to make the load process more robust. I could be worth investigating the HTTPFS DuckDB functionality and compare performances.

Regarding transformations, I chose to work with dbt as it is easier to organize and allows for a more flexible system when scaling up. Of course, it is more complex at the beginning, but I believe it pays off on the long run.
More test could be implemented as part of dbt to check the consistency and quality of data.

There is for sure better ways to implement the app. Ways that I would investigate in a real environment and with longer time.

## Exercice 4
The repository includes a `Dockerfile` to build the image.

Executing `docker run -p 8080:8080 fx_street_elt` will run the Flask app. When a GET request is sent to http://localhost:8080, it will trigger the extract, load, and SQL transformation process as defined in the app and return the final database in .duckdb format.

A volume can be mounted to get the resulting database in .duckdb within the local filesystem. Ex: `docker run -p 8080:8080 -v ./results:/results fx_street_elt`.
Of course it is best to run it in detached mode by adding the `-d` argument.